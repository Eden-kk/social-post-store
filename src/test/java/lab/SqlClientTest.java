package lab;

import com.sun.net.httpserver.*;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class SqlClientTest {
    private static final Duration ATTEMPT = Duration.ofMillis(150);
    private static final Duration BACKOFF = Duration.ofMillis(10);
    private static final Duration DEADLINE = Duration.ofSeconds(3);

    private static final class Server implements AutoCloseable {
        final HttpServer server;
        final ExecutorService workers = Executors.newCachedThreadPool();
        Server(HttpHandler handler) throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(workers);
            server.createContext("/sql", handler);
            server.start();
        }
        URI uri() { return URI.create("http://127.0.0.1:" + server.getAddress().getPort()); }
        public void close() { server.stop(0); workers.shutdownNow(); }
    }
    private static void reply(HttpExchange exchange, int code, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(code, bytes.length);
        try (var out = exchange.getResponseBody()) { out.write(bytes); }
        exchange.close();
    }
    private static void check(boolean valid, String message) {
        if (!valid) throw new AssertionError(message);
    }
    private static SqlClient client(URI... endpoints) {
        return new SqlClient(List.of(endpoints), ATTEMPT, BACKOFF);
    }
    private static void lostResponseAndUnknown() throws Exception {
        var seen = new CopyOnWriteArrayList<String>();
        var results = new ConcurrentHashMap<String, String>();
        var effects = new AtomicInteger();
        var calls = new AtomicInteger();
        String sql = " UPDATE t SET n = n + 1 WHERE id = 1;\n";
        try (var server = new Server(exchange -> {
            String id = exchange.getRequestHeaders().getFirst("X-Request-ID");
            seen.add(id + ":" + new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            String result = results.computeIfAbsent(id, key -> { effects.incrementAndGet(); return "OK UPDATE 1"; });
            int call = calls.incrementAndGet();
            if (call == 1) { exchange.close(); return; }
            reply(exchange, 200, call == 2 ? "UNKNOWN retry same request_id" : result);
        })) {
            check(client(server.uri()).execute("stable-1", sql, DEADLINE).equals("OK UPDATE 1"), "lost response result");
            check(calls.get() >= 3 && effects.get() == 1, "dedup effect");
            check(seen.stream().allMatch(s -> s.equals("stable-1:" + sql)), "changed request bytes or ID");
        }
    }
    private static void redirectAndErrors() throws Exception {
        var followerCalls = new AtomicInteger();
        var leaderCalls = new AtomicInteger();
        try (var follower = new Server(e -> { followerCalls.incrementAndGet(); reply(e, 200, "NOT_LEADER 99"); });
             var leader = new Server(e -> { leaderCalls.incrementAndGet(); reply(e, 200, "ERR duplicate primary key"); })) {
            check(client(follower.uri(), leader.uri()).execute("err-1", "INSERT bad", DEADLINE)
                .equals("ERR duplicate primary key"), "ERR must return unchanged");
            check(followerCalls.get() == 1 && leaderCalls.get() == 1, "ERR was retried");
        }
    }
    private static void boundedFailures() throws Exception {
        try (var unavailable = new Server(e -> reply(e, 503, "unavailable"));
             var slowBody = new Server(e -> {
                 e.sendResponseHeaders(200, 100);
                 e.getResponseBody().write('O'); e.getResponseBody().flush();
                 try { Thread.sleep(2000); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                 e.close();
             })) {
            for (URI endpoint : List.of(unavailable.uri(), slowBody.uri())) {
                long start = System.nanoTime();
                try { client(endpoint).execute("bounded", "SELECT * FROM t", Duration.ofMillis(350));
                    throw new AssertionError("deadline returned a result");
                } catch (SqlClient.OutcomeUnknownException expected) {
                    check(expected.getMessage().contains("outcome unknown"), "misleading timeout");
                }
                long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                check(elapsed >= 300 && elapsed < 1500, "unbounded/early deadline: " + elapsed);
            }
        }
        URI closed;
        try (var socket = new ServerSocket(0)) { closed = URI.create("http://127.0.0.1:" + socket.getLocalPort()); }
        try (var healthy = new Server(e -> reply(e, 200, "OK SELECT result"))) {
            check(client(closed, healthy.uri()).execute("connect", "SELECT * FROM t", DEADLINE)
                .equals("OK SELECT result"), "connection failure did not rotate");
        }
    }
    private static void interruption() throws Exception {
        var entered = new CountDownLatch(1);
        var outcome = new AtomicReference<Throwable>();
        try (var server = new Server(e -> {
            entered.countDown();
            try { Thread.sleep(2000); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            e.close();
        })) {
            var client = new SqlClient(List.of(server.uri()), Duration.ofSeconds(10), BACKOFF);
            var thread = new Thread(() -> {
                try { client.execute("interrupt", "SELECT * FROM t", Duration.ofSeconds(20)); }
                catch (Throwable failure) { outcome.set(failure); }
            });
            thread.start();
            try {
                check(entered.await(2, TimeUnit.SECONDS), "request never started");
                thread.interrupt(); thread.join(1000);
                check(!thread.isAlive() && outcome.get() instanceof InterruptedException, "interruption not propagated");
            } finally { thread.interrupt(); thread.join(1000); }
        }
    }
    private static void invalid(Runnable action) {
        try { action.run(); throw new AssertionError("invalid input accepted"); }
        catch (IllegalArgumentException expected) { }
    }
    private static void interruptedBackoffAndMalformedReply() throws Exception {
        var replied = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var outcome = new AtomicReference<Throwable>();
        try (var server = new Server(e -> {
            calls.incrementAndGet(); reply(e, 200, "not a terminal protocol response"); replied.countDown();
        })) {
            var client = new SqlClient(List.of(server.uri()), Duration.ofSeconds(1), Duration.ofSeconds(10));
            var thread = new Thread(() -> {
                try { client.execute("backoff", "SELECT * FROM t", Duration.ofSeconds(20)); }
                catch (Throwable failure) { outcome.set(failure); }
            });
            thread.start();
            try {
                check(replied.await(2, TimeUnit.SECONDS), "no malformed reply");
                long limit = System.nanoTime() + Duration.ofSeconds(2).toNanos();
                while (!inBackoff(thread) && System.nanoTime() < limit)
                    Thread.sleep(5);
                check(inBackoff(thread), "no bounded retry wait");
                thread.interrupt(); thread.join(1000);
                check(!thread.isAlive() && outcome.get() instanceof InterruptedException, "backoff interruption");
                check(calls.get() == 1, "retried before backoff elapsed");
            } finally { thread.interrupt(); thread.join(1000); }
            Thread.currentThread().interrupt();
            try {
                client.execute("pre-interrupted", "SELECT * FROM t", DEADLINE);
                throw new AssertionError("pre-interrupted request sent");
            } catch (InterruptedException expected) {
                check(calls.get() == 1, "pre-interrupted caller reached server");
            } finally { Thread.interrupted(); }
        }
    }
    private static boolean inBackoff(Thread thread) {
        return Arrays.stream(thread.getStackTrace()).anyMatch(frame ->
            frame.getClassName().equals("java.util.concurrent.TimeUnit") && frame.getMethodName().equals("sleep"));
    }
    private static void validation() throws Exception {
        URI endpoint = URI.create("http://127.0.0.1:1");
        invalid(() -> new SqlClient(List.of(), ATTEMPT, BACKOFF));
        invalid(() -> client(URI.create("http://localhost/sql")));
        invalid(() -> client(URI.create("ftp://localhost")));
        invalid(() -> new SqlClient(List.of(endpoint), Duration.ZERO, BACKOFF));
        invalid(() -> new SqlClient(List.of(endpoint), ATTEMPT, Duration.ofMillis(-1)));
        invalid(() -> new SqlClient(List.of(endpoint), Duration.ofSeconds(Long.MAX_VALUE), BACKOFF));
        var client = client(endpoint);
        for (String id : Arrays.asList(null, "", " ", "a\nb", " x", "x".repeat(129))) {
            try { client.execute(id, "SELECT * FROM t", DEADLINE); throw new AssertionError("invalid ID accepted"); }
            catch (IllegalArgumentException expected) { }
        }
        for (String sql : Arrays.asList(null, "", " ", "x".repeat(8193))) {
            try { client.execute("id", sql, DEADLINE); throw new AssertionError("invalid SQL accepted"); }
            catch (IllegalArgumentException expected) { }
        }
        try { client.execute("id", "SELECT * FROM t", Duration.ZERO); throw new AssertionError("zero deadline"); }
        catch (IllegalArgumentException expected) { }
    }
    public static void main(String[] args) throws Exception {
        validation(); lostResponseAndUnknown(); redirectAndErrors(); boundedFailures(); interruption();
        interruptedBackoffAndMalformedReply();
        System.out.println("SqlClientTest passed");
    }
}

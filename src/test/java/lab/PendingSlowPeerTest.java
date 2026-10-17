package lab;

import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public final class PendingSlowPeerTest {
    public static void main(String[] args) throws Exception {
        var executor = Executors.newCachedThreadPool();
        var release = new CountDownLatch(1);
        var thirdStarted = new CountDownLatch(1);
        var requests = new java.util.concurrent.atomic.AtomicInteger();
        var recovered = new java.util.concurrent.atomic.AtomicBoolean();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/raft/like", exchange -> {
            String id = exchange.getRequestHeaders().getFirst("X-Request-ID");
            if (id.equals("recover") && recovered.get()) {
                byte[] body = "OK SAME count=1".getBytes();
                exchange.sendResponseHeaders(200, body.length); exchange.getResponseBody().write(body); exchange.close(); return;
            }
            if (id.startsWith("size-")) {
                byte[] body = ("OK " + "x".repeat(Integer.parseInt(id.substring(5)) - 3)).getBytes();
                exchange.sendResponseHeaders(200, body.length); exchange.getResponseBody().write(body); exchange.close(); return;
            }
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().write("OK ".getBytes()); exchange.getResponseBody().flush();
            if (requests.incrementAndGet() == 3) thirdStarted.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            exchange.close();
        });
        server.start();
        var url = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        try (var node = new RaftNode(1, Map.of(1, URI.create("http://localhost:1"), 2, url,
                3, URI.create("http://localhost:2")), Files.createTempDirectory("slow-raft-"))) {
            node.block(Set.of(3));
            if (node.pendingLike("size-2048", "LIKE\tp\tu\tw\t1\ttrue").length() != 2048)
                throw new AssertionError("2048 byte boundary rejected");
            if (!node.pendingLike("size-2049", "LIKE\tp\tu\tw\t1\ttrue").startsWith("UNKNOWN"))
                throw new AssertionError("oversized body accepted");
            long before = System.nanoTime();
            String result = node.pendingLike("slow", "LIKE\tp\tu\tw\t1\ttrue");
            if (!result.startsWith("UNKNOWN") || TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before) > 3000)
                throw new AssertionError("stalled body must time out");
            try (var queue = new PendingLikes(Files.createTempDirectory("slow-pending-"), 3,
                    (id, body) -> id.equals("slow") ? node.pendingLike(id, body) : "OK LIKE count=1")) {
                queue.enqueue(queue.origin(), "slow", "LIKE\tp\tu\tw\t1\ttrue");
                queue.enqueue(queue.origin(), "next", "LIKE\tp\tu\tw\t2\ttrue");
                queue.retry();
                if (!queue.status(queue.origin(), "next").startsWith("COMMITTED")) throw new AssertionError("next intent starved");
                queue.start();
                if (!thirdStarted.await(3, TimeUnit.SECONDS)) throw new AssertionError("worker did not enter stalled body");
                before = System.nanoTime();
            }
            if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before) > 2500) throw new AssertionError("close blocked");
            Path recoveryDir = Files.createTempDirectory("slow-recovery-"); String origin;
            try (var queue = new PendingLikes(recoveryDir, 2, node::pendingLike)) {
                origin = queue.origin(); queue.enqueue(origin, "recover", "LIKE\tp\tu\tw\t1\ttrue");
                queue.retry();
                if (!queue.status(origin, "recover").equals("PENDING")) throw new AssertionError("timeout changed pending status");
                recovered.set(true); queue.retry();
                if (!queue.status(origin, "recover").equals("COMMITTED\nOK SAME count=1")) throw new AssertionError("recovery result lost");
            }
            try (var queue = new PendingLikes(recoveryDir, 2, (id, body) -> { throw new AssertionError("terminal retry"); })) {
                queue.retry();
                if (!queue.status(origin, "recover").equals("COMMITTED\nOK SAME count=1")) throw new AssertionError("recovered result not durable");
            }
        } finally { release.countDown(); server.stop(0); executor.shutdownNow(); }
        System.out.println("PENDING_SLOW_PEER_TEST_PASSED");
    }
}

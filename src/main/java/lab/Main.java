package lab;

import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executors;
import static lab.Protocol.*;

/** Loopback-only local lab server. Never deploy on a public network. */
public final class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) throw new IllegalArgumentException(
            "usage: Main NODE_ID DATA_DIR PORT0,PORT1,PORT2 [--test-controls]");
        int id = Integer.parseInt(args[0]); String[] ports = args[2].split(",");
        var peers = new LinkedHashMap<Integer, URI>();
        for (int i = 0; i < ports.length; i++) {
            int port = Integer.parseInt(ports[i]);
            if (port < 1 || port > 65535) throw new IllegalArgumentException("invalid port");
            peers.put(i, URI.create("http://127.0.0.1:"+port));
        }
        var node = new RaftNode(id, peers, Path.of(args[1]));
        var pendingLikes = new PendingLikes(Path.of(args[1]).resolve("pending-likes"), 10000, node::pendingLike);
        var counter = new lab.counter.CounterNode(id, peers, Path.of(args[1]).resolve("counter"));
        var sqlCounter = new lab.counter.CounterNode(id, peers, Path.of(args[1]).resolve("sql-counter"), "/sql-counter/merge");
        var views = new lab.counter.CounterNode(id, peers, Path.of(args[1]).resolve("social-views"), "/social/views/merge");
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", peers.get(id).getPort()), 32);
        var workers = Executors.newFixedThreadPool(16);
        server.setExecutor(workers);
        server.createContext("/", exchange -> {
            try {
                String path = exchange.getRequestURI().getPath(); byte[] response;
                if (path.equals("/status") && exchange.getRequestMethod().equals("GET")) {
                    response = node.status().getBytes(StandardCharsets.UTF_8);
                } else if (path.equals("/social/likes/queue") && exchange.getRequestMethod().equals("GET")) {
                    response = ("origin=" + pendingLikes.origin()).getBytes(StandardCharsets.UTF_8);
                } else if (path.equals("/counter/status") && exchange.getRequestMethod().equals("GET")) {
                    response = counter.status().getBytes(StandardCharsets.UTF_8);
                } else if (path.equals("/sql-counter/status") && exchange.getRequestMethod().equals("GET")) {
                    response = sqlCounter.status().getBytes(StandardCharsets.UTF_8);
                } else if (path.equals("/social/views/status") && exchange.getRequestMethod().equals("GET")) {
                    response = views.status().getBytes(StandardCharsets.UTF_8);
                } else {
                    if (!exchange.getRequestMethod().equals("POST")) {send(exchange, 405, "POST required".getBytes()); return;}
                    byte[] body = exchange.getRequestBody().readNBytes(1_048_577);
                    if (body.length > 1_048_576) throw new IllegalArgumentException("body too large");
                    switch (path) {
                        case "/social/likes/pending" -> response = pendingLikes.enqueue(
                            exchange.getRequestHeaders().getFirst("X-Pending-Origin"),
                            exchange.getRequestHeaders().getFirst("X-Request-ID"),
                            new String(body, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
                        case "/social/likes/result" -> response = pendingLikes.status(
                            exchange.getRequestHeaders().getFirst("X-Pending-Origin"),
                            new String(body, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
                        case "/raft/like" -> response = node.forwardedLike(
                            Integer.parseInt(exchange.getRequestHeaders().getFirst("X-Raft-Peer")),
                            exchange.getRequestHeaders().getFirst("X-Request-ID"),
                            new String(body, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
                        case "/social" -> {
                            String key = exchange.getRequestHeaders().getFirst("X-Request-ID");
                            if (key == null || key.isBlank() || key.length() > 120
                                    || key.chars().anyMatch(c -> c < 33 || c > 126))
                                throw new IllegalArgumentException("X-Request-ID required, max 120 characters");
                            response = node.submit("social:" + key, SocialState.PREFIX + new String(body, StandardCharsets.UTF_8), 3000)
                                .getBytes(StandardCharsets.UTF_8);
                        }
                        case "/social/view", "/social/views" -> {
                            String post = SocialState.identifier(new String(body, StandardCharsets.UTF_8));
                            // A terminal tombstone reply must belong to the requested incarnation.
                            if (path.equals("/social/view") && !views.origin().equals(
                                    exchange.getRequestHeaders().getFirst("X-Counter-Origin")))
                                throw new IllegalArgumentException("retry must use original incarnation");
                            // Admission uses a locally applied snapshot, not a cross-store transaction.
                            // Concurrent or remotely committed deletion may still admit telemetry.
                            node.requireActivePost(post);
                            String result = path.equals("/social/views") ? "OK EVENTUAL " + views.value(post)
                                : views.add(exchange.getRequestHeaders().getFirst("X-Counter-Origin"),
                                    exchange.getRequestHeaders().getFirst("X-Request-ID"), post + "\n1");
                            response = result.getBytes(StandardCharsets.UTF_8);
                        }
                        case "/social/views/merge" -> {
                            response = views.receive(Integer.parseInt(exchange.getRequestHeaders().getFirst("X-Counter-Peer")), body);
                            if (views.droppingAck()) { exchange.close(); return; }
                        }
                        case "/test/social-views-drop-ack" -> {
                            if (args.length != 4 || !args[3].equals("--test-controls")) {send(exchange, 404, new byte[0]); return;}
                            views.dropAck(new String(body, StandardCharsets.UTF_8).equals("true")); response = "OK".getBytes();
                        }
                        case "/sql-counter/merge" -> {
                            response = sqlCounter.receive(Integer.parseInt(exchange.getRequestHeaders().getFirst("X-Counter-Peer")), body);
                            if (sqlCounter.droppingAck()) { exchange.close(); return; }
                        }
                        case "/counter/add" -> response = counter.add(exchange.getRequestHeaders().getFirst("X-Counter-Origin"),
                            exchange.getRequestHeaders().getFirst("X-Request-ID"), new String(body, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
                        case "/counter/read" -> response = counter.value(new String(body, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
                        case "/counter/merge" -> {
                            response = counter.receive(Integer.parseInt(exchange.getRequestHeaders().getFirst("X-Counter-Peer")), body);
                            if (counter.droppingAck()) { exchange.close(); return; }
                        }
                        case "/test/counter-drop-ack" -> {
                            if (args.length != 4 || !args[3].equals("--test-controls")) {send(exchange, 404, new byte[0]); return;}
                            counter.dropAck(new String(body, StandardCharsets.UTF_8).equals("true")); response = "OK".getBytes();
                        }
                        case "/test/sql-counter-drop-ack" -> {
                            if (args.length != 4 || !args[3].equals("--test-controls")) {send(exchange, 404, new byte[0]); return;}
                            sqlCounter.dropAck(new String(body, StandardCharsets.UTF_8).equals("true")); response = "OK".getBytes();
                        }
                        case "/raft/vote" -> {
                            var reply = node.vote(readVote(body));
                            response = encode(o -> {o.writeLong(reply.term()); o.writeBoolean(reply.granted());});
                        }
                        case "/raft/append" -> {
                            var reply = node.append(readAppend(body));
                            response = encode(o -> {o.writeLong(reply.term()); o.writeBoolean(reply.success()); o.writeInt(reply.matchIndex());});
                        }
                        case "/sql" -> {
                            String key = exchange.getRequestHeaders().getFirst("X-Request-ID");
                            if (key == null) throw new IllegalArgumentException("X-Request-ID required");
                            String sql = new String(body, StandardCharsets.UTF_8);
                            if (sql.startsWith(SocialState.PREFIX) || key.startsWith("social:"))
                                throw new IllegalArgumentException("reserved social command/request namespace");
                            String result;
                            if (SqlEngine.isCounterStatement(sql)) {
                                var command = node.counterCommand(sql);
                                result = command.delta() == null ? "OK EVENTUAL " + sqlCounter.value(command.storageKey())
                                    : sqlCounter.add(exchange.getRequestHeaders().getFirst("X-Counter-Origin"), key,
                                        command.storageKey() + "\n" + command.delta());
                            } else result = node.submit(key, sql, 3000);
                            response = result.getBytes(StandardCharsets.UTF_8);
                        }
                        case "/test/block" -> {
                            if (args.length != 4 || !args[3].equals("--test-controls")) {send(exchange, 404, new byte[0]); return;}
                            var ids = new HashSet<Integer>();
                            String value = new String(body, StandardCharsets.UTF_8).trim();
                            if (!value.isEmpty()) for (String x : value.split(",")) ids.add(Integer.parseInt(x));
                            node.block(ids); counter.block(ids); sqlCounter.block(ids); views.block(ids); response = "OK".getBytes(StandardCharsets.UTF_8);
                        }
                        default -> {send(exchange, 404, new byte[0]); return;}
                    }
                }
                send(exchange, 200, response);
            } catch (IllegalArgumentException e) { send(exchange, 400, e.getMessage().getBytes(StandardCharsets.UTF_8)); }
            catch (Exception e) { send(exchange, 503, ("unavailable: "+e.getClass().getSimpleName()).getBytes(StandardCharsets.UTF_8)); }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(0); workers.shutdownNow(); try {pendingLikes.close();} catch (IOException ignored) {}
            try {node.close();} catch (IOException ignored) {}
            try {counter.close();} catch (IOException ignored) {}
            try {sqlCounter.close();} catch (IOException ignored) {}
            try {views.close();} catch (IOException ignored) {}
        }));
        server.start(); node.start(); counter.start(); sqlCounter.start(); views.start(); pendingLikes.start(); System.out.println("STARTED node="+id);
    }
    private static void send(HttpExchange exchange, int code, byte[] body) throws IOException {
        exchange.sendResponseHeaders(code, body.length);
        try (var output = exchange.getResponseBody()) {output.write(body);}
        exchange.close();
    }
}

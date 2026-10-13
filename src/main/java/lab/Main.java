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
        var counter = new lab.counter.CounterNode(id, peers, Path.of(args[1]).resolve("counter"));
        var sqlCounter = new lab.counter.CounterNode(id, peers, Path.of(args[1]).resolve("sql-counter"), "/sql-counter/merge");
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", peers.get(id).getPort()), 32);
        var workers = Executors.newFixedThreadPool(16);
        server.setExecutor(workers);
        server.createContext("/", exchange -> {
            try {
                String path = exchange.getRequestURI().getPath(); byte[] response;
                if (path.equals("/status") && exchange.getRequestMethod().equals("GET")) {
                    response = node.status().getBytes(StandardCharsets.UTF_8);
                } else if (path.equals("/counter/status") && exchange.getRequestMethod().equals("GET")) {
                    response = counter.status().getBytes(StandardCharsets.UTF_8);
                } else if (path.equals("/sql-counter/status") && exchange.getRequestMethod().equals("GET")) {
                    response = sqlCounter.status().getBytes(StandardCharsets.UTF_8);
                } else {
                    if (!exchange.getRequestMethod().equals("POST")) {send(exchange, 405, "POST required".getBytes()); return;}
                    byte[] body = exchange.getRequestBody().readNBytes(1_048_577);
                    if (body.length > 1_048_576) throw new IllegalArgumentException("body too large");
                    switch (path) {
                        case "/sql-counter/merge" -> response = sqlCounter.receive(Integer.parseInt(exchange.getRequestHeaders().getFirst("X-Counter-Peer")), body);
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
                            node.block(ids); counter.block(ids); sqlCounter.block(ids); response = "OK".getBytes(StandardCharsets.UTF_8);
                        }
                        default -> {send(exchange, 404, new byte[0]); return;}
                    }
                }
                send(exchange, 200, response);
            } catch (IllegalArgumentException e) { send(exchange, 400, e.getMessage().getBytes(StandardCharsets.UTF_8)); }
            catch (Exception e) { send(exchange, 503, ("unavailable: "+e.getClass().getSimpleName()).getBytes(StandardCharsets.UTF_8)); }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(0); workers.shutdownNow(); try {node.close();} catch (IOException ignored) {}
            try {counter.close();} catch (IOException ignored) {}
            try {sqlCounter.close();} catch (IOException ignored) {}
        }));
        server.start(); node.start(); counter.start(); sqlCounter.start(); System.out.println("STARTED node="+id);
    }
    private static void send(HttpExchange exchange, int code, byte[] body) throws IOException {
        exchange.sendResponseHeaders(code, body.length);
        try (var output = exchange.getResponseBody()) {output.write(body);}
        exchange.close();
    }
}

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
        var started = new CountDownLatch(1);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/raft/like", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().write("OK ".getBytes()); exchange.getResponseBody().flush();
            started.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            exchange.close();
        });
        server.start();
        var url = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        try (var node = new RaftNode(1, Map.of(1, URI.create("http://localhost:1"), 2, url,
                3, URI.create("http://localhost:2")), Files.createTempDirectory("slow-raft-"))) {
            node.block(Set.of(3));
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
                Thread.sleep(300);
                before = System.nanoTime();
            }
            if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before) > 2500) throw new AssertionError("close blocked");
        } finally { release.countDown(); server.stop(0); executor.shutdownNow(); }
        System.out.println("PENDING_SLOW_PEER_TEST_PASSED");
    }
}

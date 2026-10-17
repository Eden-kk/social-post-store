package lab;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.UUID;
import java.util.stream.IntStream;

/** Actual three JVMs, local test-control partitions, durable restart; not physical regions. */
public final class PendingLikesClusterTest {
    private static void check(boolean value, String text) { if (!value) throw new AssertionError(text); }
    private static HttpResponse<String> request(CounterClusterTest t, int node, String path, String origin, String id, String body) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + t.ports[node] + path))
            .timeout(Duration.ofSeconds(5)).header("X-Pending-Origin", origin)
            .POST(HttpRequest.BodyPublishers.ofString(body));
        if (id != null) builder.header("X-Request-ID", id);
        return t.client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }
    private static String status(CounterClusterTest t, int node, String origin, String id) throws Exception {
        return request(t, node, "/social/likes/result", origin, null, id).body();
    }
    private static void await(CounterClusterTest t, int node, String origin, String id, String prefix) throws Exception {
        long end = System.nanoTime() + Duration.ofSeconds(20).toNanos(); String result = "";
        while (System.nanoTime() < end) {
            result = status(t, node, origin, id); if (result.startsWith(prefix)) return; Thread.sleep(80);
        }
        throw new AssertionError("pending timeout " + id + " " + result);
    }
    public static void main(String[] args) throws Exception {
        try (var t = new CounterClusterTest()) {
            System.out.println("EVIDENCE_DIR=" + t.root);
            for (int i = 0; i < 3; i++) t.start(i);
            var client = SqlClient.social(IntStream.range(0, 3).mapToObj(i -> URI.create("http://127.0.0.1:" + t.ports[i])).toList(), Duration.ofSeconds(4), Duration.ofMillis(40));
            check(client.execute("create", "CREATE\tp\towner\tcontent", Duration.ofSeconds(15)).startsWith("OK"), "create");
            int follower = -1;
            for (int i = 0; i < 3; i++) if (t.call(i, "/status", null, null, null).body().contains("role=follower")) { follower = i; break; }
            check(follower >= 0, "follower discovered");
            int n = follower;
            String origin = t.call(n, "/social/likes/queue", null, null, null).body().substring(7);
            for (int i = 0; i < 3; i++) if (i == n) t.isolate(i); else t.call(i, "/test/block", "" + n, null, null);
            String command = "LIKE\tp\tu\tphone\t1\ttrue";
            check(request(t, n, "/social/likes/pending", origin, "one", command).body().equals("PENDING"), "accept isolated intent");
            check(request(t, n, "/social/likes/pending", origin, "one", command).body().equals("PENDING"), "retry same intent");
            check(request(t, n, "/social/likes/pending", origin, "one", command.replace("true", "false")).statusCode() == 400, "payload conflict");
            check(request(t, n, "/social/likes/pending", "wrong", "one", command).statusCode() == 400, "origin pinning");
            Thread.sleep(1200); check(status(t, n, origin, "one").equals("PENDING"), "no partition bypass");
            t.stop(n); t.start(n); t.isolate(n);
            check(t.call(n, "/social/likes/queue", null, null, null).body().equals("origin=" + origin), "restart same incarnation");
            Thread.sleep(1000); check(status(t, n, origin, "one").equals("PENDING"), "restart still partitioned");
            t.heal(); await(t, n, origin, "one", "COMMITTED\nOK LIKE");
            check(client.execute(UUID.randomUUID().toString(), "GET\tp", Duration.ofSeconds(15)).contains("\"likes\":1"), "applied once");
            request(t, n, "/social/likes/pending", origin, "three", "LIKE\tp\tu\tphone\t3\ttrue");
            await(t, n, origin, "three", "COMMITTED");
            request(t, n, "/social/likes/pending", origin, "two", "LIKE\tp\tu\tphone\t2\tfalse");
            await(t, n, origin, "two", "COMMITTED\nOK STALE");
            request(t, n, "/social/likes/pending", origin, "missing", "LIKE\tmissing\tu\tphone\t1\ttrue");
            await(t, n, origin, "missing", "REJECTED\nERR post missing");
            String terminal = status(t, n, origin, "one");
            t.stop(n); t.start(n);
            check(status(t, n, origin, "one").equals(terminal), "terminal restart stable");
            check(client.execute(UUID.randomUUID().toString(), "GET\tp", Duration.ofSeconds(15)).contains("\"likes\":1"), "reordered exact count");
            client.execute("create-q", "CREATE\tq\towner\tcontent", Duration.ofSeconds(15));
            for (int i = 0; i < 3; i++) if (i == n) t.isolate(i); else t.call(i, "/test/block", "" + n, null, null);
            request(t, n, "/social/likes/pending", origin, "deleted", "LIKE\tq\tu\tphone\t1\ttrue");
            check(client.execute("delete-q", "DELETE\tq\towner\t1", Duration.ofSeconds(15)).startsWith("OK"), "majority deletion while pending");
            t.heal(); await(t, n, origin, "deleted", "REJECTED\nERR post deleted");
            // Force the journal's terminal save to fail after a real Raft commit.
            for (int i = 0; i < 3; i++) if (i == n) t.isolate(i); else t.call(i, "/test/block", "" + n, null, null);
            request(t, n, "/social/likes/pending", origin, "lost-result", "LIKE\tp\tv\tphone\t1\ttrue");
            java.nio.file.Path temp = t.root.resolve("node" + n + "/pending-likes/queue.wal");
            java.nio.file.Path saved = temp.resolveSibling("queue.wal.saved");
            java.nio.file.Files.move(temp, saved);
            java.nio.file.Files.createDirectory(temp); t.heal();
            long end = System.nanoTime() + Duration.ofSeconds(15).toNanos(); boolean committed = false;
            while (System.nanoTime() < end) {
                if (client.execute(UUID.randomUUID().toString(), "GET\tp", Duration.ofSeconds(5)).contains("\"likes\":2")) { committed = true; break; }
            }
            check(committed, "Raft applied before queue result save");
            Thread.sleep(400); t.stop(n); java.nio.file.Files.delete(temp); java.nio.file.Files.move(saved, temp); t.start(n);
            await(t, n, origin, "lost-result", "COMMITTED\nOK LIKE");
            check(client.execute(UUID.randomUUID().toString(), "GET\tp", Duration.ofSeconds(15)).contains("\"likes\":2"), "real lost result replay exactly once");
            System.out.println("PENDING_LIKES_CLUSTER_TEST_PASSED follower routing, partition, restart, dedup, stale sequence, rejection");
        }
    }
}

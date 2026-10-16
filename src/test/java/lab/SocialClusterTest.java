package lab;

import java.net.URI;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/** Narrated real-process delivery test; all network partitions are loopback test controls. */
public final class SocialClusterTest {
    private static final Duration DEADLINE = Duration.ofSeconds(15);
    private static void check(boolean condition, String message) { SocialStateTest.check(condition, message); }
    private static String command(SqlClient client, String text) throws Exception {
        return client.execute(UUID.randomUUID().toString(), text, DEADLINE);
    }
    private static void views(CounterClusterTest t, int node, String post, String expected) throws Exception {
        long end = System.nanoTime() + DEADLINE.toNanos();
        String last = "";
        while (System.nanoTime() < end) {
            last = t.call(node, "/social/views", post, null, null).body();
            if (last.equals("OK EVENTUAL " + expected)) return;
            Thread.sleep(60);
        }
        throw new AssertionError("view convergence node=" + node + " last=" + last);
    }
    private static void deleted(CounterClusterTest t, int node) throws Exception {
        long end = System.nanoTime() + DEADLINE.toNanos();
        while (System.nanoTime() < end) {
            if (t.call(node, "/social/views", "p", null, null).statusCode() == 400) return;
            Thread.sleep(60);
        }
        throw new AssertionError("tombstone not applied");
    }
    public static void main(String[] args) throws Exception {
        try (var t = new CounterClusterTest()) {
            System.out.println("EVIDENCE_DIR=" + t.root);
            for (int i = 0; i < 3; i++) t.start(i);
            var peers = IntStream.range(0, 3).mapToObj(i -> URI.create("http://127.0.0.1:" + t.ports[i])).toList();
            var client = SqlClient.social(peers, Duration.ofSeconds(4), Duration.ofMillis(40));
            check(command(client, "CREATE\tp\towner\tobject://post").equals("OK CREATED version=1"), "post create");
            check(command(client, "CREATE\tq\towner\tobject://other").startsWith("OK"), "second post");
            check(client.execute("like1", "LIKE\tp\tu\tphone\t1\ttrue", DEADLINE).endsWith("count=1"), "first like");
            String latest = client.execute("like3", "LIKE\tp\tu\tphone\t3\ttrue", DEADLINE);
            check(latest.endsWith("count=1"), "desired true twice counts once");
            check(command(client, "LIKE\tp\tu\tphone\t2\tfalse").startsWith("OK STALE"), "reorder 1,3,2");
            check(command(client, "LIKE\tp\tu\tphone\t3\tfalse").startsWith("ERR"), "equal sequence conflict");
            check(command(client, "LIKE\tp\tu\tlaptop\t4\tfalse").equals("ERR writer conflict"), "writer conflict");
            check(client.execute("like3", "LIKE\tp\tu\tphone\t3\tfalse", DEADLINE).startsWith("ERR request_id"), "payload-bound retry");
            check(t.call(0, "/sql", SocialState.PREFIX + "GET\tp", "bypass", null).statusCode() == 400, "no SQL dialect bypass");
            check(t.call(0, "/social", "GET\tp", "invalid id", null).statusCode() == 400, "invalid request ID");
            var pool = Executors.newFixedThreadPool(4);
            try {
                var tasks = IntStream.range(0, 8).<java.util.concurrent.Callable<String>>mapToObj(i -> () -> {
                    String text = "LIKE\tp\tuser" + i + "\twriter\t1\ttrue";
                    String first = client.execute("parallel" + i, text, DEADLINE);
                    check(first.equals(client.execute("parallel" + i, text, DEADLINE)), "concurrent retry result");
                    return first;
                }).toList();
                for (var result : pool.invokeAll(tasks)) check(result.get().startsWith("OK"), "parallel likes");
            } finally { pool.shutdownNow(); pool.awaitTermination(5, TimeUnit.SECONDS); }
            check(command(client, "GET\tp").contains("\"likes\":9"), "exact distinct-user count");
            System.out.println("PASS ordered posts, likes seq1/3/2, writer conflict, concurrent retries and exact count=9");
            String[] origin = new String[3];
            for (int i = 0; i < 3; i++) {
                views(t, i, "p", "0");
                origin[i] = t.call(i, "/social/views/status", null, null, null).body().split("\n")[0].substring(7);
            }
            for (int i = 0; i < 3; i++) t.isolate(i);
            String firstView = t.call(0, "/social/view", "p", "event", origin[0]).body();
            check(firstView.startsWith("LOCAL_DURABLE"), "partition local view");
            for (int i = 1; i < 3; i++) check(t.call(i, "/social/view", "p", "event", origin[i]).body().startsWith("LOCAL_DURABLE"), "regional view");
            check(t.call(1, "/social/view", "p", "event", origin[0]).statusCode() == 400, "cross-origin retry rejected");
            check(t.call(0, "/social/view", "q", "event", origin[0]).statusCode() == 400, "event payload reuse rejected");
            for (int i = 0; i < 3; i++) {
                String result = t.call(i, "/social", "LIKE\tp\tisolated\twriter\t1\ttrue", "uncertain", null).body();
                check(result.startsWith("NOT_LEADER") || result.startsWith("UNKNOWN"), "no minority success: " + result);
            }
            t.stop(0); t.start(0); t.isolate(0);
            check(t.call(0, "/social/view", "p", "event", origin[0]).body().equals(firstView), "view restart dedup");
            t.heal();
            // UNKNOWN may later commit: retry the exact request rather than claiming cancellation.
            check(client.execute("uncertain", "LIKE\tp\tisolated\twriter\t1\ttrue", DEADLINE).startsWith("OK"), "resolve uncertain");
            for (int i = 0; i < 3; i++) views(t, i, "p", "3");
            check(command(client, "GET\tp").contains("\"likes\":10"), "uncertain retry exactly once");
            System.out.println("PASS all-partition local views, no minority Raft success, origin retries, heal views=3 likes=10");
            int leader = -1;
            for (int i = 0; i < 3; i++) if (t.call(i, "/status", null, null, null).body().contains("role=leader\n")) leader = i;
            check(leader >= 0, "leader discovered");
            t.stop(leader);
            check(client.execute("like3", "LIKE\tp\tu\tphone\t3\ttrue", DEADLINE).equals(latest), "new leader retains original result");
            check(command(client, "GET\tp").contains("\"likes\":10"), "leader loss count");
            t.start(leader);
            for (int i = 0; i < 3; i++) views(t, i, "p", "3");
            System.out.println("PASS leader process loss, new-leader retry and exact committed count");
            t.call(1, "/test/social-views-drop-ack", "true", null, null);
            t.call(0, "/test/block", "2", null, null); t.isolate(2);
            String lostAck = t.call(0, "/social/view", "p", "lost-ack", origin[0]).body();
            check(lostAck.startsWith("LOCAL_DURABLE"), "lost ACK accepted locally");
            views(t, 1, "p", "4");
            t.stop(0); t.start(0);
            check(t.call(0, "/social/view", "p", "lost-ack", origin[0]).body().equals(lostAck), "lost ACK restart retry");
            t.call(1, "/test/social-views-drop-ack", "false", null, null); t.heal();
            for (int i = 0; i < 3; i++) views(t, i, "p", "4");
            // Stale isolated metadata deliberately permits telemetry after remote deletion.
            t.isolate(2); t.call(0, "/test/block", "2", null, null); t.call(1, "/test/block", "2", null, null);
            check(command(client, "DELETE\tp\towner\t1").equals("OK DELETE version=2"), "majority tombstone");
            check(t.call(2, "/social/view", "p", "stale-telemetry", origin[2]).body().startsWith("LOCAL_DURABLE"), "documented stale admission");
            t.heal(); for (int i = 0; i < 3; i++) deleted(t, i);
            check(command(client, "UPDATE\tp\towner\t2\tnew").equals("ERR post deleted"), "no late update");
            check(command(client, "CREATE\tp\towner\tnew").startsWith("ERR"), "no recreate");
            check(command(client, "LIKE\tp\tu\tphone\t4\tfalse").equals("ERR post deleted"), "no deleted like");
            check(t.call(0, "/social/view", "q", "persistent-q", origin[0]).body().startsWith("LOCAL_DURABLE"), "surviving post view");
            for (int i = 0; i < 3; i++) views(t, i, "q", "1");
            check(command(client, "LIKE\tq\tu\tphone\t1\ttrue").endsWith("count=1"), "surviving post relationship");
            for (int i = 0; i < 3; i++) t.stop(i);
            for (int i = 0; i < 3; i++) t.start(i);
            check(command(client, "GET\tp").equals("OK POST {\"version\":2,\"deleted\":true}"), "restart tombstone");
            check(client.execute("like3", "LIKE\tp\tu\tphone\t3\ttrue", DEADLINE).equals(latest), "historical request result on restart");
            check(command(client, "GET_LIKE\tp\tu").equals("ERR post deleted"), "fresh read hides historical relationship");
            check(command(client, "GET\tq").contains("\"likes\":1"), "restart derived count");
            for (int i = 0; i < 3; i++) views(t, i, "q", "1");
            for (int i = 0; i < 3; i++) deleted(t, i);
            System.out.println("PASS lost ACK, restart dedup, stale-region telemetry boundary, tombstones and full-cluster replay");
            System.out.println("SOCIAL_CLUSTER_TEST_PASSED");
        }
    }
}

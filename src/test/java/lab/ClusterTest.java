package lab;

import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

/** Real JVM processes and HTTP transport; bounded loopback fault-injection study. */
public final class ClusterTest implements AutoCloseable {
    private final Path root = Files.createTempDirectory("minisql-raft-test-");
    private final Process[] processes = new Process[3];
    private final int[] ports = new int[3];
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(200)).build();
    private int requestSequence;
    public ClusterTest() throws Exception {
        var reservations = new ArrayList<ServerSocket>();
        for (int i = 0; i < 3; i++) {var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
            reservations.add(socket); ports[i] = socket.getLocalPort();}
        for (var socket : reservations) socket.close();
    }
    private void start(int id) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String addresses = ports[0]+","+ports[1]+","+ports[2];
        processes[id] = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
            "lab.Main", ""+id, root.resolve("node"+id).toString(), addresses, "--test-controls")
            .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(root.resolve("node"+id+".log").toFile())).start();
    }
    private String call(int id, String path, String body, String requestId) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+ports[id]+path))
            .timeout(Duration.ofSeconds(5));
        if (body == null) builder.GET();
        else {builder.POST(HttpRequest.BodyPublishers.ofString(body));
            if (requestId != null) builder.header("X-Request-ID", requestId);}
        var response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) throw new AssertionError("HTTP "+response.statusCode()+": "+response.body());
        return response.body();
    }
    private Map<String, String> status(int id) throws Exception {
        var result = new HashMap<String, String>();
        for (String line : call(id, "/status", null, null).split("\n")) {
            var pair = line.split("=", 2); result.put(pair[0], pair[1]);
        }
        return result;
    }
    private int leader(Set<Integer> allowed) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(12).toNanos();
        while (System.nanoTime() < deadline) {
            for (int i : allowed) try {
                if (processes[i] != null && processes[i].isAlive() && status(i).get("role").equals("leader")) return i;
            } catch (Exception ignored) {}
            Thread.sleep(80);
        }
        throw new AssertionError("no leader; evidence="+root);
    }
    private String sql(int id, String statement) throws Exception {
        return call(id, "/sql", statement, "test-"+(requestSequence++));
    }
    private static void expect(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    private void stop(int id) throws Exception {
        if (processes[id] != null && processes[id].isAlive()) {
            processes[id].destroyForcibly(); processes[id].waitFor();
        }
    }
    private void converge() throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(12).toNanos();
        while (System.nanoTime() < deadline) {
            try {
                int commit = Integer.parseInt(status(leader(Set.of(0,1,2))).get("committed"));
                boolean all = true;
                for (int i = 0; i < 3; i++) all &= Integer.parseInt(status(i).get("applied")) == commit;
                if (all) return;
            } catch (Exception ignored) {}
            Thread.sleep(80);
        }
        throw new AssertionError("replicas did not converge");
    }
    private void run() throws Exception {
        System.out.println("EVIDENCE_DIR="+root);
        for (int i = 0; i < 3; i++) start(i);
        int lead = leader(Set.of(0,1,2));
        expect(sql(lead, "CREATE TABLE counters (id LONG PRIMARY KEY, value LONG);").equals("OK CREATE"), "create");
        expect(sql(lead, "INSERT INTO counters VALUES (1, 0);").equals("OK INSERT 1"), "insert");
        expect(sql(lead, "INSERT INTO counters VALUES (2, 7);").equals("OK INSERT 1"), "insert deletable row");
        expect(sql(lead, "DELETE FROM counters WHERE id = 2;").equals("OK DELETE 1"), "replicated delete");
        expect(sql(lead, "SELECT value FROM counters WHERE id = 2;").contains("\"rows\":[]"), "deleted row absent");
        String increment = "UPDATE counters SET value = value + 1 WHERE id = 1;";
        expect(call(lead, "/sql", increment, "increment-once").equals("OK UPDATE 1"), "increment");
        expect(call(lead, "/sql", increment, "increment-once").equals("OK UPDATE 1"), "duplicate");
        expect(call(lead, "/sql", increment.replace("+ 1", "+ 2"), "increment-once").startsWith("ERR"), "payload binding");
        expect(sql(lead, "SELECT value FROM counters WHERE id = 1;").contains("[[1]]"), "value after retry");
        System.out.println("PASS replicated CRUD and payload-bound duplicate retry");
        converge();
        stop(lead);
        var survivors = new HashSet<>(Set.of(0,1,2)); survivors.remove(lead);
        int replacement = leader(survivors);
        expect(call(replacement, "/sql", increment, "increment-once").equals("OK UPDATE 1"), "dedup after failover");
        expect(sql(replacement, "SELECT value FROM counters WHERE id = 1;").contains("[[1]]"), "failover value");
        start(lead); converge();
        System.out.println("PASS abrupt leader kill, election, retry and restarted follower catch-up");

        // An isolated old leader may still believe it is leader, but cannot commit.
        lead = leader(Set.of(0,1,2));
        survivors = new HashSet<>(Set.of(0,1,2)); survivors.remove(lead);
        call(lead, "/test/block", String.join(",", survivors.stream().map(Object::toString).toList()), null);
        for (int i : survivors) call(i, "/test/block", ""+lead, null);
        String unknown = call(lead, "/sql", increment, "partition-retry");
        expect(unknown.startsWith("UNKNOWN"), "minority write must not succeed: "+unknown);
        expect(sql(lead, "SELECT value FROM counters WHERE id = 1;").startsWith("UNKNOWN"), "minority strong read");
        replacement = leader(survivors);
        expect(call(replacement, "/sql", increment, "partition-retry").equals("OK UPDATE 1"), "majority write");
        for (int i = 0; i < 3; i++) call(i, "/test/block", "", null);
        converge();
        lead = leader(Set.of(0,1,2));
        expect(sql(lead, "SELECT value FROM counters WHERE id = 1;").contains("[[2]]"), "partition healing value");
        System.out.println("PASS minority cannot confirm write/read; majority progresses; divergent log healed");
        converge();
        for (int i = 0; i < 3; i++) stop(i);
        for (int i = 0; i < 3; i++) start(i);
        lead = leader(Set.of(0,1,2));
        expect(sql(lead, "SELECT value FROM counters WHERE id = 1;").contains("[[2]]"), "full restart");
        expect(call(lead, "/sql", increment, "partition-retry").equals("OK UPDATE 1"), "durable dedup");
        expect(sql(lead, "SELECT value FROM counters WHERE id = 1;").contains("[[2]]"), "restart dedup value");
        System.out.println("PASS all-node restart, full-log SQL replay and durable dedup");
        var pool = java.util.concurrent.Executors.newFixedThreadPool(4);
        try {
            final int currentLeader = lead;
            var jobs = new ArrayList<java.util.concurrent.Callable<String>>();
            for (int i = 0; i < 16; i++) {
                final String key = "concurrent-" + i;
                jobs.add(() -> call(currentLeader, "/sql", increment, key));
            }
            for (var result : pool.invokeAll(jobs)) expect(result.get().equals("OK UPDATE 1"), "concurrent increment");
            expect(sql(lead, "SELECT value FROM counters WHERE id = 1;").contains("[[18]]"), "concurrent final value");
        } finally {pool.shutdownNow();}
        converge();
        for (int i = 0; i < 3; i++) stop(i);
        for (int i = 0; i < 3; i++) {
            var engine = new SqlEngine();
            var seen = new HashSet<String>();
            try (var storage = new DurableState(root.resolve("node"+i))) {
                var state = storage.load();
                for (var entry : state.entries().subList(0, state.committed()))
                    if (!entry.requestId().isEmpty() && seen.add(entry.requestId())) engine.execute(entry.sql());
            }
            expect(engine.execute("SELECT value FROM counters WHERE id = 1;").contains("[[18]]"),
                   "persisted replica replay " + i);
            expect(engine.execute("SELECT value FROM counters WHERE id = 2;").contains("\"rows\":[]"),
                   "persisted replica delete " + i);
        }
        System.out.println("PASS concurrent writes and independently replayed persisted state of all three replicas");
        System.out.println("CLUSTER_TEST_PASSED");
    }
    @Override public void close() throws Exception { for (int i=0;i<3;i++) stop(i); }
    public static void main(String[] args) throws Exception {
        try (var test = new ClusterTest()) {test.run();}
    }
}

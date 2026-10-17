package lab;

import java.net.URI;
import java.nio.file.*;
import java.util.*;
import static lab.Protocol.*;

/** Deterministic state, suffix, and atomic replacement fault tests without election timing. */
public final class SnapshotTest {
    static final Map<Integer, URI> PEERS = Map.of(0, URI.create("http://127.0.0.1:22000"),
        1, URI.create("http://127.0.0.1:22001"), 2, URI.create("http://127.0.0.1:22002"));
    static void check(boolean value, String message) { if (!value) throw new AssertionError(message); }
    static Entry sql(String id, String sql) { return new Entry(1, id, sql); }
    static Entry social(String id, String command) { return sql("social:" + id, SocialState.PREFIX + command); }
    public static void main(String[] args) throws Exception {
        Path source = Files.createTempDirectory("snapshot-source-");
        List<Entry> entries = List.of(sql("create", "CREATE TABLE t (id LONG PRIMARY KEY, n LONG)"),
            sql("insert", "INSERT INTO t VALUES (1, 0)"), sql("read", "SELECT * FROM t"),
            sql("increment", "UPDATE t SET n = n + 1 WHERE id = 1"), sql("error", "SELECT * FROM missing"),
            social("post", "CREATE\tp\tu\tcontent"), social("like", "LIKE\tp\tu\tw\t1\ttrue"),
            social("unlike", "LIKE\tp\tu\tw\t2\tfalse"), social("delete", "DELETE\tp\tu\t1"));
        try (var node = new RaftNode(0, PEERS, source)) {
            node.append(new Append(1, 1, 0, 0, entries.size(), entries)); node.takeSnapshot();
        }
        DurableState.State state;
        try (var store = new DurableState(source)) { state = store.load(); }
        check(state.baseIndex() == entries.size() && state.entries().isEmpty(), "prefix compacted");
        var restored = SnapshotState.decode(state.snapshot());
        check(restored.database().execute("SELECT * FROM t").contains("[[1,1]]"), "SQL state");
        check(restored.results().get("read").result().contains("[[1,0]]"), "historical read");
        check(restored.results().get("error").result().startsWith("ERR"), "historical error");
        check(restored.social().execute("CREATE\tp\tu\tnew").startsWith("ERR"), "tombstone");
        check(restored.results().get("social:unlike").result().contains("liked=false"), "exact unlike result");
        try (var node = new RaftNode(0, PEERS, source)) {
            check(node.status().contains("applied=9"), "restart applied");
            // Repeated committed IDs in a suffix must not reapply SQL increments.
            node.append(new Append(2, 1, 9, 1, 10, List.of(new Entry(2, "increment", entries.get(3).sql()))));
            node.takeSnapshot();
        }
        try (var store = new DurableState(source)) {
            check(SnapshotState.decode(store.load().snapshot()).database().execute("SELECT * FROM t").contains("[[1,1]]"), "dedup after restart");
        }
        for (boolean match : List.of(true, false)) {
            Path target = Files.createTempDirectory("snapshot-target-");
            try (var node = new RaftNode(2, PEERS, target)) {
                var prefix = new ArrayList<Entry>();
                for (int i = 1; i <= 10; i++) prefix.add(new Entry(match ? 1 : 2, "", ""));
                node.append(new Append(2, 1, 0, 0, 0, prefix));
                node.install(new Install(2, 1, 9, 1, state.snapshot()));
                check(node.status().contains("last=" + (match ? 10 : 9) + "\n"), "suffix retention");
                node.install(new Install(2, 1, 9, 1, state.snapshot()));
                check(node.status().contains("applied=9"), "repeat install");
            }
        }
        Path failure = Files.createTempDirectory("snapshot-failure-");
        try (var node = new RaftNode(0, PEERS, failure)) {
            node.append(new Append(1, 1, 0, 0, entries.size(), entries));
            Files.createDirectory(failure.resolve("state.bin.tmp"));
            try { node.takeSnapshot(); throw new AssertionError("write failure accepted"); } catch (java.io.IOException expected) {}
            check(node.status().contains("failed=true"), "fail closed");
        }
        Files.delete(failure.resolve("state.bin.tmp"));
        try (var node = new RaftNode(0, PEERS, failure)) { check(node.status().contains("applied=9"), "old state after failed checkpoint"); }
        // Large exact results are legal even though ordinary RPC text fields are smaller.
        var results = new HashMap<String, SnapshotState.Result>();
        results.put("big", new SnapshotState.Result("SELECT", "x".repeat(70000)));
        check(SnapshotState.decode(SnapshotState.encode(new SqlEngine(), new SocialState(), results)).results().equals(results), "large result");
        System.out.println("SNAPSHOT_TEST_PASSED");
    }
}

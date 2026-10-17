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
        SocialState active = new SocialState(); active.execute("CREATE\tp\tu\tcontent");
        active.execute("LIKE\tp\tu\tw\t2\tfalse");
        var activeRestored = SnapshotState.decode(SnapshotState.encode(new SqlEngine(), active, Map.of()));
        check(activeRestored.social().execute("LIKE\tp\tu\tw\t1\ttrue").equals("OK STALE current=2"), "false relationship sequence retained");
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
                node.install(new Install(3, 1, 9, 1, state.snapshot()));
            }
            try (var store = new DurableState(target)) { check(store.load().term() == 3 && store.load().votedFor() == -1, "stale snapshot updates hardstate"); }
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
        Path installFailure = Files.createTempDirectory("snapshot-install-failure-");
        try (var node = new RaftNode(2, PEERS, installFailure)) {
            node.vote(new Vote(2, 1, 0, 0));
            Files.createDirectory(installFailure.resolve("state.bin.tmp"));
            try { node.install(new Install(2, 1, 9, 1, state.snapshot())); throw new AssertionError("failed install ACK"); }
            catch (java.io.IOException expected) {}
        }
        Files.delete(installFailure.resolve("state.bin.tmp"));
        try (var node = new RaftNode(2, PEERS, installFailure)) {
            check(node.status().contains("applied=0"), "failed install leaves old state");
            check(node.install(new Install(2, 1, 9, 1, state.snapshot())).success(), "retry install after restart");
        }
        for (String phase : List.of("after-file-force", "after-rename", "after-directory-force")) {
            Path crash = Files.createTempDirectory("snapshot-crash-");
            var old = new DurableState.State(1, 1, entries.size(), entries);
            try (var store = new DurableState(crash)) { store.save(old); }
            try (var store = new DurableState(crash, at -> { if (at.equals(phase)) throw new java.io.IOException("injected crash"); })) {
                try { store.save(state); throw new AssertionError("fault absent"); } catch (java.io.IOException expected) {}
            }
            try (var store = new DurableState(crash)) {
                check(store.load().equals(phase.equals("after-file-force") ? old : state), "atomic crash boundary " + phase);
            }
        }
        Path legacy = Files.createTempDirectory("snapshot-legacy-");
        byte[] oldBody = Protocol.encode(out -> { out.writeInt(0x4d535131); out.writeLong(1); out.writeInt(1);
            out.writeInt(entries.size()); out.writeInt(entries.size()); for (Entry e : entries) Protocol.entry(out, e); });
        byte[] checksum = java.security.MessageDigest.getInstance("SHA-256").digest(oldBody);
        Files.write(legacy.resolve("state.bin"), java.nio.ByteBuffer.allocate(oldBody.length + checksum.length).put(oldBody).put(checksum).array());
        try (var store = new DurableState(legacy)) {
            var loaded = store.load(); check(loaded.baseIndex() == 0 && loaded.entries().equals(entries), "legacy load");
            store.save(loaded); check(store.load().equals(loaded), "legacy rewrite");
        }
        byte[] corrupt = state.snapshot().clone(); corrupt[0] ^= 1;
        byte[] wire = Protocol.install(new Install(2, 1, 9, 1, state.snapshot()));
        check(Protocol.readInstall(wire).index() == 9, "snapshot wire roundtrip");
        wire[wire.length - 40] ^= 1;
        try { Protocol.readInstall(wire); throw new AssertionError("corrupt wire accepted"); } catch (java.io.IOException expected) {}
        wire = Protocol.install(new Install(2, 1, 9, 1, state.snapshot())); wire[15] ^= 1;
        try { Protocol.readInstall(wire); throw new AssertionError("corrupt boundary accepted"); } catch (java.io.IOException expected) {}
        try (var node = new RaftNode(2, PEERS, Files.createTempDirectory("snapshot-corrupt-"))) {
            try { node.install(new Install(2, 1, 9, 1, corrupt)); throw new AssertionError("corrupt install"); } catch (java.io.IOException expected) {}
            check(node.status().contains("applied=0"), "corrupt snapshot not applied");
        }
        // Large exact results are legal even though ordinary RPC text fields are smaller.
        var results = new HashMap<String, SnapshotState.Result>();
        results.put("big", new SnapshotState.Result("SELECT", "x".repeat(70000)));
        check(SnapshotState.decode(SnapshotState.encode(new SqlEngine(), new SocialState(), results)).results().equals(results), "large result");
        Path capacity = Files.createTempDirectory("snapshot-capacity-");
        try (var node = new RaftNode(0, PEERS, capacity)) {
            var many = new ArrayList<Entry>();
            many.add(sql("table", "CREATE TABLE large (id LONG PRIMARY KEY, t TEXT)"));
            many.add(sql("row", "INSERT INTO large VALUES (1, '" + "x".repeat(8000) + "')"));
            for (int i = 0; i < 126; i++) many.add(sql("read-" + i, "SELECT * FROM large"));
            node.append(new Append(1, 1, 0, 0, 64, many.subList(0, 64)));
            node.append(new Append(1, 1, 64, 1, 128, many.subList(64, 128)));
            check(node.status().contains("applied=128") && node.status().contains("snapshot=0") && node.status().contains("failed=false"), "oversize auto snapshot keeps committed logs");
            try { node.takeSnapshot(); throw new AssertionError("oversize explicit snapshot"); } catch (SnapshotState.TooLarge expected) {}
        }
        System.out.println("SNAPSHOT_TEST_PASSED");
    }
}

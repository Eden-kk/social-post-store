package lab;
import lab.counter.*;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

public final class CounterTest {
    static void check(boolean c, String m) { if (!c) throw new AssertionError(m); }
    interface Action { void run() throws Exception; }
    static void rejects(Action a) throws Exception { boolean rejected = false; try { a.run(); } catch (Exception e) { rejected = true; } check(rejected, "expected rejection"); }
    public static void main(String[] args) throws Exception {
        var a = new CounterState().add("k", "a", 7).add("k", "a", -2);
        var b = new CounterState().add("k", "b", 9); var c = new CounterState().add("k", "c", -3);
        check(a.merge(b).equals(b.merge(a)), "commutative");
        check(a.merge(b).merge(c).equals(a.merge(b.merge(c))), "associative");
        check(a.merge(a).equals(a), "idempotent");
        check(a.merge(b).merge(c).value("k").intValueExact() == 11, "exact sum");
        check(a.merge(new CounterState().add("k", "a", 1)).equals(a), "old snapshot");
        var huge = new CounterState().add("k", "a", Long.MAX_VALUE).add("k", "b", Long.MAX_VALUE);
        check(huge.value("k").toString().equals("18446744073709551614"), "BigInteger sum");
        rejects(() -> huge.add("k", "a", 1)); rejects(() -> a.add("k", "a", Long.MIN_VALUE));
        rejects(() -> CounterState.read(new java.io.DataInputStream(new java.io.ByteArrayInputStream(new byte[]{-1,-1,-1,-1}))));
        var peers = Map.of(0, URI.create("http://127.0.0.1:9010"), 1, URI.create("http://127.0.0.1:9011"));
        Path dir = Files.createTempDirectory("counter-unit-"); String origin, result;
        try (var s = new CounterStore(dir, 0, peers)) {
            origin = s.origin(); result = s.add(origin, "one", "k", 5);
            check(s.add(origin, "one", "k", 5).equals(result), "retry");
            rejects(() -> s.add(origin, "one", "k", 6)); rejects(() -> s.add("other", "two", "k", 5));
            rejects(() -> { try (var duplicate = new CounterStore(dir, 0, peers)) {} });
            var sent = s.snapshot(); s.add(origin, "two", "k", 2); s.acknowledge(1, sent.version());
            check(s.pending() == 1, "old ACK retains newer update");
            s.merge(b); s.merge(b); check(s.value("k").equals("16"), "duplicate merge");
            s.add(origin, "other-key", "other", -2); check(s.value("other").equals("-2"), "independent key");
        }
        try (var s = new CounterStore(dir, 0, peers)) {
            check(s.origin().equals(origin), "stable identity"); check(s.pending() == 1, "durable outbox");
            check(s.add(origin, "one", "k", 5).equals(result), "durable retry"); check(s.value("k").equals("16"), "durable value");
            rejects(() -> s.merge(new CounterState().add("k", origin, 99))); rejects(() -> s.add(origin, "later", "k", 1));
        }
        rejects(() -> { try (var wrong = new CounterStore(dir, 1, peers)) {} });
        byte[] bytes = Files.readAllBytes(dir.resolve("counter.bin")); bytes[10] ^= 1; Files.write(dir.resolve("counter.bin"), bytes);
        rejects(() -> { try (var corrupt = new CounterStore(dir, 0, peers)) {} });
        Files.delete(dir.resolve("counter.bin")); rejects(() -> { try (var missing = new CounterStore(dir, 0, peers)) {} });
        try (var fresh = new CounterStore(Files.createTempDirectory("counter-fresh-"), 0, peers)) {
            check(!fresh.origin().equals(origin), "fresh incarnation"); rejects(() -> fresh.add(origin, "one", "k", 5));
        }
        Path noIdentity = Files.createTempDirectory("counter-noidentity-");
        try (var s = new CounterStore(noIdentity, 0, peers)) { s.add(s.origin(), "one", "k", 1); }
        Files.delete(noIdentity.resolve("identity.bin"));
        rejects(() -> { try (var invalid = new CounterStore(noIdentity, 0, peers)) {} });
        Path failedDisk = Files.createTempDirectory("counter-io-failure-");
        try (var s = new CounterStore(failedDisk, 0, peers)) {
            String identity = s.origin(); Files.createDirectory(failedDisk.resolve("counter.tmp"));
            rejects(() -> s.add(identity, "failed", "k", 1)); rejects(() -> s.value("k"));
        }
        Files.delete(failedDisk.resolve("counter.tmp"));
        try (var s = new CounterStore(failedDisk, 0, peers)) { check(s.value("k").equals("0"), "failed persistence did not mutate state"); }
        System.out.println("COUNTER_TEST_PASSED");
    }
}

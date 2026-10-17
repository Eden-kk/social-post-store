package lab;

import java.nio.file.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class PendingLikesTest {
    private static final String LIKE = "LIKE\tp\tu\tphone\t1\ttrue";
    private static void check(boolean value, String text) { if (!value) throw new AssertionError(text); }
    interface Action { void run() throws Exception; }
    private static void rejects(Action action) throws Exception {
        try { action.run(); } catch (Exception expected) { return; }
        throw new AssertionError("expected rejection");
    }
    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("pending-unit-"); String origin;
        try (var q = new PendingLikes(dir, 2, (id, body) -> "UNKNOWN")) {
            origin = q.origin();
            check(q.enqueue(origin, "a", LIKE).equals("PENDING"), "durable admission");
            check(q.enqueue(origin, "a", LIKE).equals("PENDING"), "duplicate");
            rejects(() -> q.enqueue(origin, "a", LIKE.replace("true", "false")));
            rejects(() -> q.enqueue("wrong", "a", LIKE));
            rejects(() -> q.enqueue(origin, "bad", "LIKE\tp\tu\tphone\t" + "0".repeat(70000) + "1\ttrue"));
            rejects(() -> new PendingLikes(dir, 2, (id, body) -> "OK"));
            q.enqueue(origin, "b", LIKE); rejects(() -> q.enqueue(origin, "c", LIKE));
            q.retry(); check(q.status(origin, "a").equals("PENDING"), "unknown retains");
        }
        AtomicInteger calls = new AtomicInteger();
        try (var q = new PendingLikes(dir, 2, (id, body) -> { calls.incrementAndGet(); return id.equals("a") ? "OK STALE current=3" : "ERR post deleted"; })) {
            check(q.origin().equals(origin), "origin persisted"); q.retry();
            check(q.status(origin, "a").equals("COMMITTED\nOK STALE current=3"), "exact stale result");
            check(q.status(origin, "b").equals("REJECTED\nERR post deleted"), "terminal error");
            q.retry(); check(calls.get() == 2, "terminal not retried");
            rejects(() -> q.enqueue(origin, "c", LIKE));
        }
        try (var q = new PendingLikes(dir, 2, (id, body) -> { throw new AssertionError("no terminal resubmit"); })) {
            q.retry(); check(q.status(origin, "b").startsWith("REJECTED"), "terminal restart");
        }
        Path crash = Files.createTempDirectory("pending-crash-"); AtomicInteger effects = new AtomicInteger();
        var committed = new java.util.HashMap<String, String>();
        PendingLikes.Submitter apply = (id, body) -> committed.computeIfAbsent(id, key -> { effects.incrementAndGet(); return "OK LIKE count=1"; });
        String crashOrigin;
        try (var q = new PendingLikes(crash, 2, apply)) {
            crashOrigin = q.origin(); q.enqueue(crashOrigin, "a", LIKE);
            Files.createDirectory(crash.resolve("queue.tmp"));
            rejects(q::retry); rejects(() -> q.status(crashOrigin, "a"));
        }
        Files.delete(crash.resolve("queue.tmp"));
        try (var q = new PendingLikes(crash, 2, apply)) {
            check(q.status(crashOrigin, "a").equals("PENDING"), "commit-before-result persists intent");
            q.retry(); check(effects.get() == 1, "same ID applied once after lost result");
            Files.createDirectory(crash.resolve("queue.tmp"));
            rejects(() -> q.enqueue(crashOrigin, "b", LIKE));
        }
        Files.delete(crash.resolve("queue.tmp"));
        try (var q = new PendingLikes(crash, 2, apply)) {
            check(q.status(crashOrigin, "b").equals("NOT_FOUND"), "unacknowledged failed admission absent");
        }
        byte[] bytes = Files.readAllBytes(crash.resolve("queue.bin")); bytes[10] ^= 1; Files.write(crash.resolve("queue.bin"), bytes);
        rejects(() -> new PendingLikes(crash, 2, apply));
        var started = new java.util.concurrent.CountDownLatch(1);
        AtomicInteger blockingCalls = new AtomicInteger();
        try (var q = new PendingLikes(Files.createTempDirectory("pending-stop-"), 2, (id, body) -> {
            blockingCalls.incrementAndGet(); started.countDown(); Thread.sleep(30000); return "UNKNOWN";
        })) {
            q.enqueue(q.origin(), "a", LIKE); q.enqueue(q.origin(), "b", LIKE); q.start();
            check(started.await(3, java.util.concurrent.TimeUnit.SECONDS), "worker started");
        }
        check(blockingCalls.get() == 1, "shutdown interrupt does not process second intent");
        System.out.println("PENDING_LIKES_TEST_PASSED");
    }
}

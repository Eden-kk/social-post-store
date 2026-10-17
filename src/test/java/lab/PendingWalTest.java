package lab;

import java.nio.file.*;
import java.util.*;

public final class PendingWalTest {
    static final String LIKE = "LIKE\tp\tu\tw\t1\ttrue";
    static void check(boolean ok, String text) { if (!ok) throw new AssertionError(text); }
    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("pending-wal-"); String origin;
        byte[] oldWal;
        try (var queue = new PendingLikes(dir, 200, (id, body) -> "OK STALE current=3")) {
            origin = queue.origin(); queue.enqueue(origin, "one", LIKE); queue.retry();
            oldWal = Files.readAllBytes(dir.resolve("queue.wal"));
            queue.checkpoint();
            check(Files.size(dir.resolve("queue.wal")) == 0, "checkpoint compacts WAL");
        }
        // Crash after checkpoint rename+directory force but before WAL truncation.
        Files.write(dir.resolve("queue.wal"), oldWal);
        try (var queue = new PendingLikes(dir, 200, (id, body) -> "ERR deleted")) {
            check(queue.status(origin, "one").equals("COMMITTED\nOK STALE current=3"), "covered prefix ignored");
            queue.enqueue(origin, "two", LIKE);
        }
        Files.write(dir.resolve("queue.wal"), new byte[]{0, 0}, StandardOpenOption.APPEND);
        try (var queue = new PendingLikes(dir, 200, (id, body) -> "ERR deleted")) {
            check(queue.status(origin, "two").equals("PENDING"), "torn tail preserves durable prefix");
            queue.retry();
        }
        try (var queue = new PendingLikes(dir, 200, (id, body) -> "UNKNOWN")) {
            check(queue.status(origin, "two").equals("REJECTED\nERR deleted"), "append after tail repair");
            for (int i = 0; i < 130; i++) queue.enqueue(origin, "auto-" + i, LIKE);
        }
        // Complete valid framing followed by an interrupted payload append is repairable.
        byte[] tail = java.nio.ByteBuffer.allocate(11).putInt(100).putInt(~100).put(new byte[]{1,2,3}).array();
        Files.write(dir.resolve("queue.wal"), tail, StandardOpenOption.APPEND);
        try (var queue = new PendingLikes(dir, 200, (id, body) -> "UNKNOWN")) {
            check(queue.status(origin, "auto-129").equals("PENDING"), "automatic checkpoint recovery");
            check(queue.status(origin, "one").equals("COMMITTED\nOK STALE current=3"), "compaction retains exact results");
            Files.createDirectory(dir.resolve("queue.tmp"));
            try { queue.checkpoint(); throw new AssertionError("checkpoint failure expected"); }
            catch (java.io.IOException expected) { }
        }
        Files.delete(dir.resolve("queue.tmp"));
        try (var queue = new PendingLikes(dir, 200, (id, body) -> "UNKNOWN")) {
            check(queue.status(origin, "auto-129").equals("PENDING"), "failed checkpoint retains old snapshot and WAL");
            queue.enqueue(origin, "after-failure", LIKE);
        }
        byte[] correct = Files.readAllBytes(dir.resolve("queue.wal"));
        byte[] badHeader = correct.clone(); badHeader[3] ^= 1;
        Files.write(dir.resolve("queue.wal"), badHeader);
        try { new PendingLikes(dir, 200, (id, body) -> "UNKNOWN"); throw new AssertionError("corrupt length accepted"); }
        catch (java.io.IOException expected) { }
        Files.write(dir.resolve("queue.wal"), correct);
        byte[] corrupted = Files.readAllBytes(dir.resolve("queue.wal")); corrupted[10] ^= 1;
        Files.write(dir.resolve("queue.wal"), corrupted);
        try { new PendingLikes(dir, 200, (id, body) -> "UNKNOWN"); throw new AssertionError("checksum accepted"); }
        catch (java.io.IOException expected) { }
        // Genuine v1 checkpoint migrates lazily, without changing its incarnation or terminal results.
        Path legacy = Files.createTempDirectory("pending-legacy-");
        byte[] body = Protocol.encode(out -> {
            out.writeInt(0x504c5131); out.writeUTF(origin); out.writeInt(1);
            out.writeUTF("old"); out.writeUTF(LIKE); out.writeUTF("OK SAME count=1");
        });
        byte[] digest = java.security.MessageDigest.getInstance("SHA-256").digest(body);
        byte[] all = Arrays.copyOf(body, body.length + 32); System.arraycopy(digest, 0, all, body.length, 32);
        Files.write(legacy.resolve("queue.bin"), all);
        try (var queue = new PendingLikes(legacy, 2, (id, command) -> "UNKNOWN")) {
            check(queue.status(origin, "old").equals("COMMITTED\nOK SAME count=1"), "legacy exact outcome");
            queue.enqueue(origin, "new", LIKE);
        }
        try (var queue = new PendingLikes(legacy, 2, (id, command) -> "UNKNOWN")) {
            check(queue.status(origin, "new").equals("PENDING"), "legacy checkpoint plus new WAL recovery"); queue.checkpoint();
        }
        try (var queue = new PendingLikes(legacy, 2, (id, command) -> "UNKNOWN")) {
            check(queue.status(origin, "new").equals("PENDING"), "migrated checkpoint recovery");
        }
        System.out.println("PENDING_WAL_TEST_PASSED");
    }
}

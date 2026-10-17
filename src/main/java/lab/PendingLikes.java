package lab;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import static java.nio.file.StandardOpenOption.*;

/** Local durable intent journal. Raft, not this journal, decides the authoritative outcome. */
public final class PendingLikes implements AutoCloseable {
    public interface Submitter { String submit(String id, String command) throws Exception; }
    private record Item(String command, String result) {}
    private final Path directory;
    private final int capacity;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final Submitter submitter;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, Item> items = new LinkedHashMap<>();
    private String origin;
    private boolean failed;
    private long sequence;
    private int sinceCheckpoint;

    public PendingLikes(Path directory, int capacity, Submitter submitter) throws IOException {
        if (capacity < 1 || capacity > 100000) throw new IllegalArgumentException("invalid queue capacity");
        this.directory = directory; this.capacity = capacity; this.submitter = submitter;
        Files.createDirectories(directory);
        lockChannel = FileChannel.open(directory.resolve("queue.lock"), CREATE, WRITE);
        FileLock acquired;
        try { acquired = lockChannel.tryLock(); if (acquired == null) throw new IOException("queue in use"); }
        catch (IOException | RuntimeException e) { lockChannel.close(); throw e; }
        lock = acquired;
        try {
            if (Files.exists(directory.resolve("queue.bin"))) load();
            else {
                if (Files.exists(directory.resolve("queue.wal"))) throw new IOException("WAL without checkpoint");
                origin = UUID.randomUUID().toString(); save();
            }
            replay();
        } catch (IOException | RuntimeException e) { close(); throw e; }
    }
    public static void validate(String id, String command) {
        if (command == null || command.length() > 512) throw new IllegalArgumentException("LIKE command exceeds 512 characters");
        if (id == null || id.isBlank() || id.length() > 120 || id.chars().anyMatch(c -> c < 33 || c > 126))
            throw new IllegalArgumentException("X-Request-ID required, max 120 printable characters");
        String[] p = command.split("\t", -1);
        if (p.length != 6 || !p[0].equals("LIKE")) throw new IllegalArgumentException("LIKE only");
        for (int i = 1; i < 4; i++) SocialState.identifier(p[i]);
        if (Long.parseLong(p[4]) <= 0) throw new IllegalArgumentException("positive sequence required");
        if (!p[5].equals("true") && !p[5].equals("false")) throw new IllegalArgumentException("desired state must be true/false");
    }
    private void check(String expected) throws IOException {
        if (failed) throw new IOException("queue failed closed");
        if (!origin.equals(expected)) throw new IllegalArgumentException("retry must use original pending incarnation");
    }
    public synchronized String origin() { return origin; }
    public synchronized String enqueue(String expected, String id, String command) throws IOException {
        check(expected); validate(id, command);
        Item prior = items.get(id);
        if (prior != null) {
            if (!prior.command.equals(command)) throw new IllegalArgumentException("request ID payload conflict");
            return render(prior);
        }
        if (items.size() >= capacity) throw new IllegalStateException("queue retained-result capacity reached");
        persist(id, new Item(command, ""));
        return "PENDING";
    }
    public synchronized String status(String expected, String id) throws IOException {
        check(expected);
        if (id == null || id.isBlank() || id.length() > 120 || id.chars().anyMatch(c -> c < 33 || c > 126))
            throw new IllegalArgumentException("invalid request ID");
        Item item = items.get(id);
        return item == null ? "NOT_FOUND" : render(item);
    }
    private String render(Item item) {
        return item.result.isEmpty() ? "PENDING" : (item.result.startsWith("OK") ? "COMMITTED\n" : "REJECTED\n") + item.result;
    }
    public void start() { worker.scheduleWithFixedDelay(this::retrySafely, 200, 300, TimeUnit.MILLISECONDS); }
    private void retrySafely() { try { retry(); } catch (Exception e) { System.err.println("pending retry: " + e.getClass().getSimpleName()); } }
    public void retry() throws Exception {
        Map<String, Item> pending;
        synchronized (this) { if (failed) throw new IOException("queue failed closed"); pending = new LinkedHashMap<>(items); }
        for (var entry : pending.entrySet()) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("pending worker interrupted");
            if (!entry.getValue().result.isEmpty()) continue;
            String result;
            try { result = submitter.submit(entry.getKey(), entry.getValue().command); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw e; }
            catch (Exception e) { continue; }
            if (result == null || result.length() > 2048) continue;
            if (!result.startsWith("OK ") && !result.startsWith("ERR ")) continue;
            synchronized (this) {
                if (failed) throw new IOException("queue failed closed");
                Item current = items.get(entry.getKey());
                if (current.result.isEmpty()) persist(entry.getKey(), new Item(entry.getValue().command, result));
            }
        }
    }
    private static byte[] digest(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }
    private void save() throws IOException {
        try {
            byte[] body = Protocol.encode(out -> {
                out.writeInt(0x504c5132); out.writeUTF(origin); out.writeLong(sequence); out.writeInt(items.size());
                for (var e : items.entrySet()) { out.writeUTF(e.getKey()); out.writeUTF(e.getValue().command); out.writeUTF(e.getValue().result); }
            });
            ByteBuffer data = ByteBuffer.allocate(body.length + 32).put(body).put(digest(body)); data.flip();
            Path temp = directory.resolve("queue.tmp");
            try (var file = FileChannel.open(temp, CREATE, TRUNCATE_EXISTING, WRITE)) { while (data.hasRemaining()) file.write(data); file.force(true); }
            Files.move(temp, directory.resolve("queue.bin"), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            try (var dir = FileChannel.open(directory, READ)) { dir.force(true); }
        } catch (IOException e) { failed = true; throw e; }
    }
    private void load() throws IOException {
        byte[] all = Files.readAllBytes(directory.resolve("queue.bin"));
        if (all.length < 32) throw new IOException("truncated pending queue");
        byte[] body = Arrays.copyOf(all, all.length - 32);
        if (!MessageDigest.isEqual(digest(body), Arrays.copyOfRange(all, body.length, all.length))) throw new IOException("pending checksum mismatch");
        var in = Protocol.input(body);
        int magic = in.readInt();
        if (magic != 0x504c5131 && magic != 0x504c5132) throw new IOException("pending format mismatch");
        origin = in.readUTF(); UUID.fromString(origin);
        sequence = magic == 0x504c5132 ? in.readLong() : 0;
        if (sequence < 0) throw new IOException("negative checkpoint sequence");
        int count = in.readInt();
        if (count < 0 || count > capacity) throw new IOException("pending capacity mismatch");
        for (int i = 0; i < count; i++) {
            String id = in.readUTF(), command = in.readUTF(), result = in.readUTF(); validate(id, command);
            if (!result.isEmpty() && !result.startsWith("OK ") && !result.startsWith("ERR ")) throw new IOException("invalid outcome");
            if (items.put(id, new Item(command, result)) != null) throw new IOException("duplicate intent");
        }
        if (in.available() != 0) throw new IOException("trailing pending bytes");
    }
    private void persist(String id, Item item) throws IOException {
        try {
            if (sequence == Long.MAX_VALUE) throw new IOException("WAL sequence exhausted");
            long next = sequence + 1;
            byte[] body = Protocol.encode(out -> {
                out.writeLong(next); out.writeUTF(origin); out.writeUTF(id);
                out.writeUTF(item.command); out.writeUTF(item.result);
            });
            ByteBuffer record = ByteBuffer.allocate(8 + body.length + 32).putInt(body.length).putInt(~body.length).put(body).put(digest(body)); record.flip();
            try (var wal = FileChannel.open(directory.resolve("queue.wal"), CREATE, WRITE, APPEND)) {
                while (record.hasRemaining()) wal.write(record);
                wal.force(true);
            }
            // The WAL filename itself must survive a crash before admission can be acknowledged.
            try (var dir = FileChannel.open(directory, READ)) { dir.force(true); }
            sequence = next; items.put(id, item);
            if (++sinceCheckpoint >= 128) checkpoint();
        } catch (IOException e) { failed = true; throw e; }
    }
    /** Retains every idempotency result; only the journal prefix is compacted. */
    public synchronized void checkpoint() throws IOException {
        if (failed) throw new IOException("queue failed closed");
        save(); // Atomic, forced checkpoint covers all records before WAL truncation.
        try {
            try (var wal = FileChannel.open(directory.resolve("queue.wal"), CREATE, WRITE, TRUNCATE_EXISTING)) { wal.force(true); }
            try (var dir = FileChannel.open(directory, READ)) { dir.force(true); }
            sinceCheckpoint = 0;
        } catch (IOException e) { failed = true; throw e; }
    }
    private void replay() throws IOException {
        Path path = directory.resolve("queue.wal");
        if (!Files.exists(path)) return;
        long checkpointSequence = sequence, previous = -1;
        try (var wal = FileChannel.open(path, READ, WRITE)) {
            long good = 0, size = wal.size();
            while (good < size) {
                if (size - good < 8) { wal.truncate(good); wal.force(true); break; }
                ByteBuffer header = ByteBuffer.allocate(8); readFully(wal, header); header.flip();
                int length = header.getInt();
                if (header.getInt() != ~length || length < 1 || length > 10000) throw new IOException("invalid WAL record length");
                if (size - good - 8 < length + 32) { wal.truncate(good); wal.force(true); break; }
                ByteBuffer payload = ByteBuffer.allocate(length + 32); readFully(wal, payload);
                byte[] all = payload.array(), body = Arrays.copyOf(all, length);
                if (!MessageDigest.isEqual(digest(body), Arrays.copyOfRange(all, length, all.length))) throw new IOException("WAL checksum mismatch");
                var in = Protocol.input(body);
                long entrySequence = in.readLong();
                if (entrySequence <= 0 || (previous != -1 && entrySequence != previous + 1)
                        || (previous == -1 && entrySequence > checkpointSequence + 1)) throw new IOException("WAL sequence gap");
                previous = entrySequence;
                if (!origin.equals(in.readUTF())) throw new IOException("WAL incarnation mismatch");
                String id = in.readUTF(), command = in.readUTF(), result = in.readUTF(); validate(id, command);
                if (in.available() != 0 || (!result.isEmpty() && !result.startsWith("OK ") && !result.startsWith("ERR ")))
                    throw new IOException("invalid WAL outcome");
                if (entrySequence > checkpointSequence) {
                    if (entrySequence != sequence + 1) throw new IOException("WAL sequence gap");
                    Item old = items.get(id);
                    if (old == null ? !result.isEmpty() || items.size() >= capacity
                            : !old.command.equals(command) || !old.result.isEmpty() || result.isEmpty())
                        throw new IOException("invalid WAL transition");
                    items.put(id, new Item(command, result)); sequence = entrySequence; sinceCheckpoint++;
                }
                good += 8L + length + 32;
            }
        }
    }
    private static void readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) if (channel.read(buffer) < 0) throw new EOFException("truncated WAL");
    }
    @Override public void close() throws IOException {
        worker.shutdownNow();
        try { if (!worker.awaitTermination(10, TimeUnit.SECONDS)) throw new IOException("pending worker did not stop"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); }
        synchronized (this) { failed = true; try { lock.release(); } finally { lockChannel.close(); } }
    }
}

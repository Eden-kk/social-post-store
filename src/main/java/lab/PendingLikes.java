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
            else { origin = UUID.randomUUID().toString(); save(); }
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
        items.put(id, new Item(command, "")); save();
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
                items.put(entry.getKey(), new Item(entry.getValue().command, result)); save();
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
                out.writeInt(0x504c5131); out.writeUTF(origin); out.writeInt(items.size());
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
        if (in.readInt() != 0x504c5131) throw new IOException("pending format mismatch");
        origin = in.readUTF(); UUID.fromString(origin);
        int count = in.readInt();
        if (count < 0 || count > capacity) throw new IOException("pending capacity mismatch");
        for (int i = 0; i < count; i++) {
            String id = in.readUTF(), command = in.readUTF(), result = in.readUTF(); validate(id, command);
            if (!result.isEmpty() && !result.startsWith("OK ") && !result.startsWith("ERR ")) throw new IOException("invalid outcome");
            if (items.put(id, new Item(command, result)) != null) throw new IOException("duplicate intent");
        }
        if (in.available() != 0) throw new IOException("trailing pending bytes");
    }
    @Override public void close() throws IOException {
        worker.shutdownNow();
        try { if (!worker.awaitTermination(10, TimeUnit.SECONDS)) throw new IOException("pending worker did not stop"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); }
        synchronized (this) { failed = true; try { lock.release(); } finally { lockChannel.close(); } }
    }
}

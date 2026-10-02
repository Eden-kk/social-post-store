package lab;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import static java.nio.file.StandardOpenOption.*;
import static lab.Protocol.*;

/** Whole-state atomic replacement: intentionally simple, not a scalable WAL. */
public final class DurableState implements AutoCloseable {
    public record State(long term, int votedFor, int committed, List<Entry> entries) {}
    private final Path directory;
    private final FileChannel lockChannel;
    private final FileLock lock;
    public DurableState(Path directory) throws IOException {
        this.directory = directory;
        Files.createDirectories(directory);
        lockChannel = FileChannel.open(directory.resolve("node.lock"), CREATE, WRITE);
        lock = lockChannel.tryLock();
        if (lock == null) { lockChannel.close(); throw new IOException("data directory in use"); }
    }
    private static byte[] digest(byte[] data) {
        try { return MessageDigest.getInstance("SHA-256").digest(data); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }
    public State load() throws IOException {
        Path path = directory.resolve("state.bin");
        if (!Files.exists(path)) return new State(0, -1, 0, List.of());
        byte[] file = Files.readAllBytes(path);
        if (file.length < 32) throw new IOException("truncated state");
        byte[] body = Arrays.copyOf(file, file.length - 32);
        if (!MessageDigest.isEqual(digest(body), Arrays.copyOfRange(file, body.length, file.length)))
            throw new IOException("state checksum mismatch");
        var in = input(body);
        if (in.readInt() != 0x4d535131) throw new IOException("unknown state format");
        long term = in.readLong(); int voted = in.readInt(), committed = in.readInt(), size = in.readInt();
        if (term < 0 || size < 0 || size > in.available() / 16 || committed < 0 || committed > size)
            throw new IOException("invalid state metadata");
        var entries = new ArrayList<Entry>();
        for (int i = 0; i < size; i++) entries.add(entry(in));
        if (in.available() != 0) throw new IOException("trailing state bytes");
        return new State(term, voted, committed, entries);
    }
    public void save(State state) throws IOException {
        byte[] body = encode(o -> {o.writeInt(0x4d535131); o.writeLong(state.term());
            o.writeInt(state.votedFor()); o.writeInt(state.committed());
            o.writeInt(state.entries().size()); for (Entry e : state.entries()) entry(o, e);});
        Path temp = directory.resolve("state.tmp");
        try (var file = FileChannel.open(temp, CREATE, TRUNCATE_EXISTING, WRITE)) {
            var data = java.nio.ByteBuffer.wrap(body);
            while (data.hasRemaining()) file.write(data);
            data = java.nio.ByteBuffer.wrap(digest(body));
            while (data.hasRemaining()) file.write(data);
            file.force(true);
        }
        Files.move(temp, directory.resolve("state.bin"), StandardCopyOption.ATOMIC_MOVE,
                   StandardCopyOption.REPLACE_EXISTING);
        try (var dir = FileChannel.open(directory, READ)) { dir.force(true); }
    }
    @Override public void close() throws IOException { lock.release(); lockChannel.close(); }
}

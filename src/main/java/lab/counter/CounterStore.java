package lab.counter;

import lab.DurableState;
import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import static java.nio.file.StandardOpenOption.*;

/** Atomic local acceptance, dedup results and coalesced per-peer outbox. */
public final class CounterStore implements AutoCloseable {
    private record Request(String key, long delta, String result) {}
    public record Snapshot(long version, CounterState state) {}
    private final Path directory;
    private final DurableState identityLock;
    private final Set<Integer> destinations;
    private String origin;
    private CounterState state = new CounterState();
    private long version;
    private Map<String, Request> requests = new TreeMap<>();
    private Map<Integer, Long> acknowledged = new TreeMap<>();
    private Map<String, Long> changedAt = new TreeMap<>();
    private boolean failed;
    public CounterStore(Path directory, int id, Map<Integer, URI> peers) throws IOException {
        this.directory = directory; destinations = new TreeSet<>(peers.keySet()); destinations.remove(id);
        identityLock = new DurableState(directory);
        try {
            boolean existing = Files.exists(directory.resolve("identity.bin"));
            if (!existing && Files.exists(directory.resolve("counter.bin"))) throw new IOException("counter state without identity");
            identityLock.bindIdentity(id, peers);
            if (Files.exists(directory.resolve("counter.bin"))) load();
            else { if (existing) throw new IOException("identity without counter state; fresh directory required");
                origin = id + ":" + UUID.randomUUID(); persist(state, version, requests, acknowledged); }
        } catch (IOException | RuntimeException e) { identityLock.close(); throw e; }
    }
    public synchronized String origin() { return origin; }
    public synchronized Snapshot snapshot() { healthy(); return new Snapshot(version, state); }
    public synchronized Snapshot pendingSnapshot(int peer) {
        healthy();
        if (!destinations.contains(peer)) throw new IllegalArgumentException("invalid peer");
        long base = acknowledged.getOrDefault(peer, 0L);
        if (base >= version) return null;
        var keys = changedAt.entrySet().stream().filter(e -> e.getValue() > base).map(Map.Entry::getKey).toList();
        return new Snapshot(version, state.selectKeys(keys));
    }
    public synchronized String value(String key) { healthy(); return state.value(key).toString(); }
    public synchronized int pending() { return (int) destinations.stream().filter(p -> acknowledged.getOrDefault(p, 0L) < version).count(); }
    public synchronized String add(String expectedOrigin, String id, String key, long delta) throws IOException {
        healthy();
        if (!origin.equals(expectedOrigin)) throw new IllegalArgumentException("retry must use original incarnation");
        if (id == null || id.isBlank() || id.length() > 128) throw new IllegalArgumentException("invalid request ID");
        var prior = requests.get(id);
        if (prior != null) {
            if (!prior.key().equals(key) || prior.delta() != delta) throw new IllegalArgumentException("request payload mismatch");
            return prior.result();
        }
        if (requests.size() >= 100_000) throw new IllegalArgumentException("lab request retention capacity reached");
        CounterState next = state.add(key, origin, delta); long nextVersion = Math.incrementExact(version);
        String result = "LOCAL_DURABLE origin=" + origin + " value=" + next.value(key);
        var nextRequests = new TreeMap<>(requests); nextRequests.put(id, new Request(key, delta, result));
        var changes = new TreeMap<>(changedAt); changes.put(key, nextVersion);
        persist(next, nextVersion, nextRequests, acknowledged, changes);
        changedAt = changes;
        state = next; version = nextVersion; requests = nextRequests; return result;
    }
    public synchronized void merge(CounterState incoming) throws IOException {
        healthy();
        if (state.advancesOrigin(incoming, origin)) { failed = true; throw new IOException("local incarnation advanced remotely; stale or cloned store"); }
        var next = state.merge(incoming);
        if (next.equals(state)) return;
        long nextVersion = Math.incrementExact(version);
        var changes = new TreeMap<>(changedAt);
        for (String key : next.changedKeys(state)) changes.put(key, nextVersion);
        persist(next, nextVersion, requests, acknowledged, changes); state = next; version = nextVersion; changedAt = changes;
    }
    public synchronized void acknowledge(int peer, long sentVersion) throws IOException {
        healthy();
        if (!destinations.contains(peer) || sentVersion < 0 || sentVersion > version) throw new IllegalArgumentException("invalid ACK");
        if (acknowledged.getOrDefault(peer, 0L) >= sentVersion) return;
        var next = new TreeMap<>(acknowledged); next.put(peer, sentVersion);
        persist(state, version, requests, next); acknowledged = next;
    }
    private void healthy() { if (failed) throw new IllegalStateException("counter store unavailable; restart required"); }
    private static byte[] digest(byte[] data) {
        try { return MessageDigest.getInstance("SHA-256").digest(data); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }
    private void persist(CounterState value, long revision, Map<String, Request> dedup, Map<Integer, Long> acks) throws IOException {
        persist(value, revision, dedup, acks, changedAt);
    }
    private void persist(CounterState value, long revision, Map<String, Request> dedup, Map<Integer, Long> acks, Map<String, Long> changes) throws IOException {
        try {
            var wire = new ByteArrayOutputStream(); value.write(new DataOutputStream(wire));
            if (wire.size() > 900_000) throw new IOException("counter snapshot exceeds lab replication limit");
            var buffer = new ByteArrayOutputStream(); var out = new DataOutputStream(buffer);
            out.writeInt(0x504e4332); out.writeUTF(origin); out.writeLong(revision); value.write(out);
            out.writeInt(dedup.size());
            for (var r : dedup.entrySet()) { out.writeUTF(r.getKey()); out.writeUTF(r.getValue().key()); out.writeLong(r.getValue().delta()); out.writeUTF(r.getValue().result()); }
            out.writeInt(acks.size()); for (var ack : acks.entrySet()) { out.writeInt(ack.getKey()); out.writeLong(ack.getValue()); }
            out.writeInt(changes.size()); for (var change : changes.entrySet()) { out.writeUTF(change.getKey()); out.writeLong(change.getValue()); }
            byte[] body = buffer.toByteArray(); var data = ByteBuffer.allocate(body.length + 32).put(body).put(digest(body)); data.flip();
            Path temp = directory.resolve("counter.tmp");
            try (var file = FileChannel.open(temp, CREATE, TRUNCATE_EXISTING, WRITE)) { while (data.hasRemaining()) file.write(data); file.force(true); }
            Files.move(temp, directory.resolve("counter.bin"), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            try (var dir = FileChannel.open(directory, READ)) { dir.force(true); }
        } catch (IOException e) { failed = true; throw e; }
    }
    private void load() throws IOException {
        byte[] file = Files.readAllBytes(directory.resolve("counter.bin"));
        if (file.length < 32) throw new IOException("truncated counter state");
        byte[] body = Arrays.copyOf(file, file.length - 32);
        if (!MessageDigest.isEqual(digest(body), Arrays.copyOfRange(file, body.length, file.length))) throw new IOException("counter checksum mismatch");
        var in = new DataInputStream(new ByteArrayInputStream(body));
        int format = in.readInt();
        if (format != 0x504e4331 && format != 0x504e4332) throw new IOException("counter format");
        origin = in.readUTF(); version = in.readLong(); if (version < 0) throw new IOException("counter version");
        state = CounterState.read(in); int n = CounterState.count(in);
        for (int i = 0; i < n; i++) requests.put(in.readUTF(), new Request(in.readUTF(), in.readLong(), in.readUTF()));
        n = CounterState.count(in);
        for (int i = 0; i < n; i++) { int peer = in.readInt(); long ack = in.readLong();
            if (!destinations.contains(peer) || ack < 0 || ack > version) throw new IOException("invalid persisted ACK"); acknowledged.put(peer, ack); }
        if (format == 0x504e4332) {
            n = CounterState.count(in);
            for (int i = 0; i < n; i++) {
                String key = in.readUTF(); long revision = in.readLong();
                if (revision <= 0 || revision > version || changedAt.put(key, revision) != null) throw new IOException("invalid change index");
            }
            if (!changedAt.keySet().equals(state.keys())) throw new IOException("incomplete change index");
        } else {
            // Legacy full-state format lacks a change index: conservatively resend all keys.
            if (version == 0 && !state.keys().isEmpty()) throw new IOException("legacy nonempty state has zero version");
            for (String key : state.keys()) changedAt.put(key, version);
            acknowledged.clear();
        }
        if (in.available() != 0) throw new IOException("trailing counter state");
        if (format == 0x504e4331) persist(state, version, requests, acknowledged);
    }
    @Override public synchronized void close() throws IOException { failed = true; identityLock.close(); }
}

package lab.counter;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Single worker sends changed keys; periodic full state repairs an empty outbox. */
public final class CounterNode implements AutoCloseable {
    private final int id;
    private final Map<Integer, URI> peers;
    private final CounterStore store;
    private final String mergePath;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(200)).build();
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();
    private volatile Set<Integer> blocked = Set.of();
    private volatile boolean dropAck;
    private volatile String lastError = "none";
    private final long fullIntervalNanos;
    private long nextFull;
    public CounterNode(int id, Map<Integer, URI> peers, Path directory) throws IOException {
        this(id, peers, directory, "/counter/merge");
    }
    public CounterNode(int id, Map<Integer, URI> peers, Path directory, String mergePath) throws IOException {
        this(id, peers, directory, mergePath, Long.getLong("counter.fullIntervalMillis", 30_000L));
    }
    public CounterNode(int id, Map<Integer, URI> peers, Path directory, String mergePath, long fullIntervalMillis) throws IOException {
        if (fullIntervalMillis < 100) throw new IllegalArgumentException("full interval must be >=100ms");
        fullIntervalNanos = TimeUnit.MILLISECONDS.toNanos(fullIntervalMillis);
        nextFull = System.nanoTime() + fullIntervalNanos;
        this.mergePath = mergePath;
        this.id = id; this.peers = Map.copyOf(peers); store = new CounterStore(directory, id, peers);
    }
    public void start() { worker.scheduleWithFixedDelay(this::replicate, 200, 300, TimeUnit.MILLISECONDS); }
    public void block(Set<Integer> ids) { blocked = Set.copyOf(ids); }
    public void dropAck(boolean enabled) { dropAck = enabled; }
    public boolean droppingAck() { return dropAck; }
    public String origin() { return store.origin(); }
    public String status() { return "origin=" + store.origin() + "\npending=" + store.pending() + "\nlastFailure=" + lastError + "\n"; }
    public String value(String key) { return store.value(key); }
    public String add(String origin, String requestId, String body) throws IOException {
        String[] parts = body.split("\n", -1);
        if (parts.length != 2) throw new IllegalArgumentException("body must be key newline delta");
        String result = store.add(origin, requestId, parts[0], Long.parseLong(parts[1]));
        // The periodic worker coalesces writes without queuing per-request network tasks.
        return result;
    }
    public byte[] receive(int sender, byte[] body) throws IOException {
        if (!peers.containsKey(sender) || sender == id || blocked.contains(sender)) throw new IOException("counter peer blocked");
        var in = new DataInputStream(new ByteArrayInputStream(body));
        var incoming = CounterState.read(in); if (in.available() != 0) throw new IOException("trailing snapshot");
        store.merge(incoming); return "DURABLE".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    private void replicate() {
        try {
            long now = System.nanoTime(); boolean full = now - nextFull >= 0;
            if (full) nextFull = now + fullIntervalNanos;
            for (var peer : peers.entrySet()) {
                if (peer.getKey() == id || blocked.contains(peer.getKey())) continue;
                var snapshot = full ? store.snapshot() : store.pendingSnapshot(peer.getKey());
                if (snapshot == null) continue;
                var buffer = new ByteArrayOutputStream(); snapshot.state().write(new DataOutputStream(buffer));
                CompletableFuture<HttpResponse<String>> pending = null;
                try {
                    var request = HttpRequest.newBuilder(peer.getValue().resolve(mergePath))
                        .timeout(Duration.ofMillis(400)).header("X-Counter-Peer", "" + id)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(buffer.toByteArray())).build();
                    pending = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
                    var response = pending.get(500, TimeUnit.MILLISECONDS);
                    if (response.statusCode() != 200 || !response.body().equals("DURABLE")) throw new IOException("peer not durable");
                    store.acknowledge(peer.getKey(), snapshot.version());
                } catch (Exception e) { lastError = e.getClass().getSimpleName(); }
                finally { if (pending != null) pending.cancel(true); }
            }
        } catch (Exception e) { lastError = e.getClass().getSimpleName(); }
    }
    @Override public void close() throws IOException { worker.shutdownNow(); store.close(); }
}

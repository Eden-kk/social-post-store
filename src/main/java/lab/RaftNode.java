package lab;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static lab.Protocol.*;
import lab.SnapshotState.Result;

/** Fixed-membership educational Raft. All protocol state is guarded by this. */
public final class RaftNode implements AutoCloseable {
    private final int id;
    private final Map<Integer, URI> peers;
    private final DurableState storage;
    private final List<Entry> log = new ArrayList<>();
    private SqlEngine database = new SqlEngine();
    private SocialState social = new SocialState();
    private final Map<String, Result> results = new HashMap<>();
    private final Map<Integer, Integer> next = new HashMap<>(), matched = new HashMap<>();
    private final Set<Integer> blocked = ConcurrentHashMap.newKeySet();
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(200)).build();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private long term, electionDeadline, lastHeartbeat;
    private int votedFor = -1, committed, applied, leader = -1;
    private int baseIndex;
    private byte[] snapshot = new byte[0];
    private String role = "follower";
    private volatile boolean failed;

    public RaftNode(int id, Map<Integer, URI> peers, Path directory) throws IOException {
        if (!peers.containsKey(id) || peers.size() != 3 || new HashSet<>(peers.values()).size() != 3)
            throw new IllegalArgumentException("exactly 3 distinct fixed peers required");
        this.id = id; this.peers = Map.copyOf(peers); storage = new DurableState(directory);
        try {
            storage.bindIdentity(id, this.peers);
            storage.bindSqlDialect();
            var state = storage.load(); term = state.term(); votedFor = state.votedFor();
            committed = state.committed(); baseIndex = state.baseIndex(); snapshot = state.snapshot();
            log.add(new Entry(state.baseTerm(), "", "")); log.addAll(state.entries());
            if (baseIndex > 0) restore(snapshot);
            applied = baseIndex;
            applyCommitted(); resetElection();
        } catch (IOException | RuntimeException e) {
            timer.shutdownNow();
            try { storage.close(); } catch (IOException closeError) { e.addSuppressed(closeError); }
            throw e;
        }
    }
    public void start() { timer.scheduleWithFixedDelay(this::tickSafely, 30, 30, TimeUnit.MILLISECONDS); }
    public synchronized SqlEngine.CounterCommand counterCommand(String sql) {
        if (failed) throw new IllegalStateException("Raft schema unavailable");
        return database.counterCommand(sql);
    }
    public synchronized void requireActivePost(String post) {
        if (failed) throw new IllegalStateException("Raft metadata unavailable");
        social.requireLocallyActive(post);
    }
    private int lastIndex() { return baseIndex + log.size() - 1; }
    private Entry at(int index) { return log.get(index - baseIndex); }
    private void restore(byte[] bytes) throws IOException {
        var restored = SnapshotState.decode(bytes);
        database = restored.database(); social = restored.social();
        results.clear(); results.putAll(restored.results());
    }
    private void resetElection() {
        electionDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(
            ThreadLocalRandom.current().nextLong(1000, 1800));
    }
    private void persist() throws IOException {
        try { storage.save(new DurableState.State(term, votedFor, committed, baseIndex, log.get(0).term(), snapshot,
                                                  new ArrayList<>(log.subList(1, log.size())))); }
        catch (IOException e) { failed = true; notifyAll(); throw e; }
    }
    private void higherTerm(long incoming) throws IOException {
        if (incoming > term) {term = incoming; votedFor = -1; role = "follower";
            leader = -1; persist(); resetElection(); notifyAll();}
    }
    public synchronized VoteReply vote(Vote request) throws IOException {
        checkPeer(request.candidate()); higherTerm(request.term());
        boolean fresh = request.lastTerm() > at(lastIndex()).term() ||
            (request.lastTerm() == at(lastIndex()).term() && request.lastIndex() >= lastIndex());
        boolean grant = request.term() == term && (votedFor == -1 || votedFor == request.candidate()) && fresh;
        if (grant) { votedFor = request.candidate(); persist(); resetElection(); }
        return new VoteReply(term, grant);
    }
    public synchronized AppendReply append(Append request) throws IOException {
        checkPeer(request.leader()); higherTerm(request.term());
        if (request.term() < term) return new AppendReply(term, false, lastIndex());
        role = "follower"; leader = request.leader(); resetElection(); notifyAll();
        int prev = request.previousIndex();
        if (prev < baseIndex || prev > lastIndex() || at(prev).term() != request.previousTerm())
            return new AppendReply(term, false, lastIndex());
        int position = prev;
        for (Entry e : request.entries()) {
            position++;
            if (position <= lastIndex() && at(position).term() != e.term()) {
                if (position <= committed) { failed = true; throw new IOException("committed prefix conflict"); }
                log.subList(position - baseIndex, log.size()).clear();
            }
            if (position > lastIndex()) log.add(e);
        }
        // Only the prefix proven consistent by this RPC can advance commitment.
        committed = Math.max(committed, Math.min(request.commitIndex(), position));
        persist(); applyCommitted(); maybeSnapshot();
        return new AppendReply(term, true, position);
    }
    private void checkPeer(int peer) throws IOException {
        if (failed || !peers.containsKey(peer) || blocked.contains(peer)) throw new IOException("peer unavailable");
    }
    private void applyCommitted() {
        while (applied < committed) {
            Entry e = at(++applied);
            if (e.requestId().isEmpty()) continue; // Leader's current-term barrier.
            Result old = results.get(e.requestId());
            if (old == null) results.put(e.requestId(), new Result(e.sql(),
                e.sql().startsWith(SocialState.PREFIX)
                    ? social.execute(e.sql().substring(SocialState.PREFIX.length())) : database.execute(e.sql())));
        }
        // Rebuild on restart from the persisted committed prefix, never replay onto an old DB.
    }
    /** A bounded optimization: retaining logs remains correct if the snapshot is too large. */
    private void maybeSnapshot() throws IOException {
        if (applied - baseIndex < 128) return;
        byte[] bytes;
        try { bytes = SnapshotState.encode(database, social, results); }
        catch (SnapshotState.TooLarge tooLarge) { return; }
        compact(bytes);
    }
    public synchronized void takeSnapshot() throws IOException {
        if (failed) throw new IOException("node storage failed");
        if (applied > baseIndex) compact(SnapshotState.encode(database, social, results));
    }
    private void compact(byte[] bytes) throws IOException {
        int index = applied; long includedTerm = at(index).term();
        var suffix = new ArrayList<>(log.subList(index - baseIndex + 1, log.size()));
        try { storage.save(new DurableState.State(term, votedFor, committed, index, includedTerm, bytes, suffix)); }
        catch (IOException e) { failed = true; notifyAll(); throw e; }
        log.clear(); log.add(new Entry(includedTerm, "", "")); log.addAll(suffix);
        baseIndex = index; snapshot = bytes;
    }
    public synchronized AppendReply install(Install request) throws IOException {
        checkPeer(request.leader()); higherTerm(request.term());
        if (request.term() < term) return new AppendReply(term, false, lastIndex());
        if (request.index() <= 0 || request.includedTerm() < 0 || request.includedTerm() > request.term())
            throw new IOException("invalid snapshot boundary");
        var restored = SnapshotState.decode(request.state());
        role = "follower"; leader = request.leader(); resetElection(); notifyAll();
        if (request.index() <= applied) return new AppendReply(term, true, request.index());
        boolean matches = request.index() <= lastIndex() && at(request.index()).term() == request.includedTerm();
        if (!matches && request.index() <= committed) throw new IOException("committed snapshot conflict");
        var suffix = matches ? new ArrayList<>(log.subList(request.index() - baseIndex + 1, log.size())) : new ArrayList<Entry>();
        int newCommit = Math.max(committed, request.index());
        try { storage.save(new DurableState.State(term, votedFor, newCommit, request.index(), request.includedTerm(), request.state(), suffix)); }
        catch (IOException e) { failed = true; notifyAll(); throw e; }
        database = restored.database(); social = restored.social(); results.clear(); results.putAll(restored.results());
        baseIndex = request.index(); snapshot = request.state().clone(); applied = baseIndex; committed = newCommit;
        log.clear(); log.add(new Entry(request.includedTerm(), "", "")); log.addAll(suffix);
        applyCommitted();
        return new AppendReply(term, true, baseIndex);
    }
    public synchronized String submit(String requestId, String sql, long timeoutMs) throws Exception {
        if (failed) throw new IOException("node storage failed");
        if (!role.equals("leader")) return "NOT_LEADER " + leader;
        if (requestId.isBlank() || requestId.length() > 128 || sql.isBlank() || sql.length() > 8192)
            throw new IllegalArgumentException("invalid request");
        Result prior = results.get(requestId);
        if (prior != null) return prior.sql().equals(sql) ? prior.result() : "ERR request_id reused with different SQL";
        // IDs are exact payload-bound, including commands that have not committed yet.
        boolean pending = false;
        for (Entry entry : log) if (entry.requestId().equals(requestId)) {
            if (!entry.sql().equals(sql)) return "ERR request_id reused with different SQL";
            pending = true;
        }
        if (!pending) {log.add(new Entry(term, requestId, sql)); persist();}
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!failed && role.equals("leader") && !results.containsKey(requestId)) {
            long remaining = end - System.nanoTime();
            if (remaining <= 0) return "UNKNOWN retry same request_id";
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
        prior = results.get(requestId);
        if (prior != null) return prior.sql().equals(sql) ? prior.result() : "ERR request_id reused with different SQL";
        return "UNKNOWN retry same request_id";
    }
    public synchronized String status() {
        return "id="+id+"\nrole="+role+"\nterm="+term+"\nleader="+leader+
            "\ncommitted="+committed+"\napplied="+applied+"\nlast="+lastIndex()+"\nsnapshot="+baseIndex+"\nfailed="+failed+"\n";
    }
    public void block(Set<Integer> ids) { blocked.clear(); blocked.addAll(ids); }
    /** Queue forwarding obeys both endpoints' simulated partition gates; never forwards recursively. */
    public String pendingLike(String requestId, String command) throws Exception {
        PendingLikes.validate(requestId, command);
        String local = submit("social:" + requestId, SocialState.PREFIX + command, 700);
        if (local.startsWith("OK ") || local.startsWith("ERR ")) return local;
        for (int peer : peers.keySet()) if (peer != id && !blocked.contains(peer)) {
            try {
                var request = HttpRequest.newBuilder(peers.get(peer).resolve("/raft/like"))
                    .timeout(Duration.ofMillis(1100)).header("X-Raft-Peer", "" + id)
                    .header("X-Request-ID", requestId).POST(HttpRequest.BodyPublishers.ofString(command)).build();
                var pending = client.sendAsync(request, info -> new BoundedHttpBody(2048));
                try {
                    var response = pending.get(1100, TimeUnit.MILLISECONDS);
                    byte[] bytes = response.body();
                    String result = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
                    if (response.statusCode() == 200 && bytes.length <= 2048 && (result.startsWith("OK ") || result.startsWith("ERR ")))
                        return result;
                } catch (TimeoutException | ExecutionException e) { /* Same-ID retry remains safe. */ }
                finally { pending.cancel(true); }
            } catch (java.io.UncheckedIOException e) { /* Retry later with the identical request and body. */ }
        }
        return "UNKNOWN retry same request_id";
    }
    public String forwardedLike(int peer, String requestId, String command) throws Exception {
        checkPeer(peer); PendingLikes.validate(requestId, command);
        return submit("social:" + requestId, SocialState.PREFIX + command, 700);
    }
    private byte[] rpc(int peer, String path, byte[] body) throws Exception {
        if (blocked.contains(peer)) throw new IOException("injected partition");
        var request = HttpRequest.newBuilder(peers.get(peer).resolve(path))
            .timeout(Duration.ofMillis(250)).POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
        var pending = client.sendAsync(request, info -> new BoundedHttpBody(1_048_576));
        try {
            var response = pending.get(250, TimeUnit.MILLISECONDS);
            if (response.statusCode() != 200) throw new IOException("peer rejected");
            return response.body();
        } finally { pending.cancel(true); }
    }
    private void tickSafely() {
        if (failed) return;
        try {
            boolean elect;
            synchronized (this) { elect = !role.equals("leader") && System.nanoTime() >= electionDeadline; }
            if (elect) election();
            synchronized (this) {
                if (!role.equals("leader") || System.nanoTime() - lastHeartbeat < TimeUnit.MILLISECONDS.toNanos(80)) return;
                lastHeartbeat = System.nanoTime();
            }
            for (int peer : peers.keySet()) if (peer != id) replicate(peer);
        } catch (Exception e) { System.err.println("tick error: " + e); }
    }
    private void election() throws IOException {
        Vote request;
        synchronized (this) {
            role = "candidate"; leader = -1; term++; votedFor = id; persist(); resetElection();
            request = new Vote(term, id, lastIndex(), at(lastIndex()).term());
        }
        int votes = 1;
        for (int peer : peers.keySet()) if (peer != id) {
            try {
                var in = input(rpc(peer, "/raft/vote", Protocol.vote(request)));
                var reply = new VoteReply(in.readLong(), in.readBoolean());
                synchronized (this) {
                    higherTerm(reply.term());
                    if (term != request.term() || !role.equals("candidate")) return;
                    if (reply.granted()) votes++;
                }
            } catch (IOException e) { if (failed) throw e; }
            catch (Exception ignored) { /* A failed RPC contributes no vote. */ }
        }
        synchronized (this) {
            if (votes >= 2 && term == request.term() && role.equals("candidate")) {
                role = "leader"; leader = id;
                for (int peer : peers.keySet()) { next.put(peer, lastIndex()+1); matched.put(peer, 0); }
                log.add(new Entry(term, "", "")); persist(); lastHeartbeat = 0;
            }
        }
    }
    private void replicate(int peer) throws IOException {
        Append request = null;
        Install transfer = null;
        synchronized (this) {
            if (!role.equals("leader") || failed) return;
            int start = next.get(peer), prev = start - 1;
            if (start <= baseIndex) transfer = new Install(term, id, baseIndex, log.get(0).term(), snapshot);
            else request = new Append(term, id, prev, at(prev).term(), committed,
                new ArrayList<>(log.subList(start - baseIndex, Math.min(log.size(), start - baseIndex + 16))));
        }
        AppendReply reply;
        try {var in = input(transfer == null ? rpc(peer, "/raft/append", Protocol.append(request))
                    : rpc(peer, "/raft/snapshot", Protocol.install(transfer)));
            reply = new AppendReply(in.readLong(), in.readBoolean(), in.readInt());}
        catch (Exception e) { return; }
        synchronized (this) {
            higherTerm(reply.term());
            if (term != (transfer == null ? request.term() : transfer.term()) || !role.equals("leader")) return;
            if (!reply.success()) {next.put(peer, Math.max(1, next.get(peer)-1)); return;}
            matched.put(peer, reply.matchIndex()); next.put(peer, reply.matchIndex()+1);
            for (int index = lastIndex(); index > committed; index--) {
                if (at(index).term() != term) continue;
                int copies = 1;
                for (int other : peers.keySet()) if (other != id && matched.getOrDefault(other, 0) >= index) copies++;
                if (copies >= 2) {committed = index; persist(); applyCommitted(); maybeSnapshot(); notifyAll(); break;}
            }
        }
    }
    @Override public void close() throws IOException { timer.shutdownNow(); storage.close(); }
}

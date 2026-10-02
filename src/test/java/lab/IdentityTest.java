package lab;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;
import java.util.*;

public final class IdentityTest {
    private static final Map<Integer, URI> PEERS = Map.of(
        0, URI.create("http://127.0.0.1:18000"),
        1, URI.create("http://127.0.0.1:18001"),
        2, URI.create("http://127.0.0.1:18002"));
    private static void rejected(int id, Map<Integer, URI> peers, Path directory, String reason)
            throws Exception {
        try (var node = new RaftNode(id, peers, directory)) {
            throw new AssertionError("accepted " + reason);
        } catch (IOException expected) {
            if (!expected.getMessage().contains(reason)) throw expected;
        }
        // Verify failed construction did not retain the exclusive directory lock.
        try (var store = new DurableState(directory)) { }
    }
    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("minisql-identity-test-");
        try (var node = new RaftNode(0, PEERS, directory)) { }
        byte[] identity = Files.readAllBytes(directory.resolve("identity.bin"));
        var reversed = new LinkedHashMap<Integer, URI>();
        for (int id : List.of(2, 1, 0)) reversed.put(id, PEERS.get(id));
        try (var node = new RaftNode(0, reversed, directory)) { }
        rejected(1, PEERS, directory, "differs");
        var changed = new HashMap<>(PEERS); changed.put(2, URI.create("http://127.0.0.1:19002"));
        rejected(0, changed, directory, "differs");
        if (!Arrays.equals(identity, Files.readAllBytes(directory.resolve("identity.bin"))))
            throw new AssertionError("mismatch overwrote identity");
        Files.write(directory.resolve("identity.bin"), new byte[] {1, 2});
        rejected(0, PEERS, directory, "differs");
        Files.write(directory.resolve("identity.bin"), identity);
        try (var store = new DurableState(directory)) {
            store.save(new DurableState.State(1, 0, 0, List.of()));
            try (var duplicate = new DurableState(directory)) {
                throw new AssertionError("duplicate lock accepted");
            } catch (OverlappingFileLockException expected) { }
        }
        byte[] validState = Files.readAllBytes(directory.resolve("state.bin"));
        Files.write(directory.resolve("state.bin"), new byte[] {1});
        rejected(0, PEERS, directory, "truncated");
        Files.write(directory.resolve("state.bin"), validState);
        try (var node = new RaftNode(0, PEERS, directory)) { }
        Path legacy = Files.createTempDirectory("minisql-unbound-test-");
        try (var store = new DurableState(legacy)) {
            store.save(new DurableState.State(0, -1, 0, List.of()));
        }
        rejected(0, PEERS, legacy, "no identity");
        if (Files.exists(legacy.resolve("identity.bin"))) throw new AssertionError("legacy rebound");
        System.out.println("IdentityTest passed; evidence=" + directory + "; legacy=" + legacy);
    }
}

package lab;

import java.nio.file.*;
import java.util.List;

public final class DurableStateTest {
    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("minisql-state-test-");
        var expected = new DurableState.State(3, 1, 1,
            List.of(new Protocol.Entry(3, "key", "SELECT * FROM example")));
        try (var store = new DurableState(directory)) {store.save(expected);}
        try (var store = new DurableState(directory)) {
            if (!store.load().equals(expected)) throw new AssertionError("roundtrip");
        }
        var bytes = Files.readAllBytes(directory.resolve("state.bin")); bytes[8] ^= 1;
        Files.write(directory.resolve("state.bin"), bytes);
        try (var store = new DurableState(directory)) {
            try {store.load(); throw new AssertionError("corruption accepted");}
            catch (java.io.IOException expectedError) {}
        }
        System.out.println("DurableStateTest passed; evidence="+directory);
        var duplicates = java.util.Map.of(0, java.net.URI.create("http://127.0.0.1:18000"),
            1, java.net.URI.create("http://127.0.0.1:18000"), 2, java.net.URI.create("http://127.0.0.1:18002"));
        try { new RaftNode(0, duplicates, directory); throw new AssertionError("duplicate endpoints accepted"); }
        catch (IllegalArgumentException expectedError) {}
    }
}

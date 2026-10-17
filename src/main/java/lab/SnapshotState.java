package lab;

import java.io.*;
import java.util.*;

/** Authoritative state only; local PENDING queues and PN-counter stores are separate. */
final class SnapshotState {
    static final int MAX_BYTES = 900_000;
    static final class TooLarge extends IOException { TooLarge() { super("snapshot exceeds bounded transfer capacity"); } }
    record Result(String sql, String result) {}
    record Restored(SqlEngine database, SocialState social, Map<String, Result> results) {}
    private static void resultText(DataOutputStream out, String text) throws IOException {
        byte[] bytes = text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(bytes.length); out.write(bytes);
    }
    private static String resultText(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > MAX_BYTES || size > in.available()) throw new IOException("invalid result size");
        return new String(in.readNBytes(size), java.nio.charset.StandardCharsets.UTF_8);
    }
    static int count(DataInputStream in) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > in.available()) throw new IOException("invalid snapshot count");
        return count;
    }
    static byte[] encode(SqlEngine db, SocialState social, Map<String, Result> results) throws IOException {
        byte[] bytes = Protocol.encode(out -> {
            out.writeInt(0x53505331); db.writeSnapshot(out); social.writeSnapshot(out);
            out.writeInt(results.size());
            for (var entry : new TreeMap<>(results).entrySet()) {
                Protocol.text(out, entry.getKey()); Protocol.text(out, entry.getValue().sql());
                resultText(out, entry.getValue().result());
            }
        });
        if (bytes.length > MAX_BYTES) throw new TooLarge();
        return bytes;
    }
    static Restored decode(byte[] bytes) throws IOException {
        if (bytes.length > MAX_BYTES) throw new IOException("snapshot too large");
        var in = Protocol.input(bytes);
        if (in.readInt() != 0x53505331) throw new IOException("unknown snapshot format");
        SqlEngine db = SqlEngine.readSnapshot(in); SocialState social = SocialState.readSnapshot(in);
        var results = new HashMap<String, Result>(); int size = count(in);
        for (int i = 0; i < size; i++) {
            String id = Protocol.text(in); Result result = new Result(Protocol.text(in), resultText(in));
            if (id.isEmpty() || results.put(id, result) != null) throw new IOException("duplicate snapshot request");
        }
        if (in.available() != 0) throw new IOException("trailing snapshot bytes");
        return new Restored(db, social, results);
    }
}

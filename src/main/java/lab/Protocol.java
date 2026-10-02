package lab;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Bounded binary messages; never deserialize arbitrary Java objects. */
public final class Protocol {
    private Protocol() {}
    public record Entry(long term, String requestId, String sql) {}
    public record Vote(long term, int candidate, int lastIndex, long lastTerm) {}
    public record VoteReply(long term, boolean granted) {}
    public record Append(long term, int leader, int previousIndex, long previousTerm,
                         int commitIndex, List<Entry> entries) {}
    public record AppendReply(long term, boolean success, int matchIndex) {}
    @FunctionalInterface public interface Writer { void write(DataOutputStream out) throws IOException; }
    public static byte[] encode(Writer writer) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) { writer.write(out); }
        return bytes.toByteArray();
    }
    public static DataInputStream input(byte[] data) {
        return new DataInputStream(new ByteArrayInputStream(data));
    }
    public static void text(DataOutputStream out, String value) throws IOException {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        if (data.length > 65536) throw new IOException("string too long");
        out.writeInt(data.length); out.write(data);
    }
    public static String text(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > 65536) throw new IOException("invalid string size");
        byte[] bytes = in.readNBytes(size);
        if (bytes.length != size) throw new EOFException();
        return new String(bytes, StandardCharsets.UTF_8);
    }
    public static void entry(DataOutputStream out, Entry e) throws IOException {
        out.writeLong(e.term()); text(out, e.requestId()); text(out, e.sql());
    }
    public static Entry entry(DataInputStream in) throws IOException {
        return new Entry(in.readLong(), text(in), text(in));
    }
    public static byte[] vote(Vote v) throws IOException {
        return encode(o -> {o.writeLong(v.term()); o.writeInt(v.candidate());
            o.writeInt(v.lastIndex()); o.writeLong(v.lastTerm());});
    }
    public static Vote readVote(byte[] data) throws IOException {
        var in = input(data);
        return new Vote(in.readLong(), in.readInt(), in.readInt(), in.readLong());
    }
    public static byte[] append(Append a) throws IOException {
        return encode(o -> {o.writeLong(a.term()); o.writeInt(a.leader());
            o.writeInt(a.previousIndex()); o.writeLong(a.previousTerm());
            o.writeInt(a.commitIndex()); o.writeInt(a.entries().size());
            for (Entry e : a.entries()) entry(o, e);});
    }
    public static Append readAppend(byte[] data) throws IOException {
        var in = input(data);
        long term = in.readLong(); int leader = in.readInt(), prev = in.readInt();
        long prevTerm = in.readLong(); int commit = in.readInt(), size = in.readInt();
        if (size < 0 || size > 64) throw new IOException("invalid append size");
        var entries = new ArrayList<Entry>();
        for (int i = 0; i < size; i++) entries.add(entry(in));
        return new Append(term, leader, prev, prevTerm, commit, entries);
    }
}

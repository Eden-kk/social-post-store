package lab.counter;

import java.io.*;
import java.math.BigInteger;
import java.util.*;

/** Immutable state-based PN counter. Merge never adds received deltas. */
public final class CounterState {
    public record Component(long positive, long negative) {
        public Component { if (positive < 0 || negative < 0) throw new IllegalArgumentException("negative component"); }
        Component merge(Component other) { return new Component(Math.max(positive, other.positive), Math.max(negative, other.negative)); }
    }
    private final Map<String, Map<String, Component>> values;
    public CounterState() { this(Map.of()); }
    private CounterState(Map<String, Map<String, Component>> input) {
        var copy = new TreeMap<String, Map<String, Component>>();
        input.forEach((key, components) -> copy.put(key, Collections.unmodifiableMap(new TreeMap<>(components))));
        values = Collections.unmodifiableMap(copy);
    }
    public CounterState add(String key, String origin, long delta) {
        if (key == null || !key.matches("[A-Za-z0-9_.-]{1,128}")) throw new IllegalArgumentException("invalid key");
        var next = new TreeMap<>(values); var components = new TreeMap<>(values.getOrDefault(key, Map.of()));
        var previous = components.getOrDefault(origin, new Component(0, 0));
        long p = previous.positive(), n = previous.negative();
        try { if (delta >= 0) p = Math.addExact(p, delta); else n = Math.addExact(n, Math.negateExact(delta)); }
        catch (ArithmeticException e) { throw new IllegalArgumentException("component overflow", e); }
        components.put(origin, new Component(p, n)); next.put(key, components); return new CounterState(next);
    }
    public CounterState merge(CounterState other) {
        var next = new TreeMap<>(values);
        other.values.forEach((key, incoming) -> {
            var components = new TreeMap<>(next.getOrDefault(key, Map.of()));
            incoming.forEach((origin, value) -> components.merge(origin, value, Component::merge));
            next.put(key, components);
        });
        return new CounterState(next);
    }
    public boolean advancesOrigin(CounterState other, String origin) {
        for (var entry : other.values.entrySet()) {
            var incoming = entry.getValue().getOrDefault(origin, new Component(0, 0));
            var local = values.getOrDefault(entry.getKey(), Map.of()).getOrDefault(origin, new Component(0, 0));
            if (incoming.positive() > local.positive() || incoming.negative() > local.negative()) return true;
        }
        return false;
    }
    public BigInteger value(String key) {
        BigInteger result = BigInteger.ZERO;
        for (var c : values.getOrDefault(key, Map.of()).values())
            result = result.add(BigInteger.valueOf(c.positive())).subtract(BigInteger.valueOf(c.negative()));
        return result;
    }
    public void write(DataOutputStream out) throws IOException {
        out.writeInt(values.size());
        for (var key : values.entrySet()) { out.writeUTF(key.getKey()); out.writeInt(key.getValue().size());
            for (var c : key.getValue().entrySet()) { out.writeUTF(c.getKey()); out.writeLong(c.getValue().positive()); out.writeLong(c.getValue().negative()); }
        }
    }
    public static CounterState read(DataInputStream in) throws IOException {
        var values = new TreeMap<String, Map<String, Component>>();
        int size = count(in);
        for (int i = 0; i < size; i++) {
            String key = in.readUTF(); var components = new TreeMap<String, Component>(); int n = count(in);
            if (!key.matches("[A-Za-z0-9_.-]{1,128}")) throw new IOException("invalid key");
            for (int j = 0; j < n; j++) { String origin = in.readUTF();
                if (components.put(origin, new Component(in.readLong(), in.readLong())) != null) throw new IOException("duplicate origin"); }
            if (values.put(key, components) != null) throw new IOException("duplicate key");
        }
        return new CounterState(values);
    }
    static int count(DataInputStream in) throws IOException {
        int n = in.readInt(); if (n < 0 || n > 100_000) throw new IOException("invalid count"); return n;
    }
    @Override public boolean equals(Object other) { return other instanceof CounterState c && values.equals(c.values); }
    @Override public int hashCode() { return values.hashCode(); }
}

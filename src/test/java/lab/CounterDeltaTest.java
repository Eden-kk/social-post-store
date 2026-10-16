package lab;

import lab.counter.*;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.*;

/** Deterministic waterline tests plus real HTTP delivery with a controllable durable sink. */
public final class CounterDeltaTest {
    static void check(boolean ok, String message) { CounterTest.check(ok, message); }
    static int size(CounterState state) throws Exception {
        var b = new ByteArrayOutputStream(); state.write(new DataOutputStream(b)); return b.size();
    }
    interface Condition { boolean get() throws Exception; }
    static void await(Condition condition) throws Exception {
        long end = System.nanoTime() + 5_000_000_000L;
        while (!condition.get()) { if (System.nanoTime() > end) throw new AssertionError("delivery timeout"); Thread.sleep(20); }
    }
    static void storeTests() throws Exception {
        var peers = Map.of(0, URI.create("http://127.0.0.1:19010"), 1, URI.create("http://127.0.0.1:19011"));
        Path dir = Files.createTempDirectory("delta-store-"); String origin;
        try (var s = new CounterStore(dir, 0, peers)) {
            origin = s.origin();
            for (int i=0;i<40;i++) s.add(origin, "initial"+i, "key"+i, 100);
            s.acknowledge(1, s.snapshot().version());
            check(s.pendingSnapshot(1)==null,"idle has no delta");
            s.add(origin,"new1","key1",1); var sent=s.pendingSnapshot(1);
            check(sent.state().keys().equals(Set.of("key1")),"one changed key only");
            check(size(sent.state()) < size(s.snapshot().state()),"delta smaller than full");
            System.out.println("SYNTHETIC_PAYLOAD_BYTES delta="+size(sent.state())+" full="+size(s.snapshot().state()));
            s.add(origin,"new2","key1",1); s.add(origin,"new3","key2",1);
            s.acknowledge(1,sent.version());
            check(s.pendingSnapshot(1).state().keys().equals(Set.of("key1","key2")),"old ACK retains all newer keys");
            check(s.pendingSnapshot(1).state().value("key1").intValueExact()==102,"cumulative not arithmetic delta");
        }
        try (var s = new CounterStore(dir,0,peers)) {
            check(s.pendingSnapshot(1).state().keys().equals(Set.of("key1","key2")),"restart preserves dirty index");
            s.acknowledge(1,s.snapshot().version());
            var foreign=new CounterState().add("foreign","other",8); s.merge(foreign);
            check(s.pendingSnapshot(1).state().keys().equals(Set.of("foreign")),"remote merge forwarded");
            long version=s.snapshot().version(); s.merge(foreign);
            check(s.snapshot().version()==version,"no-op merge does not create dirty work");
            s.acknowledge(1,version); s.acknowledge(1,version-1);
            check(s.pendingSnapshot(1)==null,"late old ACK does not regress");
        }
        // Construct a real old-format file retaining a prior request/result; migrate conservatively.
        Path legacy=Files.createTempDirectory("delta-legacy-"); String result;
        try(var s=new CounterStore(legacy,0,peers)) {origin=s.origin();result=s.add(origin,"retry","legacy",3);}
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes);
        out.writeInt(0x504e4331);out.writeUTF(origin);out.writeLong(1);
        new CounterState().add("legacy",origin,3).write(out);
        out.writeInt(1);out.writeUTF("retry");out.writeUTF("legacy");out.writeLong(3);out.writeUTF(result);
        out.writeInt(1);out.writeInt(1);out.writeLong(1);
        byte[] body=bytes.toByteArray();Files.write(legacy.resolve("counter.bin"),ByteBuffer.allocate(body.length+32).put(body).put(MessageDigest.getInstance("SHA-256").digest(body)).array());
        try(var s=new CounterStore(legacy,0,peers)) {
            check(s.add(origin,"retry","legacy",3).equals(result),"legacy dedup preserved");
            check(s.pendingSnapshot(1).state().keys().equals(Set.of("legacy")),"legacy conservative resend");
            s.acknowledge(1,s.snapshot().version());
        }
        try(var s=new CounterStore(legacy,0,peers)) {check(s.value("legacy").equals("3"),"migrated restart");check(s.pendingSnapshot(1)==null,"migration does not repeat ACK reset");}
    }
    static void transportTests() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        var received=new AtomicReference<>(new CounterState());var requests=new AtomicInteger();var drop=new AtomicBoolean(true);
        server.createContext("/counter/merge", e->{try {
            var incoming=CounterState.read(new DataInputStream(e.getRequestBody()));
            received.updateAndGet(s->s.merge(incoming));requests.incrementAndGet();
            if(drop.get()) {e.close();return;}
            byte[] reply="DURABLE".getBytes();e.sendResponseHeaders(200,reply.length);e.getResponseBody().write(reply);
        } finally {e.close();}});server.start();
        var peers=Map.of(0,URI.create("http://127.0.0.1:19010"),1,URI.create("http://127.0.0.1:"+server.getAddress().getPort()));
        try(var node=new CounterNode(0,peers,Files.createTempDirectory("delta-http-"),"/counter/merge",2500)) {
            node.start();node.add(node.origin(),"one","k\n5");
            await(()->requests.get()>=2);check(node.status().contains("pending=1"),"lost ACK remains pending");
            check(received.get().value("k").intValueExact()==5,"duplicate delivery not double counted");
            drop.set(false);await(()->node.status().contains("pending=0"));
            int before=requests.get();Thread.sleep(650);check(requests.get()==before,"idle skips high frequency deltas");
            received.set(new CounterState());check(node.status().contains("pending=0"),"repair starts with empty outbox");
            await(()->received.get().value("k").intValueExact()==5);
            System.out.println("PASS HTTP lost ACK retry, idle delta suppression, full repair with no pending writes");
        } finally {server.stop(0);}
    }
    public static void main(String[] args) throws Exception {storeTests();transportTests();System.out.println("COUNTER_DELTA_TEST_PASSED");}
}

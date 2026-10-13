package lab;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

/** Logical partitions of three local JVMs, not physical datacenters. */
public final class CounterClusterTest implements AutoCloseable {
    final Path root = Files.createTempDirectory("counter-cluster-");
    final Process[] processes = new Process[3];
    final int[] ports = new int[3];
    final String[] origins = new String[3];
    final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(200)).build();
    CounterClusterTest() throws Exception {
        var reservations = new ArrayList<ServerSocket>();
        for (int i=0;i<3;i++) { var s=new ServerSocket(0); reservations.add(s); ports[i]=s.getLocalPort(); }
        for (var s:reservations) s.close();
    }
    void start(int id) throws Exception {
        processes[id]=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-cp",System.getProperty("java.class.path"),
            "lab.Main",""+id,root.resolve("node"+id).toString(),ports[0]+","+ports[1]+","+ports[2],"--test-controls")
            .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(root.resolve("node"+id+".log").toFile())).start();
        long end=System.nanoTime()+Duration.ofSeconds(8).toNanos();
        while(System.nanoTime()<end) { try {
            var response=call(id,"/counter/status",null,null,null);
            if(response.statusCode()!=200||!processes[id].isAlive())throw new java.io.IOException("not ready");
            origins[id]=response.body().split("\n")[0].substring(7); return; }
            catch(Exception e) { Thread.sleep(50); } }
        throw new AssertionError("startup "+root);
    }
    HttpResponse<String> call(int id,String path,String body,String request,String origin) throws Exception {
        var b=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+ports[id]+path)).timeout(Duration.ofSeconds(5));
        if(body==null)b.GET();else b.POST(HttpRequest.BodyPublishers.ofString(body));
        if(request!=null)b.header("X-Request-ID",request);if(origin!=null)b.header("X-Counter-Origin",origin);
        return client.send(b.build(),HttpResponse.BodyHandlers.ofString());
    }
    String add(int id,String request,long delta) throws Exception {
        var r=call(id,"/counter/add","count\n"+delta,request,origins[id]);
        CounterTest.check(r.statusCode()==200&&r.body().startsWith("LOCAL_DURABLE"),"accept "+r.body());return r.body();
    }
    void isolate(int id) throws Exception { call(id,"/test/block",String.join(",",java.util.stream.IntStream.range(0,3).filter(x->x!=id).mapToObj(Integer::toString).toList()),null,null); }
    void stop(int id) throws Exception { if(processes[id]!=null) {processes[id].destroyForcibly();processes[id].waitFor();} }
    void converge(String value) throws Exception {
        long end=System.nanoTime()+Duration.ofSeconds(10).toNanos();
        while(System.nanoTime()<end) {
            boolean good=true;for(int i=0;i<3;i++)good &= call(i,"/counter/read","count",null,null).body().equals(value);
            if(good)return;Thread.sleep(80);
        }throw new AssertionError("convergence "+root);
    }
    void heal() throws Exception {for(int i=0;i<3;i++)call(i,"/test/block","",null,null);}
    void drained() throws Exception {
        long end=System.nanoTime()+Duration.ofSeconds(10).toNanos();
        while(System.nanoTime()<end) {
            boolean good=true;for(int i=0;i<3;i++)good &= call(i,"/counter/status",null,null,null).body().contains("pending=0\n");
            if(good)return;Thread.sleep(80);
        }throw new AssertionError("outbox not drained");
    }
    void run() throws Exception {
        System.out.println("EVIDENCE_DIR="+root);for(int i=0;i<3;i++)start(i);Thread.sleep(1800);
        for(int i=0;i<3;i++)isolate(i);
        String first=add(0,"a",7);add(1,"b",9);add(2,"c",-3);
        CounterTest.check(call(1,"/counter/add","count\n7","a",origins[0]).statusCode()==400,"cross-origin reject");
        for(int i=0;i<3;i++)CounterTest.check(!call(i,"/sql","CREATE TABLE isolated (id LONG PRIMARY KEY);","sql-"+i,null).body().startsWith("OK"),"minority SQL");
        String origin=origins[0];stop(0);start(0);isolate(0);
        CounterTest.check(origins[0].equals(origin),"restart origin");CounterTest.check(add(0,"a",7).equals(first),"restart dedup");
        heal();converge("13");System.out.println("PASS isolated writes, minority SQL protection, origin-bound retry, restart replay, exact sum");
        call(1,"/test/counter-drop-ack","true",null,null);
        call(0,"/test/block","2",null,null);call(2,"/test/block","0,1",null,null);
        add(0,"lost-ack",4);
        CounterTest.check(call(1,"/counter/read","count",null,null).body().equals("17"),"receiver persisted before discarded ACK");
        stop(0);start(0);call(1,"/test/counter-drop-ack","false",null,null);heal();converge("17");add(0,"lost-ack",4);converge("17");
        for(int i=0;i<3;i++)stop(i);for(int i=0;i<3;i++)start(i);converge("17");
        System.out.println("PASS lost ACK replay, duplicate retry and full restart");
        drained();String retired=origins[2];stop(2);
        Files.move(root.resolve("node2/counter"),root.resolve("retired-counter2"));
        start(2);CounterTest.check(!origins[2].equals(retired),"replacement incarnation");
        converge("17");drained();
        CounterTest.check(call(2,"/counter/add","count\n-3","c",retired).statusCode()==400,"old incarnation retry rejected");
        for(int i=0;i<3;i++)stop(i);
        var peers=new TreeMap<Integer,URI>();for(int i=0;i<3;i++)peers.put(i,URI.create("http://127.0.0.1:"+ports[i]));
        lab.counter.CounterState expected=null;
        for(int i=0;i<3;i++)try(var store=new lab.counter.CounterStore(root.resolve("node"+i+"/counter"),i,peers)){
            if(expected==null)expected=store.snapshot().state();else CounterTest.check(expected.equals(store.snapshot().state()),"exact component convergence");
        }
        System.out.println("PASS empty-outbox anti-entropy, fresh incarnation recovery, equal persisted components");
        System.out.println("COUNTER_CLUSTER_TEST_PASSED");
    }
    @Override public void close() throws Exception {for(int i=0;i<3;i++)stop(i);}
    public static void main(String[] args) throws Exception {try(var test=new CounterClusterTest()){test.run();}}
}

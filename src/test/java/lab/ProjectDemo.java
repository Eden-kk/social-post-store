package lab;

import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.stream.IntStream;

/** Executable local walkthrough; assertions and transcript are part of the demo. */
public final class ProjectDemo {
    private final StringBuilder transcript = new StringBuilder();
    private void show(String message) { System.out.println(message); transcript.append(message).append('\n'); }
    private void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    private String sql(SqlClient client, String id, String statement) throws Exception {
        String result = client.execute(id, statement, Duration.ofSeconds(12));
        show("SQL> " + statement + "\n" + result);
        require(result.startsWith("OK"), "ordinary SQL failed: " + result);
        return result;
    }
    private void run(CounterClusterTest t) throws Exception {
        show("EVIDENCE_DIR=" + t.root);
        show("LOCAL DEMO: three loopback JVMs simulate partitions, not physical datacenters.");
        for (int i=0;i<3;i++) t.start(i);
        var client = new SqlClient(IntStream.range(0,3).mapToObj(i -> URI.create("http://127.0.0.1:"+t.ports[i])).toList(), Duration.ofSeconds(4), Duration.ofMillis(40));
        show("1. Commit metadata and a typed counter schema through the same SQL endpoint.");
        sql(client,"posts-ddl","CREATE TABLE posts(id TEXT PRIMARY KEY,title TEXT)");
        sql(client,"post-create","INSERT INTO posts VALUES ('post','Distributed systems')");
        sql(client,"metrics-ddl","CREATE TABLE metrics(id TEXT PRIMARY KEY,value COUNTER)");
        String metadata = sql(client,"post-before","SELECT * FROM posts");
        String[] origins = new String[3];
        for (int i=0;i<3;i++) {
            SqlCounterClusterTest.awaitValue(t,i,"0");
            origins[i]=t.call(i,"/sql-counter/status",null,null,null).body().split("\n")[0].substring(7);
        }
        show("2. Isolate every node: ordinary SQL must not acknowledge success; counters accept locally.");
        for (int i=0;i<3;i++) t.isolate(i);
        for (int i=0;i<3;i++) {
            var denied=t.call(i,"/sql","SELECT * FROM posts","partition-read-"+i,null);
            show("node="+i+" ordinary read: "+denied.body());
            require(!denied.body().startsWith("OK"),"minority returned committed SQL success");
        }
        long[] deltas={7,9,-3}; String first=null;
        for (int i=0;i<3;i++) {
            String statement="INCREMENT metrics.value BY "+deltas[i]+" WHERE id='post'";
            var result=t.call(i,"/sql",statement,"increment-"+i,origins[i]);
            show("node="+i+" SQL> "+statement+"\n"+result.body());
            require(result.statusCode()==200&&result.body().startsWith("LOCAL_DURABLE"),"counter acceptance");
            if(i==0)first=result.body();
        }
        show("3. Kill and restart an origin; retry its same request ID without double counting.");
        t.stop(0);t.start(0);t.isolate(0);
        String retry=SqlCounterClusterTest.sql(t,0,"increment-0","INCREMENT metrics.value BY 7 WHERE id='post'",origins[0]);
        require(retry.equals(first),"retry changed original result");show("same-origin retry: "+retry);
        show("4. Heal: durable state replay and anti-entropy converge to 7 + 9 - 3 = 13.");
        t.heal();for(int i=0;i<3;i++)SqlCounterClusterTest.awaitValue(t,i,"13");
        for(int i=0;i<3;i++)show("node="+i+" "+SqlCounterClusterTest.sql(t,i,"show","SELECT EVENTUAL value FROM metrics WHERE id='post'",null));
        show("5. Restart all JVMs; independently recover SQL metadata, schema and counter state.");
        for(int i=0;i<3;i++)t.stop(i);for(int i=0;i<3;i++)t.start(i);
        require(sql(client,"post-after","SELECT * FROM posts").equals(metadata),"metadata changed");
        for(int i=0;i<3;i++)SqlCounterClusterTest.awaitValue(t,i,"13");
        show("LIMIT: posts and metrics share an application key, not a foreign key or atomic transaction.");
        show("LIMIT: LOCAL_DURABLE is one local disk; permanent loss before replication can lose an accepted increment.");
        show("LIMIT: fixed membership, bounded full-state replication, no authentication or production deployment.");
    }
    public static void main(String[] args) throws Exception {
        var demo=new ProjectDemo();
        var cluster=new CounterClusterTest();
        try (cluster) {
            demo.run(cluster);
        } catch (Exception | AssertionError failure) {
            demo.show("DEMO_FAILED: "+failure);
            throw failure;
        } finally {
            Files.writeString(cluster.root.resolve("demo-transcript.txt"),demo.transcript.toString());
        }
        demo.show("DEMO_PASSED; all spawned JVMs stopped; node logs and data retained.");
        Files.writeString(cluster.root.resolve("demo-transcript.txt"),demo.transcript.toString());
    }
}

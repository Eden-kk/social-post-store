package lab;

import java.time.Duration;

/** Real JVM lifecycle through the unified SQL entrypoint. */
public final class SqlCounterClusterTest {
    static String sql(CounterClusterTest t, int node, String id, String sql, String origin) throws Exception {
        return t.call(node,"/sql",sql,id,origin).body();
    }
    static void awaitValue(CounterClusterTest t, int node, String expected) throws Exception {
        long end = System.nanoTime() + Duration.ofSeconds(12).toNanos();
        while (System.nanoTime() < end) {
            if (sql(t,node,"read","SELECT EVENTUAL value FROM metrics WHERE id='post'",null).equals("OK EVENTUAL "+expected)) return;
            Thread.sleep(60);
        }
        throw new AssertionError("value " + node + " expected " + expected);
    }
    public static void main(String[] args) throws Exception {
        try (var t = new CounterClusterTest()) {
            System.out.println("EVIDENCE_DIR="+t.root);
            for(int i=0;i<3;i++)t.start(i);
            t.isolate(2);
            t.call(0,"/test/block","2",null,null);t.call(1,"/test/block","2",null,null);
            var client = new SqlClient(java.util.stream.IntStream.range(0,3).mapToObj(i -> java.net.URI.create("http://127.0.0.1:"+t.ports[i])).toList(),Duration.ofSeconds(4),Duration.ofMillis(40));
            SqlCounterTest.check(client.execute("ddl","CREATE TABLE metrics(id TEXT PRIMARY KEY,value COUNTER)",Duration.ofSeconds(12)).equals("OK CREATE"),"committed DDL");
            SqlCounterTest.check(client.execute("bad-quote","SELECT 'unterminated",Duration.ofSeconds(12)).startsWith("ERR"),"malformed SQL terminal error");
            String[] origin = new String[3];
            for(int i=0;i<3;i++)origin[i]=t.call(i,"/sql-counter/status",null,null,null).body().split("\n")[0].substring(7);
            SqlCounterTest.check(t.call(2,"/sql","INCREMENT metrics.value BY 99 WHERE id='post'","lag",origin[2]).statusCode()==400,"schema lag rejects");
            t.heal();for(int i=0;i<3;i++)awaitValue(t,i,"0");
            for(int i=0;i<3;i++)t.isolate(i);
            String[] commands={"INCREMENT metrics.value BY 7 WHERE id='post'","INCREMENT metrics.value BY 9 WHERE id='post'","INCREMENT metrics.value BY -3 WHERE id='post'"};
            String first=sql(t,0,"a",commands[0],origin[0]);
            SqlCounterTest.check(first.startsWith("LOCAL_DURABLE"),"local durable");
            for(int i=1;i<3;i++)SqlCounterTest.check(sql(t,i,"a",commands[i],origin[i]).startsWith("LOCAL_DURABLE"),"partition acceptance");
            SqlCounterTest.check(t.call(1,"/sql",commands[0],"a",origin[0]).statusCode()==400,"wrong origin");
            SqlCounterTest.check(t.call(0,"/sql",commands[1],"a",origin[0]).statusCode()==400,"changed payload");
            String typedKey=new SqlEngine.CounterCommand("metrics","value","TEXT","post",null).storageKey();
            SqlCounterTest.check(t.call(0,"/counter/add",typedKey+"\n1000","legacy",t.origins[0]).statusCode()==200,"legacy accepts separately");
            awaitValue(t,0,"7");
            t.stop(0);t.start(0);t.isolate(0);
            SqlCounterTest.check(sql(t,0,"a",commands[0],origin[0]).equals(first),"restart pinned retry");
            t.heal();for(int i=0;i<3;i++)awaitValue(t,i,"13");
            t.call(1,"/test/sql-counter-drop-ack","true",null,null);
            t.call(0,"/test/block","2",null,null);t.isolate(2);
            String ackLost=sql(t,0,"lost-ack","INCREMENT metrics.value BY 4 WHERE id='post'",origin[0]);
            SqlCounterTest.check(ackLost.startsWith("LOCAL_DURABLE"),"lost ACK local acceptance");
            awaitValue(t,1,"17");
            SqlCounterTest.check(!t.call(0,"/sql-counter/status",null,null,null).body().contains("pending=0\n"),"unacknowledged outbox retained");
            t.stop(0);t.start(0);
            SqlCounterTest.check(sql(t,0,"lost-ack","INCREMENT metrics.value BY 4 WHERE id='post'",origin[0]).equals(ackLost),"lost ACK restart retry");
            t.call(1,"/test/sql-counter-drop-ack","false",null,null);t.heal();
            for(int i=0;i<3;i++)awaitValue(t,i,"17");
            for(String statement:new String[]{"SELECT * FROM metrics","INSERT INTO metrics VALUES ('post',9)","UPDATE metrics SET value=9 WHERE id='post'","DELETE FROM metrics WHERE id='post'"})
                SqlCounterTest.check(client.execute(java.util.UUID.randomUUID().toString(),statement,Duration.ofSeconds(12)).startsWith("ERR"),"normal SQL blocked");
            for(int i=0;i<3;i++)t.stop(i);for(int i=0;i<3;i++)t.start(i);
            for(int i=0;i<3;i++)awaitValue(t,i,"17");
            System.out.println("PASS Raft DDL, lagging-schema rejection, sparse zero, partition writes, namespace isolation, origin/payload checks, lost-ACK restart retry, exact convergence");
            System.out.println("SQL_COUNTER_CLUSTER_TEST_PASSED");
        }
    }
}

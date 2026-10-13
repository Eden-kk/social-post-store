package lab;

public final class SqlCounterTest {
    static void check(boolean ok, String why) { if (!ok) throw new AssertionError(why); }
    static void rejected(Runnable action) {
        try { action.run(); throw new AssertionError("expected rejection"); }
        catch (IllegalArgumentException expected) {}
    }
    public static void main(String[] args) throws Exception {
        var db = new SqlEngine();
        check(db.execute("CREATE TABLE bad(id COUNTER PRIMARY KEY,c LONG)").startsWith("ERR"), "counter PK");
        check(db.execute("CREATE TABLE bad(id LONG PRIMARY KEY,c COUNTER,x TEXT)").startsWith("ERR"), "mixed");
        check(db.execute("CREATE TABLE metrics(id TEXT PRIMARY KEY,value COUNTER)").equals("OK CREATE"), "ddl");
        check(db.execute("CREATE TABLE numeric(id LONG PRIMARY KEY,value COUNTER)").equals("OK CREATE"), "long ddl");
        for (String sql : new String[]{"INSERT INTO metrics VALUES ('x',2)", "UPDATE metrics SET value=3 WHERE id='x'",
                "DELETE FROM metrics WHERE id='x'", "SELECT * FROM metrics", "INCREMENT metrics.value BY 2 WHERE id='x'"})
            check(db.execute(sql).startsWith("ERR"), "no replay CRDT " + sql);
        var a = db.counterCommand("INCREMENT metrics.value BY -2 WHERE id='a.b''c'");
        check(a.delta() == -2 && a.key().equals("a.b'c"), "text and signed");
        check(a.storageKey().equals(db.counterCommand("SELECT EVENTUAL value FROM metrics WHERE id='a.b''c'").storageKey()), "same address");
        check(db.counterCommand("INCREMENT numeric.value BY +2 WHERE id=+01").storageKey()
            .equals(db.counterCommand("SELECT EVENTUAL value FROM numeric WHERE id=1").storageKey()), "canonical number");
        for (String sql : new String[]{"INCREMENT missing.value BY 2 WHERE id='x'", "INCREMENT metrics.id BY 2 WHERE id='x'",
                "INCREMENT metrics.value BY 2 WHERE value='x'", "INCREMENT metrics.value BY 2 WHERE id=1",
                "INCREMENT metrics.value BY 2 WHERE id='x'; DELETE FROM metrics WHERE id='x'", "SELECT EVENTUAL value FROM metrics"})
            rejected(() -> db.counterCommand(sql));
        var client = new SqlClient(java.util.List.of(java.net.URI.create("http://127.0.0.1:1")), java.time.Duration.ofMillis(10), java.time.Duration.ZERO);
        try { client.execute("x", "INCREMENT metrics.value BY 1 WHERE id='x'", java.time.Duration.ofMillis(10));
            throw new AssertionError("client must reject"); } catch (IllegalArgumentException expected) {}
        System.out.println("SQL_COUNTER_TEST_PASSED");
    }
}

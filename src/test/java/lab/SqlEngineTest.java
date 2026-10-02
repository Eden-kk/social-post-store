package lab;

import java.util.List;
import java.util.Random;

public final class SqlEngineTest {
    public static void main(String[] args) {
        SqlEngine db = new SqlEngine();
        check("OK CREATE", db.execute("CREATE TABLE accounts (id LONG PRIMARY KEY, balance LONG, label TEXT);"));
        check("OK INSERT 1", db.execute("INSERT INTO accounts VALUES (2, 40, 'Bob''s')"));
        check("OK INSERT 1", db.execute("insert into ACCOUNTS values (1, -5, 'A')"));
        check("OK SELECT {\"columns\":[\"id\",\"balance\",\"label\"],\"rows\":[[1,-5,\"A\"],[2,40,\"Bob's\"]]}",
                db.execute("SELECT * FROM accounts"));
        check("OK UPDATE 1", db.execute("UPDATE accounts SET balance = balance + 7 WHERE id = 1"));
        check("OK UPDATE 1", db.execute("UPDATE accounts SET balance = balance + -1 WHERE id = 1"));
        check("OK UPDATE 1", db.execute("UPDATE accounts SET label = 'line\n\"\\' WHERE id = 1"));
        check("OK SELECT {\"columns\":[\"balance\",\"label\"],\"rows\":[[1,\"line\\u000a\\\"\\\\\"]]}",
                db.execute("SELECT balance,label FROM accounts WHERE id = 1"));
        check("OK UPDATE 0", db.execute("UPDATE accounts SET balance = 3 WHERE id = 99"));
        check("OK DELETE 0", db.execute("DELETE FROM accounts WHERE id = 99"));
        check("OK SELECT {\"columns\":[\"id\"],\"rows\":[]}", db.execute("SELECT id FROM accounts WHERE id = 99"));

        String snapshot = db.execute("SELECT * FROM accounts");
        for (String bad : List.of("INSERT INTO accounts VALUES (1, 2, 'duplicate')",
                "INSERT INTO accounts VALUES (3, 2, 'new'); DELETE FROM accounts WHERE id = 1",
                "INSERT INTO accounts VALUES (3, 2)", "INSERT INTO accounts VALUES (3, 2, NULL)",
                "INSERT INTO accounts VALUES ('wrong', 2, 'x')", "INSERT INTO accounts VALUES (3, 2, 'x', 4)",
                "UPDATE accounts SET balance = 100 WHERE id = 1 trailing", "UPDATE accounts SET id = 7 WHERE id = 1",
                "UPDATE accounts SET label = label + 1 WHERE id = 1", "DELETE FROM accounts WHERE balance = 1",
                "DELETE FROM accounts WHERE id = 1; garbage", "SELECT missing FROM accounts", "SELECT * FROM absent",
                "INSERT INTO accounts VALUES (3, 9223372036854775808, 'x')", "SELECT * FROM accounts WHERE id = +-1")) {
            error(db.execute(bad));
            check(snapshot, db.execute("SELECT * FROM accounts"));
        }
        check("OK UPDATE 1", db.execute("UPDATE accounts SET balance = 9223372036854775807 WHERE id = 1"));
        snapshot = db.execute("SELECT * FROM accounts");
        check("ERR integer overflow", db.execute("UPDATE accounts SET balance = balance + 1 WHERE id = 1"));
        check(snapshot, db.execute("SELECT * FROM accounts"));
        check("OK DELETE 1", db.execute("DELETE FROM accounts WHERE id = 2"));
        error(db.execute("CREATE TABLE bad (id LONG)"));
        error(db.execute("CREATE TABLE bad (id LONG PRIMARY KEY, id TEXT)"));
        error(db.execute("CREATE TABLE bad (id LONG PRIMARY KEY, other LONG PRIMARY KEY)"));
        check("OK CREATE", db.execute("CREATE TABLE bad (id TEXT PRIMARY KEY)"));
        check("OK INSERT 1", db.execute("INSERT INTO bad VALUES ('z')"));
        check("OK INSERT 1", db.execute("INSERT INTO bad VALUES ('a')"));
        check("OK SELECT {\"columns\":[\"id\"],\"rows\":[[\"a\"],[\"z\"]]}", db.execute("SELECT * FROM bad"));
        error(db.execute(null));
        error(db.execute("x".repeat(65_537)));
        error(db.execute("'unterminated"));
        error(db.execute(""));
        verifyReplayAndFuzz();
        System.out.println("SqlEngineTest passed");
    }

    private static void verifyReplayAndFuzz() {
        SqlEngine first = new SqlEngine();
        SqlEngine second = new SqlEngine();
        for (String sql : List.of("CREATE TABLE t (k LONG PRIMARY KEY, v LONG)",
                "INSERT INTO t VALUES (-9223372036854775808, +2)",
                "UPDATE t SET v=v+3 WHERE k=-9223372036854775808", "SELECT * FROM t")) {
            check(first.execute(sql), second.execute(sql));
        }
        Random random = new Random(7);
        String alphabet = "abc_123-'();,*=+ \n\u0000\ud800";
        for (int i = 0; i < 10_000; i++) {
            StringBuilder sql = new StringBuilder();
            int length = random.nextInt(100);
            for (int j = 0; j < length; j++) sql.append(alphabet.charAt(random.nextInt(alphabet.length())));
            check(first.execute(sql.toString()), second.execute(sql.toString()));
        }
    }

    private static void check(String expected, String actual) {
        if (!expected.equals(actual)) throw new AssertionError("Expected " + expected + " but got " + actual);
    }

    private static void error(String actual) {
        if (!actual.startsWith("ERR ")) throw new AssertionError("Expected error but got " + actual);
    }
}

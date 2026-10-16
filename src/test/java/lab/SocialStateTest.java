package lab;

public final class SocialStateTest {
    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    public static void main(String[] args) {
        var state = new SocialState();
        check(state.execute("CREATE\tp\tu\tref").equals("OK CREATED version=1"), "create");
        check(state.execute("LIKE\tp\tu\tphone\t1\ttrue").endsWith("count=1"), "first like");
        check(state.execute("LIKE\tp\tu\tphone\t3\ttrue").endsWith("count=1"), "new desired same state");
        check(state.execute("LIKE\tp\tu\tphone\t2\tfalse").startsWith("OK STALE"), "out of order");
        check(state.execute("LIKE\tp\tu\tphone\t3\tfalse").startsWith("ERR"), "conflicting sequence");
        check(state.execute("LIKE\tp\tu\tlaptop\t4\tfalse").equals("ERR writer conflict"), "second device explicit conflict");
        check(state.execute("LIKE\tp\tv\tdevice\t1\ttrue").endsWith("count=2"), "different user");
        check(state.execute("LIKE\tp\tu\tphone\t4\tfalse").endsWith("count=1"), "unlike");
        check(state.execute("LIKE\tp\tu\tphone\t4\tfalse").startsWith("OK SAME"), "semantic retry");
        check(state.execute("UPDATE\tp\tu\t1\tref2").equals("OK UPDATE version=2"), "conditional update");
        check(state.execute("DELETE\tp\tv\t2").startsWith("ERR"), "owner check");
        check(state.execute("DELETE\tp\tu\t1").startsWith("ERR version conflict"), "stale delete");
        check(state.execute("DELETE\tp\tu\t2").equals("OK DELETE version=3"), "delete");
        check(state.execute("UPDATE\tp\tu\t3\tref3").equals("ERR post deleted"), "no resurrection update");
        check(state.execute("CREATE\tp\tu\tref").startsWith("ERR"), "no resurrection create");
        check(state.execute("LIKE\tp\tu\tphone\t5\ttrue").equals("ERR post deleted"), "no deleted like");
        try { state.requireLocallyActive("p"); throw new AssertionError("deleted view"); }
        catch (IllegalArgumentException expected) { }
        check(state.execute("GET\tp").contains("\"likes\":1"), "rejected commands unchanged");
        check(state.execute("CREATE\tx\tu\tbad\nref").startsWith("ERR"), "bad payload");
        System.out.println("SOCIAL_STATE_TEST_PASSED");
    }
}

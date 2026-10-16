package lab;

import java.util.HashMap;
import java.util.Map;

/** Deterministic social commands applied only through the committed Raft prefix. */
public final class SocialState {
    public static final String PREFIX = "@SOCIAL1\t";
    private record Like(String writer, long sequence, boolean liked) {}
    private static final class Post {
        final String owner;
        String content;
        long version = 1, likes;
        boolean deleted;
        final Map<String, Like> relationships = new HashMap<>();
        Post(String owner, String content) { this.owner = owner; this.content = content; }
    }
    private final Map<String, Post> posts = new HashMap<>();

    public static String identifier(String value) {
        if (value == null || !value.matches("[A-Za-z0-9_-]{1,64}"))
            throw new IllegalArgumentException("identifier must be 1-64 ASCII letters/digits/_/-");
        return value;
    }
    private static void fields(String[] parts, int count) {
        if (parts.length != count) throw new IllegalArgumentException("wrong field count");
    }
    private static long positive(String value) {
        long number = Long.parseLong(value);
        if (number <= 0) throw new IllegalArgumentException("positive version/sequence required");
        return number;
    }
    private static String content(String value) {
        if (value.isEmpty() || value.length() > 2048 || value.chars().anyMatch(c -> c < 32))
            throw new IllegalArgumentException("content reference must be 1-2048 printable characters");
        return value;
    }
    private Post post(String id) {
        Post post = posts.get(identifier(id));
        if (post == null) throw new IllegalArgumentException("post missing");
        return post;
    }
    public synchronized void requireLocallyActive(String id) {
        if (post(id).deleted) throw new IllegalArgumentException("post deleted locally");
    }
    public synchronized String execute(String command) {
        try {
            String[] p = command.split("\t", -1);
            return switch (p[0]) {
                case "CREATE" -> {
                    fields(p, 4); identifier(p[1]); identifier(p[2]); content(p[3]);
                    if (posts.containsKey(p[1])) yield "ERR post id already used (including tombstones)";
                    posts.put(p[1], new Post(p[2], p[3]));
                    yield "OK CREATED version=1";
                }
                case "UPDATE", "DELETE" -> {
                    fields(p, p[0].equals("UPDATE") ? 5 : 4);
                    Post post = post(p[1]); identifier(p[2]); long expected = positive(p[3]);
                    if (p[0].equals("UPDATE")) content(p[4]);
                    if (!post.owner.equals(p[2])) yield "ERR owner mismatch";
                    if (post.deleted) yield "ERR post deleted";
                    if (post.version != expected) yield "ERR version conflict current=" + post.version;
                    long next = Math.addExact(post.version, 1);
                    if (p[0].equals("DELETE")) post.deleted = true;
                    else post.content = p[4];
                    post.version = next;
                    yield "OK " + p[0] + " version=" + next;
                }
                case "LIKE" -> {
                    fields(p, 6);
                    Post post = post(p[1]); identifier(p[2]); identifier(p[3]);
                    long sequence = positive(p[4]);
                    if (!p[5].equals("true") && !p[5].equals("false"))
                        throw new IllegalArgumentException("desired state must be true/false");
                    boolean desired = Boolean.parseBoolean(p[5]);
                    if (post.deleted) yield "ERR post deleted";
                    Like old = post.relationships.get(p[2]);
                    if (old != null && !old.writer.equals(p[3])) yield "ERR writer conflict";
                    if (old != null && sequence < old.sequence) yield "OK STALE current=" + old.sequence;
                    if (old != null && sequence == old.sequence) {
                        yield old.liked == desired ? "OK SAME sequence=" + sequence : "ERR sequence payload conflict";
                    }
                    long delta = (desired ? 1 : 0) - (old != null && old.liked ? 1 : 0);
                    long nextCount = Math.addExact(post.likes, delta);
                    post.relationships.put(p[2], new Like(p[3], sequence, desired));
                    post.likes = nextCount;
                    yield "OK LIKE sequence=" + sequence + " liked=" + desired + " count=" + nextCount;
                }
                case "GET" -> {
                    fields(p, 2); Post post = post(p[1]);
                    yield "OK POST {\"owner\":" + json(post.owner) + ",\"content\":" + json(post.content)
                        + ",\"version\":" + post.version + ",\"deleted\":" + post.deleted + ",\"likes\":" + post.likes + "}";
                }
                case "GET_LIKE" -> {
                    fields(p, 3); Post post = post(p[1]); identifier(p[2]);
                    Like like = post.relationships.get(p[2]);
                    yield like == null ? "OK LIKE absent" : "OK LIKE writer=" + like.writer
                        + " sequence=" + like.sequence + " liked=" + like.liked;
                }
                default -> "ERR unknown social command";
            };
        } catch (IllegalArgumentException | ArithmeticException e) {
            return "ERR " + e.getMessage();
        }
    }
    private static String json(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}

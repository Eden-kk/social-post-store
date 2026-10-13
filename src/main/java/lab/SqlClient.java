package lab;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;

/** Bounded retries for the local lab protocol; the caller owns durable request IDs. */
public final class SqlClient {
    private final List<URI> endpoints;
    private final long attemptNanos;
    private final long backoffNanos;
    private final HttpClient http;

    public SqlClient(List<URI> endpoints, Duration attemptTimeout, Duration backoff) {
        if (endpoints == null || endpoints.isEmpty()) throw new IllegalArgumentException("endpoints required");
        for (URI endpoint : endpoints) {
            if (endpoint == null || !"http".equals(endpoint.getScheme()) || endpoint.getHost() == null
                    || endpoint.getUserInfo() != null || endpoint.getQuery() != null
                    || endpoint.getFragment() != null || endpoint.getPort() < -1 || endpoint.getPort() == 0
                    || endpoint.getPort() > 65535
                    || !(endpoint.getPath().isEmpty() || endpoint.getPath().equals("/")))
                throw new IllegalArgumentException("endpoint must be an HTTP origin");
        }
        this.endpoints = List.copyOf(endpoints);
        attemptNanos = nanos(attemptTimeout, false);
        backoffNanos = nanos(backoff, true);
        http = HttpClient.newBuilder().connectTimeout(attemptTimeout)
            .followRedirects(HttpClient.Redirect.NEVER).build();
    }

    /** Returns the exact OK/ERR body. Timeout does not establish whether SQL executed. */
    public String execute(String requestId, String sql, Duration deadline)
            throws IOException, InterruptedException {
        long budget = nanos(deadline, false);
        if (requestId == null || requestId.isBlank() || requestId.length() > 128
                || !requestId.equals(requestId.strip())
                || requestId.chars().anyMatch(c -> c < 33 || c > 126)
                || sql == null || sql.isBlank() || sql.length() > 8192)
            throw new IllegalArgumentException("invalid request ID or SQL");
        // Encode once: every retry must carry identical SQL, including whitespace.
        byte[] body = sql.getBytes(StandardCharsets.UTF_8);
        long started = System.nanoTime();
        int next = 0;
        Throwable lastFailure = null;
        while (true) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            long remaining = budget - (System.nanoTime() - started);
            if (remaining <= 0) throw new OutcomeUnknownException(lastFailure);
            long timeout = Math.min(attemptNanos, remaining);
            var request = HttpRequest.newBuilder(endpoints.get(next).resolve("/sql"))
                .timeout(Duration.ofNanos(timeout)).header("X-Request-ID", requestId)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
            var pending = http.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            try {
                // Unlike a header-only timeout, this bounds receipt of the complete body too.
                var response = pending.get(timeout, TimeUnit.NANOSECONDS);
                String result = response.body();
                if (response.statusCode() == 200 && (result.startsWith("OK ") || result.startsWith("ERR ")))
                    return result;
                lastFailure = new IOException("nonterminal response: HTTP " + response.statusCode());
            } catch (ExecutionException e) {
                lastFailure = e.getCause();
            } catch (TimeoutException e) {
                lastFailure = e;
            } finally {
                pending.cancel(true);
            }
            // Hints can be stale and numeric IDs need not match this list's order.
            next = (next + 1) % endpoints.size();
            remaining = budget - (System.nanoTime() - started);
            if (remaining > 0 && backoffNanos > 0)
                TimeUnit.NANOSECONDS.sleep(Math.min(backoffNanos, remaining));
        }
    }

    private static long nanos(Duration value, boolean allowZero) {
        if (value == null || value.isNegative() || (!allowZero && value.isZero()))
            throw new IllegalArgumentException("invalid duration");
        try { return value.toNanos(); }
        catch (ArithmeticException e) { throw new IllegalArgumentException("duration too large", e); }
    }

    public static final class OutcomeUnknownException extends IOException {
        public OutcomeUnknownException(Throwable cause) {
            super("deadline exhausted; outcome unknown: retry the same request ID and SQL", cause);
        }
    }
}

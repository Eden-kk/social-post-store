MiniSQL + Raft lab
=================
New October 2026 AI-assisted Java reconstruction. This is NOT recovered MIT
6.824/ZJU coursework, MySQL, a port of historical code, or production software.
Original-source ownership claims must not be attached to this new implementation.

Requirements: JDK 17+, POSIX shell. Tested locally using OpenJDK 23.0.2 with
javac --release 17; no Maven, Gradle or external dependencies required.

Run all tests:
  sh test.sh
It compiles source, runs SQL/storage tests, then repeats a real three-JVM HTTP
cluster experiment three times. Each run creates a new system temporary directory,
prints EVIDENCE_DIR, retains per-node logs and durable state, and kills its own
child processes in a finally block. No user directories are cleared. Port selection
has a small close-to-bind race; a bind failure is a test/environment error.

Manual three-node demo (three terminals, same checkout, different data dirs):
  sh build.sh
  java -cp build/classes lab.Main 0 data/demo0 19100,19101,19102
  java -cp build/classes lab.Main 1 data/demo1 19100,19101,19102
  java -cp build/classes lab.Main 2 data/demo2 19100,19101,19102

Inspect each http://127.0.0.1:19100/status (19101,19102 likewise) and use whichever
has role=leader for writes and reads; example below assumes port19100:
  curl -H 'X-Request-ID: create-1' --data-binary 'CREATE TABLE accounts (id LONG PRIMARY KEY, balance LONG);' http://127.0.0.1:19100/sql
  curl -H 'X-Request-ID: insert-1' --data-binary 'INSERT INTO accounts VALUES (1, 100);' http://127.0.0.1:19100/sql
  curl -H 'X-Request-ID: add-1' --data-binary 'UPDATE accounts SET balance = balance + 5 WHERE id = 1;' http://127.0.0.1:19100/sql
  curl -H 'X-Request-ID: read-1' --data-binary 'SELECT * FROM accounts;' http://127.0.0.1:19100/sql
Repeating add-1 with identical SQL returns the original result without another
increment. Reusing an ID with different SQL returns ERR. Each FRESH read needs a
NEW request ID; retrying a read ID returns its historical result, not a new read.
NOT_LEADER gives the last known leader ID; recheck status because hints can be old.
UNKNOWN means timeout/leadership change, not proof of failure; retry SAME ID/SQL.
HTTP200 is transport completion only: inspect OK/ERR/UNKNOWN/NOT_LEADER in body.

Java failover client
SqlClient accepts a list of configured peer origins and rotates through them on
transport failure, UNKNOWN or NOT_LEADER. It does not follow arbitrary URLs or
assume a numeric leader hint is an endpoint-list index. Example:
  var client = new SqlClient(List.of(URI.create("http://127.0.0.1:19100"),
      URI.create("http://127.0.0.1:19101"), URI.create("http://127.0.0.1:19102")),
      Duration.ofSeconds(4), Duration.ofMillis(50));
  String result = client.execute("add-1",
      "UPDATE accounts SET balance = balance + 5 WHERE id = 1;", Duration.ofSeconds(12));
The same request ID and exact SQL are preserved across retries. OK and SQL ERR
are terminal outcomes. An OutcomeUnknownException after the total deadline does
NOT prove a write failed; any caller-level retry must retain the original ID/SQL.
Each attempt and retry wait share the remaining overall deadline. Interruption
cancels the client wait but cannot undo a write already accepted by the server.
This is a bounded retry client, not a guarantee of eventual completion or
end-to-end exactly-once delivery. Fresh reads need fresh IDs as described above.

Partition-available counters
An independent PN-counter API now runs alongside Raft SQL. SQL still needs a
majority; counter writes acknowledge LOCAL_DURABLE after local persistence and
converge after logical partitions heal. This is not arbitrary SQL conflict merge.
See CRDT_VERIFICATION.txt for API, fault tests, replay semantics and capacity limits.
With the demo processes above running:
  curl http://127.0.0.1:19100/counter/status
Copy its exact origin value (including UUID) into the request below:
  curl -H 'X-Counter-Origin: <origin>' -H 'X-Request-ID: count-1' --data-binary 'visits
5' http://127.0.0.1:19100/counter/add
  curl --data-binary 'visits' http://127.0.0.1:19101/counter/read
The second node may initially be stale; background replay/anti-entropy propagates
state. Retry the same request only on its original incarnation, with the same ID
and payload. Do NOT use SqlClient failover for counter requests. Replay coalesces
cumulative components in a durable local outbox, not a Kafka/MQ deployment.

SQL scope
LONG/TEXT, exactly one inline PRIMARY KEY, CREATE TABLE; positional INSERT;
SELECT * or projection, optional primary-key equality WHERE; single-column
UPDATE (literal or LONG column + signed integer), and DELETE with primary-key
WHERE. Results sort by primary key. No joins, NULL, arbitrary predicates,
multi-statement transactions, secondary indexes or full SQL compatibility.
SQL failures are deterministic replicated outcomes, not node storage errors.

Architecture and invariants
Client -> leader -> persistent log -> majority/current-term Raft commit ->
ordered SQL execution -> result. The current-term no-op on election commits
inherited prefixes safely. Fresh SELECT requests go through the same log.
Raft implements randomized elections, persisted votes/terms, log matching,
conflict truncation and next/match progress for a fixed three-peer group.
Every node persists term/vote/log/commit in a checksummed whole-state file,
forcing the temp file, atomic replacement and directory before acknowledgement.
On startup SQL state and dedup results are rebuilt from EMPTY by replaying only
the durable committed prefix; there is no separately mutable persistent SQL file.
This eliminates a DB-mutation/applied-index split-write window for this design.
Command ID, original SQL and result are logically replicated via replay. IDs are
retained indefinitely in this bounded lab, not a production dedup retention design.

Limitations
Full log and dedup grow without bound; each durable save rewrites the full state.
No snapshot/compaction, sharding, dynamic membership, Redis, ZooKeeper, TLS/auth,
cross-machine benchmarks, linearizability history checker or performance claims.
Normal endpoints bind only to localhost; all local peers/callers are trusted.
--test-controls explicitly enables local partition injection; NEVER expose it.
Process SIGKILL tests are not physical power-loss/filesystem durability tests.
Directory fsync and atomic moves must be supported by the local filesystem.
Reuse each data directory only for the SAME node ID and SAME fixed membership.
An atomic identity file binds node ID and the exact peer URI map before any Raft
state is loaded. Different IDs/endpoints or legacy state without identity are
rejected, not silently migrated. Failed initialization releases the directory lock.
The binding is configuration identity, not a globally unique cluster incarnation:
do not reuse old directories when creating a new logical cluster on the same ports.
The bounded tests demonstrate specific scenarios, not a proof of Raft correctness.

Reading references (architectural inspiration, no library implementation imported)
https://raft.github.io/raft.pdf
https://github.com/rqlite/rqlite
https://rqlite.io/docs/api/read-consistency/
https://github.com/etcd-io/raft
https://www.sofastack.tech/en/projects/sofa-jraft/jraft-user-guide/

Resume boundary
Describe as an AI-assisted independent reconstruction/extension, dated October
2026, only after reviewing the code and understanding the mechanisms. Can claim
restricted SQL + Raft integration and the specific verified local tests; cannot
claim historical integration, MySQL compatibility, production readiness,
benchmarked performance gains, or snapshots not implemented here.

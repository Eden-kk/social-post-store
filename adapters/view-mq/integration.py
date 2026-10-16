"""Real RabbitMQ + three Java processes; retained local logs, no mock broker."""
import argparse
import json
import pathlib
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from view_mq import connect, declare, queue_name, request

ROOT = pathlib.Path(__file__).resolve().parents[2]


def wait(check, label, timeout=30):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            last = check()
            if last:
                return last
        except Exception as error:
            last = error
        time.sleep(0.1)
    raise AssertionError(f"timeout {label}: {last}")


def ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for item in sockets:
            item.bind(("127.0.0.1", 0))
        return [item.getsockname()[1] for item in sockets]
    finally:
        for item in sockets:
            item.close()


def run(args):
    root = pathlib.Path(tempfile.mkdtemp(prefix="social-mq-"))
    print("EVIDENCE_DIR=" + str(root), flush=True)
    allocated = ports(4)
    nodes = allocated[:3]
    processes = []

    def start(command, name):
        with (root / (name + ".log")).open("ab") as output:
            process = subprocess.Popen(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)
        processes.append(process)
        return process

    def stop(process):
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)

    def url(node, path):
        return f"http://127.0.0.1:{nodes[node]}{path}"

    def social(body):
        rid = str(uuid.uuid4())
        def attempt():
            for port in nodes:
                code, text = request(f"http://127.0.0.1:{port}/social", body.encode(), {"X-Request-ID": rid})
                if text.startswith("OK"):
                    return text
            return False
        return wait(attempt, body)

    try:
        for node in range(3):
            start(["java", "-cp", "build/classes", "lab.Main", str(node), str(root / f"node{node}"),
                   ",".join(map(str, nodes))], f"node{node}")
        for node in range(3):
            wait(lambda: request(url(node, "/status"))[0] == 200, "node startup")
        social("CREATE\tp\towner\tobject://p")
        social("CREATE\tq\towner\tobject://q")
        wait(lambda: request(url(0, "/social/views"), b"p") == (200, "OK EVENTUAL 0"), "metadata replay")
        args.origin = request(url(0, "/social/views/status"))[1].splitlines()[0][7:]
        common = [sys.executable, "adapters/view-mq/view_mq.py", "--origin", args.origin,
                  "--broker-port", str(args.broker_port), "--max-messages", "3"]
        ingress = start(common + ["serve", "--port", str(allocated[3])], "ingress")
        ingress_url = f"http://127.0.0.1:{allocated[3]}/views"
        wait(lambda: "INGRESS_READY" in (root / "ingress.log").read_text(), "ingress startup")

        def publish(eid, post="p"):
            return request(ingress_url, json.dumps({"origin": args.origin, "event_id": eid,
                                                    "post_id": post}).encode())

        def depth():
            with connect(args) as connection:
                return declare(connection.channel(), args.origin, 3).method.message_count

        def worker(name="consumer", crash=False, target=None):
            return start(common + ["consume", "--target-port", str(target or nodes[0])]
                         + (["--test-crash-after-apply"] if crash else []), name)

        def broker_empty():
            output = subprocess.check_output(["docker", "--context", args.docker_context, "exec",
                args.container, "rabbitmqctl", "-q", "list_queues", "name", "messages_ready",
                "messages_unacknowledged", "--formatter", "json"], text=True)
            queue = next(item for item in json.loads(output) if item["name"] == queue_name(args.origin))
            return queue["messages_ready"] == 0 and queue["messages_unacknowledged"] == 0

        def acked(log, event_id):
            wait(lambda: event_id in (root / (log + ".log")).read_text(), "ACK sent " + event_id)

        def count(expected):
            return request(url(0, "/social/views"), b"p") == (200, f"OK EVENTUAL {expected}")

        for index in range(3):
            assert publish(f"backlog{index}")[0] == 202
        assert depth() == 3 and count(0)
        assert publish("overflow")[0] == 503
        assert depth() == 3
        print("PASS paused consumer backlog=3, bounded queue rejects overflow without QUEUED", flush=True)

        # This is deliberately real broker restart with its named data volume, not a stub.
        subprocess.run(["docker", "--context", args.docker_context, "restart", args.container], check=True)
        wait(lambda: depth() == 3, "broker persistent queue after restart", 60)
        consumer = worker()
        wait(lambda: count(3) and depth() == 0, "resume backlog")
        acked("consumer", "APPLIED backlog2")
        wait(broker_empty, "all backlog ACKs processed")
        assert publish("backlog0")[0] == 202
        wait(lambda: (root / "consumer.log").read_text().count("APPLIED backlog0") == 2, "duplicate ACK sent")
        wait(broker_empty, "duplicate ACK processed")
        assert count(3)
        stop(consumer)
        assert broker_empty()
        print("PASS broker restart retains confirmed backlog; drain and duplicate leave exact count=3", flush=True)

        assert publish("crash-window")[0] == 202
        consumer = worker("crash", crash=True)
        wait(lambda: consumer.poll() is not None, "post-apply consumer crash")
        assert consumer.returncode == 73 and count(4)
        wait(lambda: depth() == 1, "unacknowledged redelivery")
        consumer = worker("recovered")
        acked("recovered", "APPLIED crash-window")
        wait(broker_empty, "redelivery ACK processed")
        assert count(4)
        stop(consumer)
        assert broker_empty()
        print("PASS crash after LOCAL_DURABLE before ACK redelivers unchanged event without double count", flush=True)

        assert publish("transient")[0] == 202
        wrong_port = ports(1)[0]
        consumer = worker("unavailable", target=wrong_port)
        wait(lambda: "RETRY" in (root / "unavailable.log").read_text(), "unavailable retry")
        stop(consumer)
        wait(lambda: depth() == 1, "transient retained")
        consumer = worker("target-restored")
        wait(lambda: count(5) and depth() == 0, "target restored")
        acked("target-restored", "APPLIED transient")
        wait(broker_empty, "restored target ACK processed")
        stop(consumer)
        print("PASS unavailable target retained/retried with bounded delay, no fallback origin", flush=True)

        assert publish("deleted-event")[0] == 202
        social("DELETE\tp\towner\t1")
        for node in range(3):
            wait(lambda: request(url(node, "/social/views"), b"p")[1] == "post deleted locally", "tombstone")
        consumer = worker("wrong-origin", target=nodes[1])
        wait(lambda: "original incarnation" in (root / "wrong-origin.log").read_text(), "wrong-origin fail closed")
        stop(consumer)
        wait(lambda: depth() == 1, "wrong-origin did not ACK tombstone")
        consumer = worker("deleted")
        wait(lambda: "SKIPPED_DELETED" in (root / "deleted.log").read_text() and depth() == 0, "deletion disposition")
        wait(broker_empty, "deletion ACK processed")
        stop(consumer)
        assert broker_empty()
        print("PASS wrong-origin tombstone cannot ACK; correct origin skips known deleted post", flush=True)

        # Same ID, different payload is not silently discarded; operator intervention is required.
        assert publish("backlog0", "q")[0] == 202
        consumer = worker("conflict")
        wait(lambda: "payload mismatch" in (root / "conflict.log").read_text(), "payload conflict retained")
        stop(consumer)
        wait(lambda: depth() == 1, "conflict remained queued")
        print("PASS payload-ID conflict retained without ACK (documented head-of-line limit)", flush=True)
        print("MQ_INTEGRATION_PASSED", flush=True)
    finally:
        for process in reversed(processes):
            stop(process)
        # Remove only this run's incarnation-named test queue, never another experiment's data.
        if hasattr(args, "origin"):
            with connect(args) as connection:
                connection.channel().queue_delete(queue_name(args.origin))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-port", type=int, default=5679)
    parser.add_argument("--docker-context", default="colima-social-post")
    parser.add_argument("--container", default="social-post-mq-demo")
    run(parser.parse_args())

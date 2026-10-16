"""Optional loopback RabbitMQ adapter. No changes to Raft or counter replication."""
import argparse
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

import pika


def event(raw):
    value = json.loads(raw)
    if not isinstance(value, dict) or set(value) != {"event_id", "post_id", "origin"}:
        raise ValueError("expected event_id, post_id, origin")
    for key in value:
        if not isinstance(value[key], str):
            raise ValueError("string fields required")
    if not re.fullmatch(r"[!-~]{1,128}", value["event_id"]):
        raise ValueError("invalid event_id")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value["post_id"]):
        raise ValueError("invalid post_id")
    if not re.fullmatch(r"[0-9]+:[0-9a-f-]{36}", value["origin"]):
        raise ValueError("invalid origin")
    return value


def queue_name(origin):
    return "social.views." + hashlib.sha256(origin.encode()).hexdigest()[:24]


def connect(args):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="127.0.0.1", port=args.broker_port,
        credentials=pika.PlainCredentials(os.environ.get("MQ_USER", "social"),
                                          os.environ.get("MQ_PASSWORD", "local-demo-only")),
        heartbeat=15, socket_timeout=5, stack_timeout=10,
        blocked_connection_timeout=5, connection_attempts=1))


def declare(channel, origin, max_messages=1000):
    # Queue identity includes incarnation; consumers cannot silently migrate pending events.
    return channel.queue_declare(queue=queue_name(origin), durable=True,
                                 arguments={"x-queue-type": "classic", "x-max-length": max_messages,
                                            "x-overflow": "reject-publish"})


def request(url, body=None, headers=None):
    req = urllib.request.Request(url, data=body, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=5) as reply:
            return reply.status, reply.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def serve(args):
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(5)

        def do_POST(self):
            status, text = 400, "invalid event"
            try:
                if self.path != "/views":
                    self.respond(404, "not found")
                    return
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 2048:
                    raise ValueError("body length must be 1..2048")
                value = event(self.rfile.read(length))
                if value["origin"] != args.origin:
                    raise ValueError("origin does not match this ingress")
                # A fresh connection per request makes the tiny demo restart-safe, not fast.
                with connect(args) as connection:
                    channel = connection.channel()
                    declare(channel, args.origin, args.max_messages)
                    channel.confirm_delivery()
                    channel.basic_publish("", queue_name(args.origin),
                        json.dumps(value, sort_keys=True).encode(), mandatory=True,
                        properties=pika.BasicProperties(delivery_mode=2,
                            content_type="application/json", message_id=value["event_id"]))
                status, text = 202, "QUEUED " + value["event_id"]
            except (ValueError, UnicodeError) as error:
                text = str(error)
            except Exception as error:
                # Publish may have reached disk before response/confirm loss. Retry same payload.
                status, text = 503, "UNKNOWN retry identical event; " + type(error).__name__
            self.respond(status, text)

        def respond(self, status, text):
            data = text.encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = HTTPServer(("127.0.0.1", args.port), Handler)
    server.timeout = 5
    print("INGRESS_READY", flush=True)
    server.serve_forever()


def consume(args):
    target = "http://127.0.0.1:" + str(args.target_port)
    while True:
        try:
            with connect(args) as connection:
                channel = connection.channel()
                declare(channel, args.origin, args.max_messages)
                while True:
                    method, properties, body = channel.basic_get(queue_name(args.origin), auto_ack=False)
                    if method is None:
                        connection.sleep(0.2)
                        continue
                    value = event(body)
                    if value["origin"] != args.origin:
                        raise ValueError("queue origin mismatch; retained for operator repair")
                    code, response = request(target + "/social/view", value["post_id"].encode(),
                        {"X-Request-ID": value["event_id"], "X-Counter-Origin": value["origin"]})
                    applied = code == 200 and response.startswith("LOCAL_DURABLE ")
                    deleted = code == 400 and response == "post deleted locally"
                    if not (applied or deleted):
                        # Keep the message; no arbitrary failover or discard on capacity/ID conflict.
                        raise RuntimeError("delivery retained: " + str(code) + " " + response)
                    if applied and args.test_crash_after_apply:
                        print("TEST_CRASH_AFTER_APPLY " + value["event_id"], flush=True)
                        os._exit(73)
                    channel.basic_ack(method.delivery_tag)
                    print(("APPLIED " if applied else "SKIPPED_DELETED ") + value["event_id"], flush=True)
        except Exception as error:
            print("RETRY " + type(error).__name__ + " " + str(error), flush=True)
            time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["serve", "consume"])
    parser.add_argument("--origin", required=True)
    parser.add_argument("--broker-port", type=int, default=5679)
    parser.add_argument("--port", type=int, default=18300)
    parser.add_argument("--target-port", type=int, default=18210)
    parser.add_argument("--max-messages", type=int, default=1000)
    parser.add_argument("--test-crash-after-apply", action="store_true")
    args = parser.parse_args()
    if args.max_messages < 1:
        parser.error("max-messages must be positive")
    event(json.dumps({"origin": args.origin, "event_id": "validate", "post_id": "validate"}))
    (serve if args.mode == "serve" else consume)(args)


if __name__ == "__main__":
    main()

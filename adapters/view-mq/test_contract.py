import json
import unittest
from view_mq import event, queue_name

ORIGIN = "0:12345678-1234-1234-1234-123456789abc"


class ContractTest(unittest.TestCase):
    def test_identity_is_stable(self):
        value = {"origin": ORIGIN, "event_id": "same-event", "post_id": "p"}
        self.assertEqual(event(json.dumps(value)), value)
        self.assertEqual(queue_name(ORIGIN), queue_name(ORIGIN))
        self.assertNotEqual(queue_name(ORIGIN), queue_name("1:" + ORIGIN[2:]))

    def test_reject_malformed(self):
        for field, invalid in [("event_id", "a b"), ("event_id", "x" * 129),
                               ("post_id", "p\n"), ("origin", "anywhere"),
                               ("post_id", 2)]:
            value = {"origin": ORIGIN, "event_id": "event", "post_id": "p"}
            value[field] = invalid
            with self.assertRaises(ValueError):
                event(json.dumps(value))
        with self.assertRaises(ValueError):
            event('{}')


if __name__ == "__main__":
    unittest.main()

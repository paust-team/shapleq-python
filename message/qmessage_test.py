import unittest
from message.qmessage import *
from struct import pack
from message.api import ack_msg
from proto.api_pb2 import Ack, Ping


class QMessageTest(unittest.TestCase):

    def test_serialize(self):
        test_len = 1
        test_checksum = 1
        test_msg_type = 0
        test_data = "test"
        msg = QMessage(QHeader(test_len, test_checksum, test_msg_type), test_data.encode('utf-8'))

        expected_hex_str = bytearray(
            pack('!I', test_len)) + pack('!I', test_checksum) + pack('!H', test_msg_type) + test_data.encode('utf-8')

        self.assertEqual(msg.serialize().hex(), expected_hex_str.hex())

    def test_make_message(self):
        expected = ack_msg(1, "test")
        expected_msg = make_qmessage_from_proto(MessageType.STREAM, expected)

        actual_msg = make_qmessage_from_buffer(expected_msg.serialize())

        actual = Ack()
        actual_msg.unpack_to(actual)

        self.assertEqual(expected.msg, actual.msg)

        fake = Ping()
        actual_msg.unpack_to(fake)
        with self.assertRaises(MessageDecodeError):
            actual_msg.unpack_to(fake)

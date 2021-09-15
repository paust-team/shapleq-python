from dataclasses import dataclass
from struct import pack, unpack, calcsize
from google.protobuf import message, any_pb2
from exception import NotEnoughBufferError, InvalidChecksumError, MessageDecodeError
from enum import Enum
import zlib


class MessageType(Enum):
    TRANSACTION = 0
    STREAM = 1


@dataclass
class QHeader:
    HEADER_SIZE = calcsize('IIH')

    len: int
    checksum: int
    msg_type: int


class QMessage:
    _header: QHeader
    _data: bytes

    def __init__(self, header: QHeader, data: bytes):
        self._header = header
        self._data = data

    def serialize(self) -> bytes:
        serialized = bytearray(
            pack('!IIH', self._header.len, self._header.checksum, self._header.msg_type)) + self._data
        return serialized

    def length(self) -> int:
        return self._header.len

    def type(self) -> int:
        return self._header.msg_type

    def is_same_msg(self, msg: message.Message) -> bool:
        return msg.SerializeToString() == self._data.hex()

    def unpack_to(self, msg: message.Message) -> message.Message:
        try:
            any_pb = any_pb2.Any()
            any_pb.ParseFromString(self._data)

        except message.DecodeError as err:
            raise MessageDecodeError(msg=err)

        return msg if any_pb.Unpack(msg) else None


def make_qmessage_from_buffer(buf: bytes) -> QMessage:
    buffer_len = len(buf)

    # check header
    if buffer_len < QHeader.HEADER_SIZE:
        raise NotEnoughBufferError()

    header_struct = unpack('!IIH', buf[:QHeader.HEADER_SIZE])
    actual_data_len = header_struct[0]
    actual_checksum = header_struct[1]
    msg_type = header_struct[2]

    # check data
    received_data_len = buffer_len - QHeader.HEADER_SIZE
    if received_data_len < actual_data_len:
        raise NotEnoughBufferError()

    data = buf[QHeader.HEADER_SIZE:QHeader.HEADER_SIZE + actual_data_len]

    if actual_checksum != zlib.crc32(data):
        raise InvalidChecksumError()

    return QMessage(
        header=QHeader(actual_data_len, actual_checksum, msg_type),
        data=data
    )


def make_qmessage_from_proto(msg_type: MessageType, msg: message.Message) -> QMessage:
    any_pb = any_pb2.Any()
    any_pb.Pack(msg)
    data = any_pb.SerializeToString()

    header = QHeader(len(data), zlib.crc32(data), msg_type.value)

    return QMessage(header=header, data=data)

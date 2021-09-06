import logging
from client import ClientBase, QConfig
from exception import InvalidMessageError, SocketClosedError
from proto.data_pb2 import SessionType
from proto.api_pb2 import FetchResponse, Ack
from typing import Generator
from message.qmessage import QMessage, MessageType, make_qmessage_from_proto
from message.api import fetch_msg


class FetchedData:
    _data: bytes
    _offset: int
    _last_offset: int

    def __init__(self, data: bytes, offset: int, last_offset: int):
        self._data = data
        self._offset = offset
        self._last_offset = last_offset

    def get_data(self) -> bytes:
        return self._data


class Consumer(ClientBase):
    topic: str

    def __init__(self, config: QConfig, topic: str):
        super().__init__(config)
        self.topic = topic

    def setup(self):
        self.connect(SessionType.SUBSCRIBER, self.topic)

    def terminate(self):
        self.close()

    def subscribe(self, start_offset: int) -> Generator[FetchedData, None, None]:
        if not self.is_connected():
            raise SocketClosedError()

        try:
            msg = make_qmessage_from_proto(MessageType.STREAM, fetch_msg(start_offset))
            self._send_message(msg)
            for received in self.continuous_receive():
                yield self._handle_message(received)
        except SocketClosedError:
            return

    def _handle_message(self, msg: QMessage) -> FetchedData:

        if (fetch_response := msg.unpack_to(FetchResponse())) is not None:
            logging.debug('received response - data: {}, last offset: {}, offset: {}'.format(
                fetch_response.data, fetch_response.last_offset, fetch_response.offset))
            return FetchedData(data=fetch_response.data,
                               offset=fetch_response.offset,
                               last_offset=fetch_response.last_offset)
        elif (ack := msg.unpack_to(Ack())) is not None:
            raise InvalidMessageError(msg=ack.msg)
        else:
            raise InvalidMessageError()

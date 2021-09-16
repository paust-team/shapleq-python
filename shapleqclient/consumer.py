from dataclasses import dataclass
from shapleqclient.base import ClientBase, QConfig
from shapleqclient.common.exception import InvalidMessageError, SocketClosedError
from shapleqclient.proto.data_pb2 import SessionType
from shapleqclient.proto.api_pb2 import FetchResponse, Ack, BatchFetchResponse
from typing import Generator, Iterable
from shapleqclient.message.qmessage import QMessage, MessageType, make_qmessage_from_proto
from shapleqclient.message.api import fetch_msg


@dataclass
class FetchedData:
    data: Iterable[bytes]
    offset: int
    last_offset: int


class Consumer(ClientBase):
    topic: str

    def __init__(self, config: QConfig, topic: str):
        super().__init__("ShapleQ-Consumer", config)
        self.topic = topic

    def setup(self):
        self.connect(SessionType.SUBSCRIBER, self.topic)

    def terminate(self):
        self.close()

    def subscribe(self, start_offset: int, max_batch_size: int = 1, flush_interval: int = 100) -> Generator[FetchedData, None, None]:
        if not self.is_connected():
            raise SocketClosedError()

        try:
            msg = make_qmessage_from_proto(MessageType.STREAM, fetch_msg(start_offset, max_batch_size, flush_interval))
            self._send_message(msg)
            for received in self.continuous_receive():
                yield self._handle_message(received)
        except SocketClosedError:
            return

    def _handle_message(self, msg: QMessage) -> FetchedData:

        if (fetch_response := msg.unpack_to(FetchResponse())) is not None:
            self.logger.debug('received response - data: {}, last offset: {}, offset: {}'.format(
                fetch_response.data, fetch_response.last_offset, fetch_response.offset))
            return FetchedData(data=[fetch_response.data],
                               offset=fetch_response.offset,
                               last_offset=fetch_response.last_offset)
        elif (batch_fetch_response := msg.unpack_to(BatchFetchResponse())) is not None:
            self.logger.debug('received response - batched data: {}, last offset: {}'.format(
                batch_fetch_response.batched, batch_fetch_response.last_offset))
            return FetchedData(data=batch_fetch_response.batched,
                               last_offset=batch_fetch_response.last_offset,
                               offset=0)
        elif (ack := msg.unpack_to(Ack())) is not None:
            raise InvalidMessageError(msg=ack.msg)
        else:
            raise InvalidMessageError()

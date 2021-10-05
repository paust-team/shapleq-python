import logging
from dataclasses import dataclass
from shapleqclient.base import ClientBase, QConfig
from shapleqclient.common.exception import InvalidMessageError, SocketClosedError, RequestFailedError
from shapleqclient.proto.data_pb2 import SessionType
from shapleqclient.proto.api_pb2 import FetchResponse, BatchedFetchResponse, Ack
from typing import Generator, Iterable
from shapleqclient.message.qmessage import QMessage, MessageType, make_qmessage_from_proto
from shapleqclient.message.api import fetch_msg


@dataclass
class FetchedData:
    data: bytes
    offset: int
    seq_num: int
    node_id: str


@dataclass
class FetchResult:
    items: Iterable[FetchedData]
    last_offset: int


class Consumer:
    topic: str
    _client: ClientBase
    logger: logging.Logger

    def __init__(self, config: QConfig, topic: str, logger: logging.Logger):
        self._client = ClientBase(config, logger)
        self.logger = logger
        self.topic = topic

    def setup(self):
        self._client.connect(SessionType.SUBSCRIBER, self.topic)

    def is_connected(self) -> bool:
        return self._client.is_connected()

    def stop(self):
        self._client.close()

    def subscribe(self, start_offset: int, max_batch_size: int = 1, flush_interval: int = 100) -> Generator[FetchResult, None, None]:
        if not self._client.is_connected():
            raise SocketClosedError()

        try:
            msg = make_qmessage_from_proto(MessageType.STREAM, fetch_msg(start_offset, max_batch_size, flush_interval))
            self._client.send_message(msg)
            for received in self._client.continuous_receive():
                yield self._handle_message(received)
        except SocketClosedError:
            return

    def _handle_message(self, msg: QMessage) -> FetchResult:

        if (fetch_response := msg.unpack_to(FetchResponse())) is not None:
            self.logger.debug('received response - data: {}, offset: {}, last offset: {}, seq_num: {}, node_id: {}'.format(
                fetch_response.data, fetch_response.offset, fetch_response.last_offset, fetch_response.seq_num, fetch_response.node_id))

            fetched = FetchedData(data=fetch_response.data,
                                  offset=fetch_response.offset,
                                  seq_num=fetch_response.seq_num,
                                  node_id=fetch_response.node_id)
            return FetchResult(items=[fetched], last_offset=fetch_response.last_offset)

        elif (batched_fetch_response := msg.unpack_to(BatchedFetchResponse())) is not None:
            self.logger.debug('received response - items: {}, last offset: {}'.format(
                batched_fetch_response.items, batched_fetch_response.last_offset))
            items = []
            for item in batched_fetch_response.items:
                items.append(FetchedData(data=item.data,
                                         offset=item.offset,
                                         seq_num=item.seq_num,
                                         node_id=item.node_id))
            return FetchResult(items=items, last_offset=batched_fetch_response.last_offset)

        elif (ack := msg.unpack_to(Ack())) is not None:
            raise RequestFailedError(msg=ack.msg)
        else:
            raise InvalidMessageError()

import logging
import threading
from shapleqclient.base import ClientBase, QConfig
from shapleqclient.common.exception import InvalidMessageError, SocketClosedError, RequestFailedError, InvalidNodeIdError
from shapleqclient.proto.api_pb2 import PutResponse, Ack
from shapleqclient.proto.data_pb2 import SessionType
from shapleqclient.message.qmessage import QMessage, MessageType, make_qmessage_from_proto
from shapleqclient.message.api import put_msg


class Producer:
    topic: str
    _client: ClientBase
    logger: logging.Logger

    def __init__(self, config: QConfig, topic: str, logger: logging.Logger):
        self._client = ClientBase(config, logger)
        self.logger = logger
        self.topic = topic

    def setup(self):
        self._client.connect(SessionType.PUBLISHER, self.topic)
        th = threading.Thread(target=self._receive_message)
        th.start()

    def is_connected(self) -> bool:
        return self._client.is_connected()

    def stop(self):
        self._client.close()

    def publish(self, data: bytes, seq_num: int, node_id: str):
        if not self._client.is_connected():
            raise SocketClosedError()
        if len(node_id) != 32:
            raise InvalidNodeIdError()
        msg = make_qmessage_from_proto(MessageType.STREAM, put_msg(data, seq_num, node_id))
        self._client.send_message(msg)

    def _receive_message(self):
        try:
            for received in self._client.continuous_receive():
                self._handle_message(received)
        except SocketClosedError:
            return

    def _handle_message(self, msg: QMessage):

        if (put_response := msg.unpack_to(PutResponse())) is not None:
            self.logger.debug('received response - partition id: {}, partition offset: {}'.format(
                put_response.partition.partition_id, put_response.partition.offset))
        elif (ack := msg.unpack_to(Ack())) is not None:
            raise RequestFailedError(msg=ack.msg)
        else:
            raise InvalidMessageError()

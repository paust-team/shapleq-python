import logging
import threading
from client import ClientBase, QConfig
from exception import InvalidMessageError, SocketClosedError
from proto.api_pb2 import PutResponse, Ack
from proto.data_pb2 import SessionType
from message.qmessage import QMessage, MessageType, make_qmessage_from_proto
from message.api import put_msg


class Producer(ClientBase):
    topic: str

    def __init__(self, config: QConfig, topic: str):
        super().__init__("ShapleQ-Producer", config)
        self.topic = topic

    def setup(self):
        self.connect(SessionType.PUBLISHER, self.topic)
        th = threading.Thread(target=self._receive_message)
        th.start()

    def terminate(self):
        self.close()

    def publish(self, data: bytes):
        if not self.is_connected():
            raise SocketClosedError()

        msg = make_qmessage_from_proto(MessageType.STREAM, put_msg(data))
        self._send_message(msg)

    def _receive_message(self):
        try:
            for received in self.continuous_receive():
                self._handle_message(received)
        except SocketClosedError:
            return

    def _handle_message(self, msg: QMessage):

        if (put_response := msg.unpack_to(PutResponse())) is not None:
            self.logger.debug('received response - partition id: {}, partition offset: {}'.format(
                put_response.partition.partition_id, put_response.partition.offset))

        elif (ack := msg.unpack_to(Ack())) is not None:
            raise InvalidMessageError(msg=ack.msg)
        else:
            raise InvalidMessageError()

import socket
from shapleqclient.proto.data_pb2 import SessionType
from shapleqclient.proto.api_pb2 import DiscoverBrokerResponse, ConnectResponse, Ack
from shapleqclient.common.exception import *
from shapleqclient.message.qmessage import *
from shapleqclient.message.api import discover_broker_msg, connect_msg
from shapleqclient.common.error import PQErrCode
from typing import Generator
import logging


class QConfig:
    DEFAULT_TIMEOUT = 3000
    DEFAULT_BROKER_HOST = "localhost"
    DEFAULT_BROKER_PORT = 1101

    # timeout should be milliseconds value
    def __init__(self, broker_address: str = DEFAULT_BROKER_HOST, broker_port: int = DEFAULT_BROKER_PORT,
                 timeout: int = DEFAULT_TIMEOUT):
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.timeout = timeout

    def get_broker_address(self) -> str:
        return self.broker_address

    def get_broker_port(self) -> int:
        return self.broker_port

    def get_timeout(self) -> int:
        return self.timeout


class ClientBase:
    connected: bool = False
    config: QConfig
    logger: logging.Logger
    _sock: socket.socket
    _RECEIVE_BUFFER_SIZE = 4 * 1024

    def __init__(self, config: QConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger

    def is_connected(self) -> bool:
        return self.connected

    def connect_to_broker(self, address: str, port: int):
        if self.connected:
            raise ClientConnectionError("already connected to broker")

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self.config.get_timeout() / 1000)
        try:
            self._sock.connect((address, port))
            self.connected = True
            self.logger.info('connected to broker target {}:{}'.format(address, port))

        except socket.timeout:
            raise ClientConnectionError("cannot connect to broker : timeout")

    def send_message(self, msg: QMessage):
        if self._sock.send(msg.serialize()) <= 0:
            raise SocketWriteError()
        self.logger.info('sent data successfully')

    def read_message(self) -> QMessage:
        msg_buf: bytearray = bytearray(b'')
        while True:
            if not self.is_connected():
                raise SocketClosedError()

            try:
                received = self._sock.recv(self._RECEIVE_BUFFER_SIZE)
                if not received:
                    self.close()
                    raise SocketClosedError()
                self.logger.info('read data successfully')

                msg_buf += bytearray(received)

                return make_qmessage_from_buffer(msg_buf)
            except NotEnoughBufferError:
                continue
            except socket.error as msg:
                self.logger.error(msg)
                self.close()
                raise SocketReadError()

    def continuous_receive(self) -> Generator[QMessage, None, None]:
        msg_buf: bytearray = bytearray(b'')

        while True:
            if not self.is_connected():
                raise SocketClosedError()

            try:
                received = self._sock.recv(self._RECEIVE_BUFFER_SIZE)
                if not received:
                    self.close()
                    raise SocketClosedError()
            except socket.timeout:
                continue
            except socket.error as msg:
                if not self.is_connected():
                    return
                self.logger.error(msg)
                self.close()
                raise SocketReadError()

            self.logger.info('received data')
            msg_buf += bytearray(received)

            # unmarshal QMessages from received buffer
            while True:
                try:
                    qmsg = make_qmessage_from_buffer(msg_buf)
                    yield qmsg

                    read_bytes = QHeader.HEADER_SIZE + qmsg.length()
                    msg_buf = msg_buf[read_bytes:]

                except NotEnoughBufferError:
                    break

    def _init_stream(self, session_type: SessionType, topic: str):
        if not self.is_connected():
            raise SocketClosedError()

        msg = make_qmessage_from_proto(MessageType.STREAM, connect_msg(session_type, topic))
        self.send_message(msg)
        received = self.read_message()

        if received.unpack_to(ConnectResponse()) is not None:
            self.logger.info('stream initialized')
            return
        elif (ack := msg.unpack_to(Ack())) is not None:
            raise RequestFailedError(msg=ack.msg)
        else:
            raise InvalidMessageError()

    def connect(self, session_type: SessionType, topic: str):
        if len(topic) == 0:
            raise TopicNotSetError()

        self.connect_to_broker(self.config.get_broker_address(), self.config.get_broker_port())
        msg = make_qmessage_from_proto(MessageType.TRANSACTION, discover_broker_msg(topic, session_type))

        self.send_message(msg)
        received = self.read_message()

        discover_broker_response = DiscoverBrokerResponse()
        if received.unpack_to(discover_broker_response) is None:
            raise MessageDecodeError(msg="cannot unpack to `DiscoverBrokerResponse`")

        if discover_broker_response.error_code != PQErrCode.Success.value:
            raise RequestFailedError(msg=discover_broker_response.error_message)

        self.close()
        new_addr = discover_broker_response.address.split(':')
        self.connect_to_broker(new_addr[0], int(new_addr[1]))
        self._init_stream(session_type, topic)

    def close(self):
        if self.connected:
            self.connected = False
            self._sock.close()
            self.logger.info('connection closed')

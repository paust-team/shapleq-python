import socket
from proto.data_pb2 import SessionType
from proto.api_pb2 import DiscoverBrokerResponse,  ConnectResponse
from exception import *
from message.qmessage import *
from message.api import discover_broker_msg, connect_msg
from error import PQErrCode
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

    def __init__(self, name: str, config: QConfig):
        self.logger = logging.getLogger(name)
        self.config = config

    def is_connected(self) -> bool:
        return self.connected

    def _connect_to_broker(self, address: str, port: int):
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

    def _send_message(self, msg: QMessage):
        if self._sock.send(msg.serialize()) <= 0:
            raise SocketWriteError()
        self.logger.info('sent data successfully')

    def _read_message(self) -> QMessage:
        while True:
            if not self.is_connected():
                raise SocketClosedError()

            try:
                received = self._sock.recv(4 * 1024)
                if not received:
                    raise SocketReadError()
                self.logger.info('read data successfully')

                return make_qmessage_from_buffer(received)
            except NotEnoughBufferError:
                continue
            except socket.error as msg:
                self.logger.error(msg)
                raise SocketReadError(msg)

    def continuous_receive(self) -> Generator[QMessage, None, None]:
        msg_buf: bytearray

        while True:
            if not self.is_connected():
                raise SocketClosedError()

            try:
                received = self._sock.recv(4 * 1024)
                if not received:
                    raise SocketReadError()
            except socket.error as msg:
                if not self.is_connected():
                    break
                self.logger.error(msg)
                raise SocketReadError(msg)

            self.logger.info('received data')
            msg_buf = bytearray(received)

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
        self._send_message(msg)
        received = self._read_message()

        connect_response = ConnectResponse()
        if received.unpack_to(connect_response) is None:
            raise MessageDecodeError(msg="cannot unpack to `ConnectResponse`")

        self.logger.info('stream initialized')

    def connect(self, session_type: SessionType, topic: str):
        if len(topic) == 0:
            raise TopicNotSetError()

        self._connect_to_broker(self.config.get_broker_address(), self.config.get_broker_port())
        msg = make_qmessage_from_proto(MessageType.TRANSACTION, discover_broker_msg(topic, session_type))

        self._send_message(msg)
        received = self._read_message()

        discover_broker_response = DiscoverBrokerResponse()
        if received.unpack_to(discover_broker_response) is None:
            raise MessageDecodeError(msg="cannot unpack to `DiscoverBrokerResponse`")

        if discover_broker_response.error_code != PQErrCode.Success.value:
            raise RequestFailedError(msg=discover_broker_response.error_message)

        self.close()
        new_addr = discover_broker_response.address.split(':')
        self._connect_to_broker(new_addr[0], int(new_addr[1]))
        self._init_stream(session_type, topic)

    def close(self):
        self.connected = False
        self._sock.close()
        self.logger.info('connection closed')

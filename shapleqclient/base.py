import socket
from shapleqclient.proto.data_pb2 import SessionType, PUBLISHER
from shapleqclient.proto.api_pb2 import ConnectResponse, Ack
from shapleqclient.common.exception import *
from shapleqclient.message.qmessage import *
from shapleqclient.message.api import connect_msg
from shapleqclient.common.error import PQErrCode
from typing import Generator
import logging
import os
from shapleqclient.zk_client import ZKClient, ZKLocalConfig, ZKProductionConfig
import random


class QConfig:
    DEFAULT_TIMEOUT = 3000
    DEFAULT_ZK_QUORUM = "localhost:2181"

    # timeout should be milliseconds value
    def __init__(self, zk_quorum: str = DEFAULT_ZK_QUORUM, timeout: int = DEFAULT_TIMEOUT):
        self.zk_quorum = zk_quorum
        self.timeout = timeout

    def get_zk_quorum(self) -> str:
        return self.zk_quorum

    def get_timeout(self) -> int:
        return self.timeout


class ClientBase:
    connected: bool = False
    config: QConfig
    logger: logging.Logger
    _sock: socket.socket
    _RECEIVE_BUFFER_SIZE = 4 * 1024
    _zk_client: ZKClient

    def __init__(self, config: QConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        zk_config = None
        if 'FLASK_ENV' in os.environ and os.environ['FLASK_ENV'] == 'production':
            zk_config = ZKProductionConfig()
        else:
            zk_config = ZKLocalConfig()
        zk_config.hosts = config.get_zk_quorum()
        self._zk_client = ZKClient(config=zk_config)

    def is_connected(self) -> bool:
        return self.connected

    def connect_to_broker(self, host: str):
        if self.connected:
            raise ClientConnectionError("already connected to broker")

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self.config.get_timeout() / 1000)
        try:
            addr = host.split(":")
            self._sock.connect((addr[0], int(addr[1])))
            self.connected = True
            self.logger.info('connected to broker target {}'.format(host))

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

        self._zk_client.connect()
        topic_brokers = self._zk_client.get_topic_brokers(topic)

        if len(topic_brokers) > 0:
            self.connect_to_broker(topic_brokers[0])
        elif session_type == PUBLISHER:  # if publisher, pick random broker unless topic broker exists
            brokers = self._zk_client.get_brokers()
            print(brokers)
            if len(brokers) == 0:
                raise ClientConnectionError(msg="broker not exists")
            else:
                self.connect_to_broker(brokers[random.randrange(0,len(brokers))])
        else:
            raise ClientConnectionError(msg="broker not exists")

        self._init_stream(session_type, topic)

    def close(self):
        self._zk_client.close()
        if self.connected:
            self.connected = False
            self._sock.close()
            self.logger.info('connection closed')

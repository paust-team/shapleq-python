import logging

from shapleqclient.base import ClientBase, QConfig
from shapleqclient.message.qmessage import MessageType, make_qmessage_from_proto
from shapleqclient.message.api import create_topic_msg, delete_topic_msg, describe_topic_msg, list_topic_msg, ping_msg
from shapleqclient.proto.api_pb2 import CreateTopicResponse, DeleteTopicResponse, ListTopicResponse, DescribeTopicResponse, Pong
from shapleqclient.proto.data_pb2 import Topic
from shapleqclient.common.exception import MessageDecodeError, RequestFailedError
from shapleqclient.common.error import PQErrCode
from typing import List


class Admin:
    _client: ClientBase
    logger: logging.Logger

    def __init__(self, broker_address: str, timeout: int, logger: logging.Logger):
        config = QConfig(timeout=timeout)
        self._broker_address = broker_address
        self._client = ClientBase(config, logger)
        self.logger = logger

    def setup(self):
        self._client.connect_to_broker(self._broker_address)

    def stop(self):
        self._client.close()

    def create_topic(self, topic_name: str, topic_meta: str, num_partitions: int, replication_factor: int):
        msg = make_qmessage_from_proto(MessageType.TRANSACTION,
                                       create_topic_msg(topic_name, topic_meta, num_partitions, replication_factor))
        self._client.send_message(msg)
        received = self._client.read_message()

        response = CreateTopicResponse()
        if received.unpack_to(response) is None:
            raise MessageDecodeError(msg="cannot unpack to `CreateTopicResponse`")

        if response.error_code != PQErrCode.Success.value:
            self.logger.error(response.error_message)
            raise RequestFailedError(msg=response.error_message)

    def delete_topic(self, topic_name: str):
        msg = make_qmessage_from_proto(MessageType.TRANSACTION, delete_topic_msg(topic_name))
        self._client.send_message(msg)
        received = self._client.read_message()

        response = DeleteTopicResponse()
        if received.unpack_to(response) is None:
            raise MessageDecodeError(msg="cannot unpack to `DeleteTopicResponse`")

        if response.error_code != PQErrCode.Success.value:
            self.logger.error(response.error_message)
            raise RequestFailedError(msg=response.error_message)

    def describe_topic(self, topic_name: str) -> Topic:
        msg = make_qmessage_from_proto(MessageType.TRANSACTION, describe_topic_msg(topic_name))
        self._client.send_message(msg)
        received = self._client.read_message()

        response = DescribeTopicResponse()
        if received.unpack_to(response) is None:
            raise MessageDecodeError(msg="cannot unpack to `DescribeTopicResponse`")

        if response.error_code != PQErrCode.Success.value:
            self.logger.error(response.error_message)
            raise RequestFailedError(msg=response.error_message)

        return response.topic

    def list_topic(self) -> List[Topic]:
        msg = make_qmessage_from_proto(MessageType.TRANSACTION, list_topic_msg())
        self._client.send_message(msg)
        received = self._client.read_message()

        response = ListTopicResponse()
        if received.unpack_to(response) is None:
            raise MessageDecodeError(msg="cannot unpack to `ListTopicResponse`")

        if response.error_code != PQErrCode.Success.value:
            self.logger.error(response.error_message)
            raise RequestFailedError(msg=response.error_message)

        return response.topics

    def heartbeat(self, msg: str, broker_id: int) -> Pong:
        msg = make_qmessage_from_proto(MessageType.TRANSACTION, ping_msg(msg, broker_id))
        self._client.send_message(msg)
        received = self._client.read_message()

        response = Pong()
        if received.unpack_to(response) is None:
            raise MessageDecodeError(msg="cannot unpack to `Pong`")

        return response

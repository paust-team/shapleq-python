from typing import Iterable

from proto import api_pb2, data_pb2

MAGIC_NUM: int = 1101


def list_topic_msg() -> api_pb2.ListTopicRequest:
    msg = api_pb2.ListTopicRequest()
    msg.magic = MAGIC_NUM

    return msg


def describe_topic_msg(topic_name: str) -> api_pb2.DescribeTopicRequest:
    msg = api_pb2.DescribeTopicRequest()
    msg.magic = MAGIC_NUM
    msg.topic_name = topic_name

    return msg


def create_topic_msg(topic_name: str, description: str, num_partitions: int, replication_factor: int) -> api_pb2.CreateTopicRequest:
    topic = data_pb2.Topic()
    topic.name = topic_name
    topic.description = description
    topic.num_partitions = num_partitions
    topic.replication_factor = replication_factor

    msg = api_pb2.CreateTopicRequest()
    msg.magic = MAGIC_NUM
    msg.topic.CopyFrom(topic)

    return msg


def delete_topic_msg(topic_name: str) -> api_pb2.DeleteTopicRequest:
    msg = api_pb2.DeleteTopicRequest()
    msg.magic = MAGIC_NUM
    msg.topic_name = topic_name

    return msg


def ping_msg(msg: str, broker_id: int) -> api_pb2.Ping:
    msg = api_pb2.Ping()
    msg.magic = MAGIC_NUM
    msg.echo = msg
    msg.broker_id = broker_id

    return msg


def connect_msg(session_type: data_pb2.SessionType, topic_name: str) -> api_pb2.ConnectRequest:
    msg = api_pb2.ConnectRequest()
    msg.magic = MAGIC_NUM
    msg.session_type = session_type
    msg.topic_name = topic_name

    return msg


def put_msg(data: bytes) -> api_pb2.PutRequest:
    msg = api_pb2.PutRequest()
    msg.magic = MAGIC_NUM
    msg.data = data

    return msg


def fetch_msg(start_offset: int) -> api_pb2.FetchRequest:
    msg = api_pb2.FetchRequest()
    msg.magic = MAGIC_NUM
    msg.start_offset = start_offset

    return msg


def ack_msg(code: int, text: str) -> api_pb2.Ack:
    msg = api_pb2.Ack()
    msg.magic = MAGIC_NUM
    msg.code = code
    msg.msg = text

    return msg


def discover_broker_msg(topic_name: str, session_type: data_pb2.SessionType) -> api_pb2.DiscoverBrokerRequest:
    msg = api_pb2.DiscoverBrokerRequest()
    msg.magic = MAGIC_NUM
    msg.topic_name = topic_name
    msg.session_type = session_type

    return msg


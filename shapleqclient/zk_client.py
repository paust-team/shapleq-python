import dataclasses
from kazoo.client import KazooClient

from shapleqclient.common.exception import PathNotExists


@dataclasses.dataclass
class ZKConfig(object):
    timeout: int = 3000
    quorum: str = "127.0.0.1:2181"
    base_path: str = ""

    def get_brokers_path(self):
        return self.base_path+"/brokers"

    def get_topic_path(self, topic: str):
        return self.base_path+"/topics/"+topic

    def get_topic_broker_path(self, topic: str):
        return self.base_path+"/topics/"+topic+"/brokers"


@dataclasses.dataclass
class ZKLocalConfig(ZKConfig):
    def __init__(self):
        self.base_path = "/shapleq-debug"


@dataclasses.dataclass
class ZKProductionConfig(ZKConfig):
    def __init__(self):
        self.base_path = "/shapleq"


class ZKClient(object):
    config: ZKConfig
    _client: KazooClient

    def __init__(self, config: ZKConfig):
        self.config = config
        self._client = KazooClient(hosts=self.config.quorum)

    def connect(self):
        self._client.start(timeout=self.config.timeout)

    def is_connected(self):
        return self._client.connected

    def close(self):
        self._client.stop()
        self._client.close()

    def get_topic_brokers(self, topic: str) -> [str]:
        path = self.config.get_topic_broker_path(topic)
        if not self._client.ensure_path(path):
            raise PathNotExists(path)
        result = self._client.get(path)

        if len(result) > 0 and len(result[0]) > 0:
            return result[0].decode().split(",")
        else:
            return []

    def get_brokers(self) -> [str]:
        path = self.config.get_brokers_path()
        if not self._client.ensure_path(path):
            raise PathNotExists(path)
        result = self._client.get(path)

        if len(result) > 0 and len(result[0]) > 0:
            return result[0].decode().split(",")
        else:
            return []
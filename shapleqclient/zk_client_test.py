import unittest

from shapleqclient.zk_client import ZKClient, ZKLocalConfig


class ZKClientTest(unittest.TestCase):

    def test_get_topic_brokers(self):
        config = ZKLocalConfig()
        client = ZKClient(config=config)
        client.connect()

        self.assertTrue(client.is_connected())
        client.close()

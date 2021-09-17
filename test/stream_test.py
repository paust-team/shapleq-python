import logging
import threading
import time
import unittest

from kazoo.client import KazooClient

from shapleqclient.admin import Admin
from shapleqclient.base import QConfig
from shapleqclient.consumer import Consumer
from shapleqclient.producer import Producer


class StreamTest(unittest.TestCase):
    zk_host = "127.0.0.1:2181"
    broker_port = 1101
    broker_address = "127.0.0.1"
    timeout = 3000
    config: QConfig

    logger = logging.getLogger()
    logger.level = logging.DEBUG

    @classmethod
    def setUpClass(cls) -> None:
        cls.config = QConfig(cls.broker_address, cls.broker_port, cls.timeout)

    @classmethod
    def tearDownClass(cls) -> None:
        zk = KazooClient(hosts=cls.zk_host)
        zk.start()

        zk.delete("/shapleq-debug", recursive=True)
        zk.stop()
        zk.close()

    def create_topic(self, topic: str):
        admin = Admin(self.config)
        admin.setup()
        admin.create_topic(topic, "meta", 1, 1)
        admin.close()

    def test_connect(self):
        topic = "test_topic_1"

        self.create_topic(topic)

        producer = Producer(self.config, topic)
        producer.setup()

        consumer = Consumer(self.config, topic)
        consumer.setup()

        self.assertTrue(producer.is_connected())
        self.assertTrue(consumer.is_connected())

        consumer.close()
        producer.close()

    def test_pupsub(self):
        topic = "test_topic_2"
        expected_records = [b'google', b'paust', b'123456']
        actual_records = []

        self.create_topic(topic)

        producer = Producer(self.config, topic)
        producer.setup()

        consumer = Consumer(self.config, topic)
        consumer.setup()

        def publish():
            time.sleep(1)
            for record in expected_records:
                producer.publish(record)

        producer_thread = threading.Thread(target=publish)
        producer_thread.start()

        for fetched_data in consumer.subscribe(0):
            for data in fetched_data.data:
                actual_records.append(data)
            if len(actual_records) == len(expected_records):
                break

        producer_thread.join()

        for index, data in enumerate(actual_records):
            self.assertEqual(data, expected_records[index])

        producer.close()
        consumer.close()

    def test_batch_fetch(self):
        topic = "test_topic_3"
        expected_records = [b'google', b'paust', b'123456',
                            b'google2', b'paust2', b'1234562',
                            b'google3', b'paust3', b'1234563',
                            b'google4', b'paust4', b'1234564']
        actual_records = []

        self.create_topic(topic)

        producer = Producer(self.config, topic)
        producer.setup()

        consumer = Consumer(self.config, topic)
        consumer.setup()

        def publish():
            time.sleep(1)
            for record in expected_records:
                producer.publish(record)

        producer_thread = threading.Thread(target=publish)
        producer_thread.start()

        for fetched_data in consumer.subscribe(0, max_batch_size=3, flush_interval=200):
            for data in fetched_data.data:
                actual_records.append(data)
            if len(actual_records) == len(expected_records):
                break

        producer_thread.join()

        for index, data in enumerate(actual_records):
            self.assertEqual(data, expected_records[index])

        producer.close()
        consumer.close()

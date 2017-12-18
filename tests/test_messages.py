# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest
import time
from multiprocessing import Process
from bushitaka.messages import Broker

TOPIC = "topic"
CHANNEL_1 = "channel_1"
CHANNEL_2 = "channel_2"

@pytest.fixture(scope="function")
def broker():
    _broker = Broker()
    return _broker

class Service(Process):
    def __init__(self, broker):
        super(Service, self).__init__()
        self.broker = broker

class Publisher(Service):
    def __init__(self, broker, topic=TOPIC, channels=None, loop=50):
        super(Publisher, self).__init__(broker)
        self.channels = channels
        self.loop = loop
        self.topic = topic

    def run(self):
        for i in range(self.loop):
            self.broker.publish(self.topic, i, channels=self.channels)
        self.broker.publish(self.topic, None, channels=self.channels)

class Subscriber(Service):
    def __init__(self, broker, topic=TOPIC, channel=None):
        super(Subscriber, self).__init__(broker)
        self.channel = channel
        self.topic = topic

    def run(self):
        messages = self.broker.subscribe(self.topic, channel=self.channel)
        assert messages is not None
        assert False # TODO: raise exception for tests

class Consumer(Service):
    def __init__(self, broker, topic=TOPIC, channel=None):
        super(Consumer, self).__init__(broker)
        self.channel = channel
        self.topic = topic

    def run(self):
        messages = self.broker.subscribe(self.topic, channel=self.channel)
        while True:
            message = messages.get()
            if message is None:
                break

def test_publish(broker):
    publisher = Publisher(broker)
    publisher.start()
    publisher.join()

def test_publish_channel(broker):
    publisher = Publisher(broker, channels=[CHANNEL_1])
    publisher.start()
    publisher.join()

def test_publish_channesl(broker):
    publisher = Publisher(broker, channels=[CHANNEL_1, CHANNEL_2])
    publisher.start()
    publisher.join()

def test_subscribe(broker):
    subscriber = Subscriber(broker)
    subscriber.start()
    subscriber.join()

def test_subscribe_channel(broker):
    subscriber = Subscriber(broker, channel=CHANNEL_1)
    subscriber.start()
    subscriber.join()

def test_1to1(broker):
    publisher = Publisher(broker)
    consumer = Consumer(broker)
    consumer.start()
    time.sleep(1)

    publisher.start()

    publisher.join()
    consumer.join()

def test_1to1_channel(broker):
    publisher = Publisher(broker, channels=[CHANNEL_1])
    consumer = Consumer(broker, channel=CHANNEL_1)
    consumer.start()
    time.sleep(1)

    publisher.start()

    publisher.join()
    consumer.join()

def test_1toMany(broker):
    publisher = Publisher(broker)
    consumers = (Consumer(broker) for _ in range(3))

    for consumer in consumers:
        consumer.start()

    time.sleep(1)

    publisher.start()

    publisher.join()

    for consumer in consumers:
        consumer.join()


def test_1toMany_channel():
    pass

def test_1toMany_channels():
    pass

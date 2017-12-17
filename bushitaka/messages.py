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

'''
Broker for pubsub communication
'''

import logging
from multiprocessing.managers import BaseManager
import Queue
import uuid

from bushitaka.locks import ReadWriteLock

def default_channel_strategy(queues):
    '''Load balancing strategy over a channel

    Args:
        queues: The set of subscriber queues for a channel
    '''
    for queue in queues:
        if queue.full():
            continue
        return queue
    return None


class _Broker(object):
    '''Remote part of the broker to put in a manager.
    '''
    def __init__(self, channel_strategy=default_channel_strategy):
        self.topics = dict()
        self.channel_strategy = channel_strategy
        self.lock = ReadWriteLock()
        self.logger = logging.getLogger("MessageBroker")

    def publish(self, topic, message, channels=None):
        '''Publish a new message in the topic

        Args:
            topic: The topic name.
            message: A serializable (pickale) python object.
            channels: An optional list of channels name, if None,
                     publish on all channels

        Returns:
            None
        '''
        if not topic in self.topics:
            return

        self.lock.acquire_read()
        try:
            topic = self.topics[topic]

            queues = {channel: self.channel_strategy(qs)
                      for channel, qs in topic.iteritems()
                      if channels is None or channel in channels}
        finally:
            self.lock.release_read()

        for channel, queue in queues.iteritems():
            if queue is None:
                self.logger.warning("""Can not deliver message for on
                topic <%s> for channel <%s>""", topic, channel)
            else:
                queue.put_nowait(message)

    def subscribe(self, topic, queue, channel=None):
        '''Subscribe to a topic

        Args:
            topic: The topic name
            queue: The subscriber queue
            channel: An optional channel name, if None
                     a fresh one is created

        Return:
            None
        '''
        if channel is None:
            channel = str(uuid.uuid4())

        self.lock.acquire_write()
        try:
            topic = self.topics.setdefault(topic, dict())
            channel = topic.setdefault(channel, list())
            channel.append(queue)
        finally:
            self.lock.release_write()

class _BrokerManager(BaseManager):
    pass
_BrokerManager.register('Broker', _Broker)
_BrokerManager.register('Queue', Queue.Queue)

class Broker(object):
    '''A the frontend broker

    Args:
        channel_strategy: A function to pick the right
                          queue in a channel
    '''
    def __init__(self, channel_strategy=default_channel_strategy):
        self.manager = _BrokerManager()
        self.manager.start()
        self._broker = self.manager.Broker(channel_strategy)

    def publish(self, topic, message, channels=None):
        '''Publish a new message in the topic

        Args:
            topic: The topic name.
            message: A serializable (pickale) python object.
            channel: An optional channel name

        Returns:
            None
        '''
        self._broker.publish(topic, message, channels=channels)

    def subscribe(self, topic, channel=None):
        '''Subscribe to a topic

        Args:
            topic: The topic name
            queue: The subscriber queue
            channel: An optional channel name

        Return:
            A queue to get messages
        '''
        queue = self.manager.Queue()
        self._broker.subscribe(topic, queue, channel=channel)
        return queue

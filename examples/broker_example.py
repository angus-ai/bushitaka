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
An example for broker: one publisher, many subscriber (default channel)
'''

from multiprocessing import Process
from bushitaka.messages import Broker

class Publisher(Process):
    '''A very simple publisher that publish count (from 0 to 500)
    Args:
        broker: the broker to use
    '''
    def __init__(self, broker):
        super(Publisher, self).__init__()
        self.broker = broker

    def run(self):
        for i in range(500):
            self.broker.publish("count", i)
        print("Publisher ends")
        self.broker.publish("count", None)

class Consumer(Process):
    '''A very simple consumer that ends with a None message
    '''
    def __init__(self, name, broker):
        super(Consumer, self).__init__()
        self.broker = broker
        self.name = name

    def run(self):
        messages = self.broker.subscribe("count")
        while True:
            message = messages.get()
            if message is None:
                print("Consumer {} ends".format(self.name))
                break

N_CONSUMERS = 10

def main():
    broker = Broker()
    publisher = Publisher(broker)
    consumers = [Consumer("consumer-{}".format(n), broker) for n in range(N_CONSUMERS)]

    publisher.start()
    for consumer in consumers:
        consumer.start()

    publisher.join()
    for consumer in consumers:
        consumer.join()

if __name__ == '__main__':
    main()

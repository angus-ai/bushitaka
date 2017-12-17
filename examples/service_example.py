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
An example for service
'''
import time

from bushitaka.services import Service, rpc

class Service1(Service):

    def __init__(self):
        super(Service1, self).__init__()
        self.message = "Do something"

    @rpc
    def change(self, message):
        self.message = message

    def main(self):
        while True:
            if self.message is None:
                break
            print(self.message)
            time.sleep(1)

def main():
    service = Service1()
    service.start()

    for i in range(10):
        service.change("Message changed with {}".format(i))
        time.sleep(2)
    service.change(None)

if __name__=='__main__':
    main()

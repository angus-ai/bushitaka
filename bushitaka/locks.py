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
More advance locks based on multiprocessing
https://www.safaribooksonline.com/library/view/python-cookbook/0596001673/ch06s04.html
Author: Sami Hangaslammi
'''

from threading import Lock, Condition

class ReadWriteLock(object):
    ''' A lock object that allows many simultaneous "read locks", but
    only one "write lock." '''

    def __init__(self):
        self._read_ready = Condition(Lock())
        self._readers = 0

    def acquire_read(self):
        ''' Acquire a read lock. Blocks only if a thread has
        acquired the write lock. '''
        self._read_ready.acquire()
        try:
            self._readers += 1
        finally:
            self._read_ready.release()

    def release_read(self):
        ''' Release a read lock. '''
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notifyAll()
        finally:
            self._read_ready.release()

    def acquire_write(self):
        ''' Acquire a write lock. Blocks until there are no
        acquired read or write locks. '''
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def release_write(self):
        ''' Release a write lock. '''
        self._read_ready.release()

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
A framework for implement a service oriented architecture.
'''

from multiprocessing import Process, Pipe, Lock
from threading import Thread

def rpc(func):
    ''' A decorator to expose method through rpc
    '''
    def func_wrapper(self, *args, **kwargs):
        ''' Call proxy if it is the parent or the method
        if it is the child
        '''
        # pylint: disable=protected-access
        if self._child:
            return func(self, *args, **kwargs)

        # pylint: disable=protected-access
        with self._rpc_lock:
            self._rpc_parent.send((func.__name__, args, kwargs))
            result = self._rpc_parent.recv()
        return result

    # pylint: disable=protected-access
    func_wrapper._rpc = True
    return func_wrapper

class Service(Process):
    ''' A service is a standalone process
    it offers a an rpc interface for its parent'''

    def __init__(self):
        super(Service, self).__init__()
        parent, child = Pipe(duplex=True)
        self._rpc_parent = parent
        self._rpc_child = child
        self._rpc_thread = None
        self._rpc_lock = Lock()
        self._child = False

    def _call_rpc(self, methods):
        ''' Thread function to execute remote call
        '''
        while True:
            func_name, args, kwargs = self._rpc_child.recv()
            if func_name in methods:
                # pylint: disable=protected-access
                result = methods[func_name](*args, **kwargs)
                self._rpc_child.send(result)

    def _install_rpc(self):
        ''' Find methods to expose through rpc.
        '''
        methods = {
            method_name: getattr(self, method_name)
            for method_name in dir(self)
        }
        methods = {
            method_name: method  for method_name, method in methods.iteritems()
            if hasattr(method, '_rpc')
        }

        self._rpc_thread = Thread(target=self._call_rpc, args=(methods,))
        self._rpc_thread.daemon = True
        self._rpc_thread.start()

    def _initialize(self):
        ''' Method before main function
        '''
        self._child = True
        self._install_rpc()

    def _finalize(self):
        ''' Method after main function
        '''
        pass

    def main(self):
        ''' The body of the service
        '''
        raise NotImplementedError("You must override main method")

    def run(self):
        self._initialize()
        self.main()
        self._finalize()

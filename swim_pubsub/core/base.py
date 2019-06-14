"""
Copyright 2019 EUROCONTROL
==========================================

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the 
following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following 
   disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following 
   disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products 
   derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, 
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE 
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE 
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

==========================================

Editorial note: this license is an instance of the BSD license template as provided by the Open Source Initiative: 
http://opensource.org/licenses/BSD-3-Clause

Details on EUROCONTROL: http://www.eurocontrol.int
"""
import threading

from proton._reactor import Container

from swim_pubsub.clients.clients import Publisher, Subscriber, Client
from swim_pubsub.core.errors import AppError

__author__ = "EUROCONTROL (SWIM)"


class _ProtonContainer:

    def __init__(self, handler=None):
        self._handler = handler
        self._thread = None

    def is_running(self):
        if self._thread:
            return self._handler.started and self._thread.is_alive()
        else:
            return self._handler.started

    def run(self, threaded=False):
        if threaded and not self.is_running():
            self._thread = threading.Thread(target=self._run)
            self._thread.daemon = True

            return self._thread.start()

        return self._run()

    def _run(self):
        self._container = Container(self._handler)
        self._container.run()


class App(_ProtonContainer):

    def __init__(self, handler):
        super().__init__(handler=handler)

        self._handler = handler
        self._pre_run_actions = []
        self.config = None
        self.clients = []

    def before_run(self, f):
        self._pre_run_actions.append(f)

    def run(self, threaded=False):
        for action in self._pre_run_actions:
            action()

        super().run(threaded=threaded)

    def register_client(self, username, password, client_class=Client):
        if client_class != Client:
            if Client not in client_class.__bases__:
                raise AppError(f"client_class should be Client or should inherit from Client")

        client = client_class.create(self._handler, self.config['SUBSCRIPTION-MANAGER'], username, password)

        self.clients.append(client)

        return client

    def register_publisher(self, username, password):
        return self.register_client(username, password, client_class=Publisher)

    def register_subscriber(self, username, password):
        return self.register_client(username, password, client_class=Subscriber)

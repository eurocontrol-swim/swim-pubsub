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
from typing import Optional, List, Callable, Type

from proton._reactor import Container

from swim_pubsub.clients.clients import Publisher, Subscriber, Client
from swim_pubsub.core import ConfigDict
from swim_pubsub.core.errors import AppError
from swim_pubsub.core.handlers import BrokerHandler

__author__ = "EUROCONTROL (SWIM)"


class _ProtonContainer:

    def __init__(self, handler: Optional[BrokerHandler] = None) -> None:
        """
        A `proton.Container` extension allowing threaded running.
        :param handler:
        """
        self._handler: BrokerHandler = handler
        self._thread: Optional[threading.Thread] = None

    def is_running(self):
        """
        Determines whether the container is running by checking the handler and the underlying thread if it's running in
        threaded mode.
        :return:
        """
        if self._thread:
            return self._handler.started and self._thread.is_alive()
        else:
            return self._handler.started

    def run(self, threaded: bool = False):
        """
        Runs the container in threaded or not mode
        :param threaded:
        :return:
        """
        if threaded and not self.is_running():
            self._thread = threading.Thread(target=self._run)
            self._thread.daemon = True

            return self._thread.start()

        return self._run()

    def _run(self):
        """
        The actual runner
        """
        self._container = Container(self._handler)
        self._container.run()


class App(_ProtonContainer):

    def __init__(self, handler: BrokerHandler):
        """
        A `_ProtonContainer` extension which acts like an app by keeping track of:
         - the running of the contaner.
         - the clients (publishers, subscribers) using it.
         - any actions to be run before the actual run of the container.
        :param handler:
        """
        super().__init__(handler=handler)

        self._handler: BrokerHandler = handler
        self._before_run_actions: List[Callable] = []
        self.config: Optional[ConfigDict] = None
        self.clients: List[Client] = []

    def before_run(self, f: Callable):
        """
        Decorator to be used on any action that needs to be run before starting the application. The actions will be run
        in FIFO mode.

        Usage:
        >>> handler = BrokerHandler()
        >>> app = App(handler)
        >>> @app.pre_run
        >>> def action():
        >>>     print("Before run")
        >>> app.run()
        """
        self._before_run_actions.append(f)

    def run(self, threaded: bool = False):
        """
        Overrides the container run by running first any registered as 'before_run' action.
        :param threaded:
        """
        for action in self._before_run_actions:
            action()

        super().run(threaded=threaded)

    def register_client(self, username: str, password: str, client_class: Type[Client] = Client):
        """
        Creates a new client (publisher, subscriber) that will be using this app.

        :param username:
        :param password:
        :param client_class:
        :return:
        """
        if client_class != Client:
            if Client not in client_class.__bases__:
                raise AppError(f"client_class should be Client or should inherit from Client")

        client = client_class.create(self._handler, self.config['SUBSCRIPTION-MANAGER'], username, password)

        self.clients.append(client)

        return client

    def register_publisher(self, username: str, password: str) -> Publisher:
        """
        Creates a Publisher client
        """
        return self.register_client(username, password, client_class=Publisher)

    def register_subscriber(self, username: str, password: str) -> Subscriber:
        """
        Creates a Subscriber client
        """
        return self.register_client(username, password, client_class=Subscriber)

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

from swim_pubsub.users.users import Publisher, Subscriber

__author__ = "EUROCONTROL (SWIM)"


class Proton:

    def __init__(self, messaging_handler, auto_run=False):
        self.messaging_handler = messaging_handler

        self._thread = None

        if auto_run:
            self.run()

    def is_running(self):
        if self._thread:
            return self.messaging_handler.started and self._thread.is_alive()
        else:
            return self.messaging_handler.started

    def run(self, threaded=False):
        if threaded and not self.is_running():
            self._thread = threading.Thread(target=self._run)
            self._thread.daemon = True

            return self._thread.start()

        return self._run()

    def _run(self):
        self._container = Container(self.messaging_handler)
        self._container.run()


class App(Proton):

    def __init__(self, msg_handler, sm_config):
        Proton.__init__(self, messaging_handler=msg_handler)

        self.sm_config = sm_config
        self.users = []

    @property
    def publishers(self):
        return [user for user in self.users if user.is_publisher]

    @property
    def subscribers(self):
        return [user for user in self.users if user.is_subscriber]


class PublisherApp(App):

    def create_publisher(self, username, password):
        publisher = Publisher.create(self.messaging_handler, self.sm_config, username, password)
        self.users.append(publisher)

        return publisher

    def _populate_publisher_topics(self):
        for publisher in self.publishers:
            publisher.populate_topics()

    def run(self, threaded=False):
        self._populate_publisher_topics()

        super().run(threaded=threaded)


class SubscriberApp(App):

    def create_subscriber(self, username, password):
        subscriber = Subscriber.create(self.messaging_handler, self.sm_config, username, password)
        self.users.append(subscriber)

        return subscriber

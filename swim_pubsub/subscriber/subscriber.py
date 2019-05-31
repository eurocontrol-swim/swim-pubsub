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

from swim_pubsub.base import PubSubApp
from swim_pubsub.subscriber.utils import sms_error_handler
from swim_pubsub.subscriber.handler import SubscriberHandler

__author__ = "EUROCONTROL (SWIM)"


class SubscriberApp(PubSubApp):

    def __init__(self, config_file):
        PubSubApp.__init__(self, config_file)
        self.subscriptions = {}

        self._handler = SubscriberHandler(
            host=self.config['BROKER']['host'],
            ssl_domain=self.ssl_domain
        )

        self._thread = threading.Thread(target=self.run)
        self._thread.daemon = True
        self._thread.start()

    def is_running(self):
        return self._thread.is_alive() and self._handler.has_started()

    def run(self):
        self._container = Container(self._handler)
        self._container.run()

    @sms_error_handler
    def get_topics(self):
        return self.sm_facade.get_topics()

    @sms_error_handler
    def subscribe(self, topic_name, data_handler):
        queue = self.sm_facade.subscribe(topic_name)
        print("Subscribed in SM")

        self._handler.add_receiver(queue, data_handler)
        print('Created receiver')

        self.subscriptions[topic_name] = queue

    @sms_error_handler
    def unsubscribe(self, topic_name):
        queue = self.subscriptions[topic_name]

        self._handler.remove_receiver(queue)
        print('Removed receiver')

        self.sm_facade.unsubscribe(queue)
        print("Deleted subscription from SM")

    @sms_error_handler
    def pause(self, topic_name):
        queue = self.subscriptions[topic_name]

        self.sm_facade.pause(queue)
        print("Paused subscription in SM")

    @sms_error_handler
    def resume(self, topic_name):
        queue = self.subscriptions[topic_name]

        self.sm_facade.resume(queue)
        print("Resumed subscription in SM")

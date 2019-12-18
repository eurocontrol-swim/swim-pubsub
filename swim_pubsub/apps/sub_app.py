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

__author__ = "EUROCONTROL (SWIM)"

from typing import Dict, Callable

from swim_pubsub.apps.base import App
from swim_pubsub.broker_handlers.subscriber import SubscriberBrokerHandler


class SubApp(App):

    def __init__(self, broker_handler: SubscriberBrokerHandler):
        App.__init__(self, broker_handler)

        self.broker_handler: SubscriberBrokerHandler = broker_handler  # for type hint

        self.queues: Dict[str, str] = {}

    def attach_queue(self, queue: str, data_consumer: Callable) -> None:
        """

        :param queue:
        :param data_consumer: consumes the messages coming from the broker. Signature:
                              data_consumer(message: proton.Message) -> Any
                              raises: DataConsumerError
        """
        self.broker_handler.create_receiver(queue, data_consumer)

    def detach_queue(self, queue: str) -> None:
        """

        :param queue:
        """
        self.broker_handler.remove_receiver(queue)

    @classmethod
    def create_from_config(cls, config_file: str):
        return cls._create_from_config(config_file, broker_handler_class=SubscriberBrokerHandler)

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
import logging
from typing import Dict, Tuple, Callable

import proton

from swim_pubsub.broker_handlers.base import BrokerHandler
from swim_pubsub.broker_handlers.connectors import Connector
from swim_pubsub.errors import AppError, BrokerHandlerError, DataConsumerError

__author__ = "EUROCONTROL (SWIM)"


_logger = logging.getLogger(__name__)


class SubscriberBrokerHandler(BrokerHandler):

    def __init__(self, connector: Connector) -> None:
        """
        An implementation of a broker client that is supposed to act as subscriber. It subscribes to queues of the
        broker by creating instances of `proton.Receiver` for each one of them.

        :param connector: takes care of the connection .i.e TSL, SASL etc
        """
        BrokerHandler.__init__(self, connector)

        # keep track of all the queues by receiver
        self.receivers: Dict[proton.Receiver, Tuple[str, Callable]] = {}

    def _get_receiver_by_queue(self, queue: str) -> proton.Receiver:
        """
        Find the receiver that corresponds to the given queue.
        :param queue:
        :return:
        """
        for receiver, (receiver_queue, _) in self.receivers.items():
            if queue == receiver_queue:
                return receiver

    def create_receiver(self, queue: str, data_consumer: Callable) -> proton.Receiver:
        """
        Create a new `proton.Receiver` and assign the queue and the callback to it

        :param queue: the queue name
        :param data_consumer: consumes the messages coming from the broker. Signature:
                              data_consumer(message: proton.Message) -> Any
                              raises: DataConsumerError
        """
        receiver = self._create_receiver(queue)

        self.receivers[receiver] = (queue, data_consumer)

        _logger.debug(f"Created receiver {receiver} on {queue}")

        return receiver

    def remove_receiver(self, queue: str) -> None:
        """
        Remove the receiver that corresponds to the given queue.

        :param queue: the queue name
        """
        receiver = self._get_receiver_by_queue(queue)

        if not receiver:
            raise ValueError(f'No receiver found for queue: {queue}')

        # close the receiver
        receiver.close()
        _logger.debug(f"Closed receiver {receiver} on queue {queue}")

        # remove it from the list
        del self.receivers[receiver]

    def on_message(self, event: proton.Event) -> None:
        """
        Is triggered upon reception of data via the receiver.

        :param event:
        """
        queue, data_consumer = self.receivers[event.receiver]

        try:
            data_consumer(event.message)
        except DataConsumerError as e:
            _logger.error(f"Error while processing message {event.message} from queue {queue}: {str(e)}")

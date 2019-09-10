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
import uuid
from unittest import mock

import pytest

from swim_pubsub.core.errors import BrokerHandlerError, AppError
from swim_pubsub.subscriber import SubscriberBrokerHandler

__author__ = "EUROCONTROL (SWIM)"


def test__create_receiver(caplog):
    caplog.set_level(logging.DEBUG)

    receiver = mock.Mock()
    queue = uuid.uuid4().hex
    callback = mock.Mock()

    handler = SubscriberBrokerHandler(mock.Mock())
    handler._create_receiver = mock.Mock(return_value=receiver)

    handler.create_receiver(queue, callback)

    assert handler.receivers[receiver] == (queue, callback)

    log_messages = caplog.records
    assert f'Created receiver {receiver}' == log_messages[0].message
    assert f'Start receiving on {queue}' == log_messages[1].message


def test_remove_receiver__receiver_not_found__raises_BrokerHandlerError(caplog):
    caplog.set_level(logging.DEBUG)

    queue = uuid.uuid4().hex

    handler = SubscriberBrokerHandler(mock.Mock())
    handler._get_receiver_by_queue = mock.Mock(return_value=None)

    with pytest.raises(BrokerHandlerError) as e:
        handler.remove_receiver(queue)
        assert f'no receiver found for queue: {queue}' == str(e)


def test_remove_receiver__no_errors(caplog):
    caplog.set_level(logging.DEBUG)

    receiver = mock.Mock()
    receiver.close = mock.Mock()
    queue = uuid.uuid4().hex
    callback = mock.Mock()

    handler = SubscriberBrokerHandler(mock.Mock())
    handler._get_receiver_by_queue = mock.Mock(return_value=receiver)

    handler.receivers[receiver] = (queue, callback)

    handler.remove_receiver(queue)
    receiver.close.assert_called_once()

    log_message = caplog.records[0]
    assert f'Closed receiver {receiver} on queue {queue}' == log_message.message


def test_on_message__callback_raises_AppError__error_is_logged(caplog):
    caplog.set_level(logging.DEBUG)

    receiver = mock.Mock()
    receiver.close = mock.Mock()
    queue = uuid.uuid4().hex
    callback = mock.Mock(side_effect=AppError('error'))
    event = mock.Mock()
    event.receiver = receiver
    event.message = 'message'

    handler = SubscriberBrokerHandler(mock.Mock())
    handler.receivers[receiver] = (queue, callback)

    handler.on_message(event)

    callback.assert_called_once_with(event.message)
    log_message = caplog.records[0]
    assert f"Error while processing message {event.message} from queue {queue}: error" == log_message.message

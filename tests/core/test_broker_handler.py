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
from unittest import mock

import pytest

from swim_pubsub.core.broker_handlers import BrokerHandler
from swim_pubsub.core.errors import BrokerHandlerError

__author__ = "EUROCONTROL (SWIM)"


@pytest.mark.parametrize('host, ssl_domain, expected_host', [
    ('hostname', mock.Mock(), 'amqps://hostname'),
    ('hostname', None, 'amqp://hostname')
])
def test__broker_handler__host_string(host, ssl_domain, expected_host):
    broker_handler = BrokerHandler(host, ssl_domain)

    assert expected_host == broker_handler.host


def test_broker_handler__create_from_config():
    broker_config = {
        'cert_db': 'cert_db',
        'cert_file': 'cert_file',
        'cert_key': 'cert_key',
        'cert_password': 'cert_password',
        'tls_enabled': True,
        'host': 'hostname'
    }

    with mock.patch('swim_pubsub.core.utils.get_ssl_domain', return_value=mock.Mock()):
        broker_handler = BrokerHandler.create_from_config(broker_config)

        assert isinstance(broker_handler, BrokerHandler)


def test_broker_handler__create_receiver_fails__raises_BrokerHAndlerError():
    broker_handler = BrokerHandler('host')
    broker_handler.container = mock.Mock()
    broker_handler.container.create_receiver = mock.Mock(side_effect=Exception('proton error'))

    with pytest.raises(BrokerHandlerError) as e:
        broker_handler._create_receiver('endpoint')
        assert f"proton error" == str(e)


def test_broker_handler__create_sender_fails__raises_BrokerHAndlerError():
    broker_handler = BrokerHandler('host')
    broker_handler.container = mock.Mock()
    broker_handler.container.create_sender = mock.Mock(side_effect=Exception('proton error'))

    with pytest.raises(BrokerHandlerError) as e:
        broker_handler._create_sender('endpoint')
        assert f"proton error" == str(e)
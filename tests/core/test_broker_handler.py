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
from unittest import mock
from unittest.mock import Mock

import pytest

from swim_pubsub.core.broker_handlers_ import BrokerHandler, Connector, TLSConnector, SASLConnector
from swim_pubsub.errors import BrokerHandlerError

__author__ = "EUROCONTROL (SWIM)"


def test__connector__host_is_none__raises_valueerror():
    with pytest.raises(ValueError) as e:
        Connector(host=None)
    assert 'no host was provided' == str(e.value)


def test__connector__url():
    hostname = 'hostname'
    expected_url = 'amqp://hostname'

    with mock.patch('swim_pubsub.core.utils.create_ssl_domain', return_value=mock.Mock()):
        connector = Connector(hostname)

        assert expected_url == connector.url


def test__tlsconnector__url():
    hostname = 'hostname'
    expected_url = 'amqps://hostname'

    with mock.patch('swim_pubsub.core.utils.create_ssl_domain', return_value=mock.Mock()):
        connector = TLSConnector(hostname, 'cert_db', 'cert_file', 'cert_key', 'cert_password')

        assert expected_url == connector.url


def test__saslconnector__url():
    hostname = 'hostname'
    expected_url = 'amqps://hostname'

    with mock.patch('swim_pubsub.core.utils.create_ssl_domain', return_value=mock.Mock()):
        connector = SASLConnector(hostname, 'cert_db', 'cert_file', 'cert_key', 'cert_password')

        assert expected_url == connector.url


def test_connector__connect():
    connector = Connector('host')

    container = mock.Mock()
    container_connect = mock.Mock()
    container.connect = container_connect

    connector.connect(container)

    container_connect.assert_called_once_with(connector.url)


@mock.patch('swim_pubsub.core.utils.create_ssl_domain', return_value=mock.Mock())
def test_tlsconnector__connect(mock_create_ssl_domain):
    connector = TLSConnector('hostname', 'cert_db', 'cert_file', 'cert_key', 'cert_password')

    container = mock.Mock()
    container_connect = mock.Mock()
    container.connect = container_connect

    connector.connect(container)

    container_connect.assert_called_once_with(connector.url, ssl_domain=mock_create_ssl_domain())


@mock.patch('swim_pubsub.core.utils.create_ssl_domain', return_value=mock.Mock())
def test_saslconnector__connect(mock_create_ssl_domain):
    connector = SASLConnector('hostname', 'cert_db', 'sasl_user', 'sasl_password', 'PLAIN')

    container = mock.Mock()
    container_connect = mock.Mock()
    container.connect = container_connect

    connector.connect(container)

    container_connect.assert_called_once_with(connector.url,
                                              ssl_domain=mock_create_ssl_domain(),
                                              sasl_enabled=True,
                                              allowed_mechs='PLAIN',
                                              user='sasl_user',
                                              password='sasl_password')


@mock.patch('swim_pubsub.core.utils.create_ssl_domain', return_value=mock.Mock())
def test_broker_handler__create_from_config__creates_handler_with_simple_connector(mock_create_ssl_domain):
    broker_config = {
        'host': 'hostname'
    }

    broker_handler = BrokerHandler.create_from_config(broker_config)

    assert isinstance(broker_handler, BrokerHandler)
    assert isinstance(broker_handler.connector, Connector)


def test_broker_handler__create_from_config__creates_handler_with_tls_connector():
    broker_config = {
        'cert_db': 'cert_db',
        'cert_file': 'cert_file',
        'cert_key': 'cert_key',
        'cert_password': 'cert_password',
        'host': 'hostname'
    }

    with mock.patch('swim_pubsub.core.utils.create_ssl_domain', return_value=mock.Mock()):
        broker_handler = BrokerHandler.create_from_config(broker_config)

        assert isinstance(broker_handler, BrokerHandler)
        assert isinstance(broker_handler.connector, TLSConnector)


def test_broker_handler__create_from_config__creates_handler_with_sasl_connector():
    broker_config = {
        'cert_db': 'cert_db',
        'sasl_user': 'sasl_user',
        'sasl_password': 'sasl_password',
        'host': 'hostname'
    }

    with mock.patch('swim_pubsub.core.utils.create_ssl_domain', return_value=mock.Mock()):
        broker_handler = BrokerHandler.create_from_config(broker_config)

        assert isinstance(broker_handler, BrokerHandler)
        assert isinstance(broker_handler.connector, SASLConnector)


def test_broker_handler__create_receiver_fails__raises_BrokerHandlerError():
    broker_handler = BrokerHandler(mock.Mock())
    broker_handler.container = mock.Mock()
    broker_handler.container.create_receiver = mock.Mock(side_effect=Exception('proton error'))

    with pytest.raises(BrokerHandlerError) as e:
        broker_handler._create_receiver('endpoint')
    assert f"proton error" == str(e.value)


def test_broker_handler__create_sender_fails__raises_BrokerHAndlerError():
    broker_handler = BrokerHandler(mock.Mock())
    broker_handler.container = mock.Mock()
    broker_handler.container.create_sender = mock.Mock(side_effect=Exception('proton error'))

    with pytest.raises(BrokerHandlerError) as e:
        broker_handler._create_sender('endpoint')
    assert f"proton error" == str(e.value)


def test_broker_handler__on_start(caplog):
    caplog.set_level(logging.DEBUG)

    connector = Mock()
    connector.url = "URL"
    connection = Mock()
    connector.connect = Mock(return_value=connection)
    handler = BrokerHandler(connector)

    event = Mock()
    event.container = Mock()

    handler.on_start(event)

    assert event.container == handler.container
    assert connection == handler.conn
    assert handler.started is True

    log_message = caplog.records[0]
    assert f'Connected to broker @ {handler.connector.url}' == log_message.message

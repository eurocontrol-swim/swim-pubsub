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
from unittest.mock import Mock

import pytest

from swim_pubsub.apps.base import App
from swim_pubsub.core.broker_handlers_ import BrokerHandler
from swim_pubsub.core.clients import PubSubClient
from swim_pubsub.errors import AppError, PubSubClientError

__author__ = "EUROCONTROL (SWIM)"


def test_app__before_run__appends_callables():
    handler = mock.Mock()

    app = App(handler)

    @app.before_run
    def callable1(): pass

    @app.before_run
    def callable2(): pass

    action_names = [c.__name__ for c in app._before_run_actions]

    assert 2 == len(action_names)
    assert ['callable1', 'callable2'] == action_names


@pytest.mark.parametrize('invalid_callable_argument', [
    1, 2.3, True, (), {}, []
])
def test_app__before_run__argument_no_callable__raises_AppError(invalid_callable_argument):
    handler = mock.Mock()

    app = App(handler)

    with pytest.raises(AppError) as e:
        app.before_run(invalid_callable_argument)
    assert f'{invalid_callable_argument} is not callable' == str(e.value)


@mock.patch('swim_pubsub.core.base._ProtonContainer.run', return_value=None)
def test_app__run__before_run_actions_run_before_app_run(mock_protoncontainer):
    handler = mock.Mock()

    app = App(handler)

    callable1 = mock.Mock()
    callable2 = mock.Mock()

    app.before_run(callable1)
    app.before_run(callable2)

    app.run()

    assert callable1.called
    assert callable2.called


def test_app__register_client__client_class_is_not_of_type_Client__raises_pubsubclienterror():
    handler = mock.Mock()

    app = App(handler)

    class FakeClient:
        pass

    with pytest.raises(PubSubClientError) as e:
        app.register_client('username', 'password', client_class=FakeClient)
    assert "client_class should be PubSubClient or should inherit from PubSubClient" == str(e.value)


def test_app__register_client__client_is_not_valid__raises_pubsubclienterror():
    handler = mock.Mock()

    app = App(handler)
    app.config = {'SUBSCRIPTION-MANAGER': {}}

    class TestClient(PubSubClient):
        def is_valid(self):
            return False
    TestClient.create = Mock(return_value=TestClient(broker_handler=Mock(), sm_service=Mock()))

    with pytest.raises(PubSubClientError) as e:
        app.register_client('username', 'password', client_class=TestClient)

    assert "User 'username' is not valid" == str(e.value)


def test_app_register_client__client_class_is_valid__client_is_properly_registered():
    handler = mock.Mock()

    app = App(handler)
    app.config = {'SUBSCRIPTION-MANAGER': {}}

    class CustomPubSubClient(PubSubClient):
        pass

    client = CustomPubSubClient(broker_handler=mock.Mock(), sm_service=mock.Mock())

    CustomPubSubClient.create = mock.Mock(return_value=client)

    app.register_client('username', 'password', CustomPubSubClient)

    assert client in app.clients


def test_app__remove_client__client_is_not_registered__raises_AppError():
    handler = mock.Mock()

    app = App(handler)

    client = PubSubClient(broker_handler=mock.Mock(), sm_service=mock.Mock())

    with pytest.raises(AppError) as e:
        app.remove_client(client)
    assert f"PubSubClient {client} was not found" == str(e.value)


def test_app__remove_client__client_is_registered_and_removed_properly():
    handler = mock.Mock()

    app = App(handler)

    client = PubSubClient(broker_handler=mock.Mock(), sm_service=mock.Mock())

    app.clients.append(client)

    app.remove_client(client)

    assert [] == app.clients


def test_app__create_from_config():
    config = {
        'BROKER': {
            'host': 'host'
        },
    }

    with mock.patch('swim_pubsub.core.utils.yaml_file_to_dict', return_value=config):
        app = App._create_from_config('config_file', broker_handler_class=BrokerHandler)

        assert isinstance(app._handler, BrokerHandler)
        assert config == app.config

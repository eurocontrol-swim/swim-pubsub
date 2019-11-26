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

from rest_client.errors import APIError
from subscription_manager_client.subscription_manager import SubscriptionManagerClient

from swim_pubsub.core import ConfigDict
from swim_pubsub.core.broker_handlers import BrokerHandler
from swim_pubsub.core.subscription_manager_service import SubscriptionManagerService
from swim_pubsub.core.errors import PubSubClientError

__author__ = "EUROCONTROL (SWIM)"

_logger = logging.getLogger(__name__)


class PubSubClient:

    def __init__(self, broker_handler: BrokerHandler, sm_service: SubscriptionManagerService) -> None:
        """
        Represents an actual SubscriptionManager user (publisher or subscriber)

        :param broker_handler: the BrokerHandler used by the app which is passed to every client using it.
        :param sm_service: the service proxy which will be used to access the SubscriptionManager
        """
        self.broker_handler: BrokerHandler = broker_handler
        self.sm_service: SubscriptionManagerService = sm_service

    @classmethod
    def create(cls,
               broker_handler: BrokerHandler,
               sm_config: ConfigDict,
               username: str,
               password: str):
        """
        Helper factory constructor
        :param broker_handler:
        :param sm_config:
        :param username: the actual SubscriptionManager username of the client
        :param password: the actual SubscriptionManager password of the client
        :return: PubSubClient
        """
        sm_client = cls._create_sm_client(sm_config, username, password)

        sm_service = SubscriptionManagerService(sm_client)

        return cls(broker_handler, sm_service)

    @staticmethod
    def _create_sm_client(config: ConfigDict, username: str, password: str) -> SubscriptionManagerClient:
        """
        Create a SubscriptionManagerClient but first make a check whether the given credentials a valid.
        :param config:
        :param username:
        :param password:
        :return:
        """
        sm_client: SubscriptionManagerClient = SubscriptionManagerClient.create(
            host=config['host'],
            https=config['https'],
            timeout=config['timeout'],
            verify=config['verify'],
            username=username,
            password=password
        )

        try:
            sm_client.ping_credentials()
        except APIError as e:
            if e.status_code == 401:
                raise PubSubClientError('Invalid user credentials')
            raise

        return sm_client

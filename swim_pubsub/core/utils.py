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
from functools import wraps
from typing import Callable, Union, Optional

import yaml
from proton import SSLDomain, SSLUnavailable

from swim_pubsub.core import ConfigDict
from swim_pubsub.core.errors import SubscriptionManagerServiceError, BrokerHandlerError

__author__ = "EUROCONTROL (SWIM)"


_logger = logging.getLogger(__name__)


def yaml_file_to_dict(filename: str) -> ConfigDict:
    """
    Converts a yml file into a dict
    :param filename:
    :return:
    """
    if not filename.endswith(".yml"):
        raise ValueError("YAML config files should end with '.yml' extension (RTFMG).")

    with open(filename) as f:
        obj = yaml.load(f, Loader=yaml.FullLoader)

    return obj or None


def _get_ssl_domain(mode: int) -> Union[SSLDomain, None]:
    """

    :param mode:
    :return:
    """
    try:
        return SSLDomain(mode)
    except SSLUnavailable as e:
        _logger.warning(str(e))
        return None


def create_ssl_domain(cert_db: str,
                      cert_file: Optional[str] = None,
                      cert_key: Optional[str] = None,
                      cert_password: Optional[str] = None,
                      mode: int = SSLDomain.VERIFY_PEER) -> Union[SSLDomain, None]:
    """
    Creates an SSLDomain to be passed upon connecting to the broker
    :param cert_db: path to certificate DB
    :param cert_file: path to client certificate
    :param cert_key: path to client key
    :param cert_password: password of the client
    :param mode: one of MODE_CLIENT, MODE_SERVER, VERIFY_PEER, VERIFY_PEER_NAME, ANONYMOUS_PEER
    :return:
    """
    ssl_domain = _get_ssl_domain(mode)

    ssl_domain.set_trusted_ca_db(cert_db)

    if cert_file and cert_key and cert_password:
        ssl_domain.set_credentials(cert_file, cert_key, cert_password)

    return ssl_domain


def handle_sms_error(f: Callable) -> Callable:
    """
    Handles SubscriptionManagerServiceError cases by logging the error before raising
    :param f:
    :return:
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except SubscriptionManagerServiceError as e:
            message = f"Error while accessing Subscription Manager: {str(e)}"
            _logger.error(message)
            raise e
    return wrapper


def handle_broker_handler_error(f: Callable) -> Callable:
    """
    Handles BrokerHandlerError cases by logging the error before raising
    :param f:
    :return:
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except BrokerHandlerError as e:
            _logger.error(str(e))
            raise e
    return wrapper

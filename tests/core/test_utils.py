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

import pytest
from proton import SSLUnavailable, SSLDomain

from swim_pubsub.core import utils

__author__ = "EUROCONTROL (SWIM)"


@mock.patch('proton.SSLDomain.__init__', side_effect=SSLUnavailable('SSL unavailable'))
def test__get_ssl_domain__sslunavailable__returns_none_and_logs_a_warning(mock_ssldomain, caplog):
    caplog.set_level(logging.DEBUG)

    ssl_domain = utils._get_ssl_domain(SSLDomain.VERIFY_PEER)

    assert ssl_domain is None
    assert 'SSL unavailable' == caplog.messages[0]


def test__get_ssl_domain():
    ssl_domain = utils._get_ssl_domain(SSLDomain.VERIFY_PEER)

    assert isinstance(ssl_domain, SSLDomain)


@pytest.mark.parametrize('cert_db, cert_file, cert_key, cert_password, set_credentials_called', [
    ('cert_db', 'cert_file', 'cert_key', 'cert_password', True),
    ('cert_db', None, 'cert_key', 'cert_password', False),
    ('cert_db', 'cert_file', None, 'cert_password', False),
    ('cert_db', 'cert_file', 'cert_key', None, False)
])
def test_create_ssl_domain(cert_db, cert_file, cert_key, cert_password, set_credentials_called):
    with mock.patch('swim_pubsub.core.utils._get_ssl_domain') as mock__get_ssl_domain:

        # mock_set_trusted_ca_db = mock.Mock()
        # mock_set_credentials = mock.Mock()

        mock_ssldomain = mock.Mock()
        mock_ssldomain.set_trusted_ca_db = mock.Mock()
        mock_ssldomain.set_credentials = mock.Mock()

        mock__get_ssl_domain.return_value = mock_ssldomain

        utils.create_ssl_domain(cert_db=cert_db, cert_file=cert_file, cert_key=cert_key, cert_password=cert_password)

        mock_ssldomain.set_trusted_ca_db.assert_called_once_with(cert_db)

        if set_credentials_called:
            mock_ssldomain.set_credentials.assert_called_once_with(cert_file, cert_key, cert_password)
        else:
            mock_ssldomain.set_credentials.assert_not_called()

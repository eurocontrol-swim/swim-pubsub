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
import json
from datetime import datetime, timedelta
from functools import partial
from typing import Any, Optional, List

from opensky_network_client.models import FlightConnection
from opensky_network_client.opensky_network import OpenskyNetworkClient
from proton import Message
from rest_client.errors import APIError

from swim_pubsub.core.topics import TopicGroup
from swim_pubsub.publisher import PubApp

__author__ = "EUROCONTROL (SWIM)"


def _today():
    today = datetime.today()
    begin = datetime(today.year, today.month, today.day, 0, 0) - timedelta(days=1)
    end = datetime(today.year, today.month, today.day, 23, 59, 59)

    return int(begin.timestamp()), int(end.timestamp())


class OpenSkyNetworkDataHandler:
    def __init__(self):
        self.client = OpenskyNetworkClient.create('opensky-network.org')

    def _get_arrivals_today(self, icao24: str) -> List[FlightConnection]:
        begin, end = _today()

        try:
            result = self.client.get_flight_arrivals(icao24, begin, end, json=True)
        except APIError:
            result = []

        return result

    def arrivals_today_handler(self, icao24: str, topic_group_data: Optional[Any] = None) -> Message:
        arrivals = self._get_arrivals_today(icao24)

        message = Message()
        message.content_type = 'application/json'
        message.body = json.dumps(arrivals)

        return message

    def _get_departures_today(self, icao24: str) -> List[FlightConnection]:
        begin, end = _today()

        try:
            result = self.client.get_flight_departures(icao24, begin, end, json=True)
        except APIError:
            result = []

        return result

    def departures_today_handler(self, icao24: str, topic_group_data: Optional[Any] = None) -> Message:
        departures = self._get_departures_today(icao24)

        message = Message()
        message.content_type = 'application/json'
        message.body = json.dumps(departures)

        return message


if __name__ == '__main__':

    app = PubApp.create_from_config('config.yml')

    data_handler = OpenSkyNetworkDataHandler()

    airports = {
        'Brussels': 'EBBR',
        'Amsterdam': 'EHAM',
        'Paris': 'LFPG',
        'Berlin': 'EDDB',
        'Athens': 'LGAV',
        'Madrid': 'LECU'
    }

    arrivals_topic = TopicGroup('arrivals', 5)
    departures_topic = TopicGroup('departures', 5)

    for airport, icao24 in airports.items():
        arrivals_callback = partial(data_handler.arrivals_today_handler, icao24)
        departures_callback = partial(data_handler.departures_today_handler, icao24)

        arrivals_topic.create_topic(topic_id=f"arrivals.{airport.lower()}", callback=arrivals_callback)
        departures_topic.create_topic(topic_id=f"departures.{airport.lower()}", callback=departures_callback)

    publisher = app.register_publisher('swim-adsb', 'swim-adsb')
    publisher.register_topic_group(arrivals_topic)
    publisher.register_topic_group(departures_topic)

    @app.before_run
    def populate_publisher_topics():
        publisher.populate_topics()

    app.run()

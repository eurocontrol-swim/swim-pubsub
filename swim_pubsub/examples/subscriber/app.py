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
import os

from proton import Message

from swim_pubsub.subscriber import SubApp

__author__ = "EUROCONTROL (SWIM)"


def handler(message: Message, topic: str) -> None:

    with open(f'/home/alex/data/{topic}', 'a') as f:
        data = json.loads(message.body)

        f.write("New message:\n")
        f.write(f'Content-Type: {message.content_type}\n')
        f.write(f'Data: {json.dumps(data, indent=4, sort_keys=True)}\n\n')


current_dir = os.path.dirname(os.path.realpath(__file__))
config_file = os.path.join(current_dir, 'config.yml')
app = SubApp.create_from_config(config_file)

app.run(threaded=True)

subscriber = app.register_subscriber('swim-explorer', 'swim-explorer')


# basic functions of the core
#
# >>> from functools import partial
# >>> subscriber.subscribe('arrivals.paris', callback=partial(handler, topics='arrivals.paris'))
# >>> subscriber.pause('arrivals.paris')
# >>> subscriber.resume('arrivals.paris')
# >>> subscriber.unsubscribe('arrivals.paris')

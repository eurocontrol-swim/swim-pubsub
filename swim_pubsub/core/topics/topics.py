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
import inspect
import logging
from typing import Optional, Callable, Any, Iterable

import proton
from proton.handlers import MessagingHandler

__author__ = "EUROCONTROL (SWIM)"

_logger = logging.getLogger(__name__)


class PipelineError(Exception):
    pass


class Pipeline(list):

    def __init__(self, handler_sequence: Iterable = ()) -> None:
        """
        A collection of handlers to be executed one by one in a pipeline mode. See below `append` for details on the
        expected handler signature

        :param handler_sequence:
        """
        for handler in handler_sequence:
            self._validate_handler(handler)

        super().__init__(handler_sequence)

    def append(self, handler: Callable) -> None:
        """
        :param handler: optional callback to generate data for the specific topics
                          - after and arbitrary number of positional arguments it accepts an optional parameter 'context'
                            for passing potential data from the parent topics and then an arbitrary number of keyword
                            arguments
                          - it returns a proton.Message or any other type
                          - it raises a PipelineError in case or error

                          Signature:
                            callback(*args, context: Optional[TopicDataType] = None, **kwargs) -> TopicDataType
                                \"""
                                :raises PipelineError
                                \"""
        """
        super().append(self._validate_handler(handler))

    @staticmethod
    def _validate_handler(handler: Callable) -> Callable:
        if not isinstance(handler, Callable):
            raise ValueError(f"{handler} is not callable")

        parameters = inspect.getfullargspec(handler).args

        if 'context' not in parameters:
            raise ValueError(f"handler {handler} should accept a `context` parameter")

        return handler

    def run(self, context: Optional[Any] = None) -> Any:
        """
        Runs the handlers in pipeline mode and returns the result
        :param context:
        :return:
        """
        data = context
        for handler in self:
            data = handler(context=data)

        return data


class Topic:

    def __init__(self, topic_id: str, pipeline: Pipeline):
        """
        Represents a topic in the broker identified by topic_id. The provided pipeline will generate the data to be sent
        in the broker for this topic.

        :param topic_id:
        :param pipeline:
        """
        self.id = topic_id
        self.pipeline = pipeline

    def __repr__(self):
        return f"<Topic '{self.id}'>"

    def run_pipeline(self, context: Optional[Any] = None) -> Any:
        """
        :param context:
        :return:
        """
        return self.pipeline.run(context=context)


class ScheduledTopic(MessagingHandler, Topic):

    def __init__(self, topic_id: str, pipeline: Pipeline, interval_in_sec: int, **kwargs) -> None:
        """
        A topic to be run upon interval periods.
        It inherits from `proton.MessagingHandler` in order to take advantage of its event scheduling functionality

        :param interval_in_sec:
        """
        MessagingHandler.__init__(self)
        Topic.__init__(self, topic_id, pipeline)

        self.interval_in_sec = interval_in_sec

        # a callback to be set from the broker handler upon first scheduling
        self._message_send_callback: Optional[Callable] = None

    def __repr__(self):
        return f"<ScheduledTopic '{self.id}'>"

    def set_message_send_callback(self, message_send_callback: Callable):
        """

        :param message_send_callback: has signature: callback(message: Any, subject: str)
        """
        self._message_send_callback = message_send_callback

    def _trigger_message_send(self) -> None:
        """
        Generates the topic data and sends them via the broker
        :return:
        """
        if not self._message_send_callback:
            _logger.warning(f"Not able to send messages because no sender "
                            f"has been assigned yet for topic '{self.id}'")
            return

        try:
            data = self.pipeline.run()
        except PipelineError as e:
            _logger.error(f"Error while getting data of scheduled topic {self.id}: {str(e)}")
            return

        _logger.info(f"Sending message for scheduled topic {self.id}")
        self._message_send_callback(message=data, subject=self.id)

    def on_timer_task(self, event: proton.Event):
        """
        Is triggered upon a scheduled action. The first scheduling will be done by the broker handler and then the topic
        will be re-scheduling itself

        :param event:
        """
        # send the pipeline data
        self._trigger_message_send()

        # and re-schedule the topic
        event.container.schedule(self.interval_in_sec, self)

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
from abc import ABC, abstractmethod
from distutils.util import strtobool
from functools import reduce
from itertools import zip_longest

__author__ = "EUROCONTROL (SWIM)"


# a generic (not) default value. To be used for parameters that take no default value
_NO_DEFAULT = object
_ALLOWED_TYPES = [int, float, str, bool]

class Parameter:
    _type = None

    def __init__(self, name, default=_NO_DEFAULT):
        self.name = name

        if default != _NO_DEFAULT:
            self.default = default

    def is_required(self):
        return 'default' not in self.__dict__

    def _cast(self, value):
        return self._type(value)

    def cast(self, value):
        try:
            return self._cast(value)
        except ValueError:
            raise Exception(f"Unable to cast '{value}' to {self._type}")


class ParameterBool(Parameter):
    _type = bool

    def _cast(self, value):
        return bool(strtobool(value))


class ParameterInt(Parameter):
    _type = int


class ParameterFloat(Parameter):
    _type = float


class ParameterStr(Parameter):
    _type = str


PARAMETER_TYPES = {
    int: ParameterInt,
    float: ParameterFloat,
    str: ParameterStr,
    bool: ParameterBool
}


class FilterStringInterface(ABC):

    _separator = None

    @classmethod
    @abstractmethod
    def from_string(cls, filter_str):
        pass

    @abstractmethod
    def to_dict(self):
        pass


class SingleFilter(FilterStringInterface):
    _separator = '='

    def __init__(self, name, value):
        self.name = name
        self.value = value

    @classmethod
    def from_string(cls, filter_str):
        try:
            name, value = filter_str.split(cls._separator)
        except ValueError:
            raise Exception(f"SingleFilter string should follow the format 'name{cls._separator}value'")

        return cls(name, value)

    def to_dict(self):
        return {
            self.name: self.value
        }


class MultipleFilter(FilterStringInterface):
    _separator = "&"

    def __init__(self, filters):
        self.filters = filters

    @classmethod
    def from_string(cls, filter_str):
        try:
            single_filter_strings = filter_str.split(cls._separator)
        except ValueError:
            raise Exception(f"MultipleFilter string should follow the format "
                            f"'name1_value1{cls._separator}name2_value2[...]'")

        filters = [SingleFilter.from_string(single_filter_str) for single_filter_str in single_filter_strings]

        return cls(filters)

    def to_dict(self):
        filter_dicts = [f.to_dict() for f in self.filters]

        return reduce(lambda d1, d2: dict(d1.items() | d2.items()), filter_dicts, {})


class TopicHandler:

    def __init__(self, handler):
        """

        :param handler:
        """
        self._handler_spec = inspect.getfullargspec(handler)

        if self._check_missing_annotation(self._handler_spec):
            raise TypeError('some arguments are missing annotation hints')

        self._params_defaults = list(reversed(list(zip_longest(reversed(self._handler_spec.args),
                                                               reversed(self._handler_spec.defaults),
                                                               fillvalue=_NO_DEFAULT))))
        self.params = [self._make_param(param_name, default_value)
                       for param_name, default_value
                       in self._params_defaults]

        self._handler = handler

    def __call__(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        kwargs = self.validate_calling_args(**kwargs)

        return self._handler(**kwargs)

    def _make_param(self, param_name, default_value):
        """

        :param param_name:
        :param default_value:
        :return:
        """
        param_type = self._handler_spec.annotations[param_name]

        try:
            param_class = PARAMETER_TYPES[param_type]
            return param_class(param_name, default=default_value)
        except KeyError:
            raise Exception(f"Unexpected type '{param_type} detected'. Allowed types: {PARAMETER_TYPES.keys()}")

    def _check_missing_annotation(self, spec):
        """

        :param spec:
        :return:
        """
        return set(spec.args) != set(spec.annotations.keys())

    def validate_calling_args(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        for param in self.params:
            if param.name not in kwargs:
                if param.is_required():
                    raise ValueError(f"required argument '{param.name}' is missing")
                else:
                    kwargs[param.name] = param.default
            else:
                kwargs[param.name] = param.cast(kwargs[param.name])

        return kwargs


class TopicProcessor:

    def __init__(self, handler, filter_str):
        self.topic_handler = TopicHandler(handler)
        self.filters = MultipleFilter.from_string(filter_str)
        self.calling_args = self.topic_handler.validate_calling_args(**self.filters.to_dict())

    def __call__(self, *args, **kwargs):
        return self.topic_handler(**self.calling_args)

    'arrivals.<str:airport_icao24>.<int:start>.<int.end>'
    'arrivals.EBBR.#'
    'arrivals.EBBR.#'

    'states.<str:airport>.<list[float]:bbox>'
    'states.*.[4.5, 3.4, 4.6, 2.3]'

    'states.<str:airplane_icao24>.<str:country>'
    'states.*.Belgium'
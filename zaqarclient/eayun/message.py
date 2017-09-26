# Copyright (c) 2017 Eayun, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from oslo_utils import timeutils
from zaqarclient.queues.v2 import message


class Message(message.Message):
    def __init__(self, queue, ttl, age, body,
                 href=None, id=None, status_end=None,
                 handle=None, status=None, consume_count=None,
                 first_consumed_at=None, next_consume_at=None,
                 created_at=None):
        super(Message, self).__init__(queue, ttl, age, body,
                                      href=href, id=id)
        self.status = status
        self.status_end = status_end
        self.handle = handle
        self.consume_count = consume_count
        self._first_consumed_at = first_consumed_at
        self._next_consume_at = next_consume_at
        self._created_at = created_at

    @property
    def status_end_time(self):
        if self.status_end:
            return timeutils.iso8601_from_timestamp(self.status_end)
        else:
            return '--'

    @property
    def first_consumed_at(self):
        if self._first_consumed_at:
            return timeutils.iso8601_from_timestamp(self._first_consumed_at)
        else:
            return '--'

    @property
    def next_consume_at(self):
        if self._next_consume_at:
            return timeutils.iso8601_from_timestamp(self._next_consume_at)
        else:
            return '--'

    @property
    def created_at(self):
        if self._created_at:
            return timeutils.iso8601_from_timestamp(self._created_at)
        else:
            return '--'


def create_object(parent):
    return lambda args: Message(parent, **args)

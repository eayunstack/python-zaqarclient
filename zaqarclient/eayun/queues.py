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
from zaqarclient.queues.v2 import queues


class Queue(queues.Queue):

    def __init__(self, client, name, href=None, metadata=None,
                 created_at=None, updated_at=None,
                 auto_create=True, force_create=False):
        super(Queue, self).__init__(client, name, href=href,
                                    metadata=metadata,
                                    auto_create=auto_create,
                                    force_create=force_create)
        self._created_at = created_at
        self._updated_at = updated_at

    @property
    def created_at(self):
        if self._created_at:
            return timeutils.iso8601_from_timestamp(self._created_at)

    @property
    def updated_at(self):
        if self._updated_at:
            return timeutils.iso8601_from_timestamp(self._updated_at)


def create_object(parent):
    return lambda args: Queue(parent, args["name"], href=args.get("href"),
                              metadata=args.get("metadata"),
                              created_at=args.get("created_at"),
                              updated_at=args.get("updated_at"),
                              auto_create=False)

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
from zaqarclient.eayun import core
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

    @property
    def monitor(self):
        req, trans = self.client._request_and_transport()
        monitor = core.queue_get_monitor(trans, req, self._name)
        queue = core.queue_get(trans, req, self._name)
        self._created_at = queue['queue'].get('created_at')
        self._updated_at = queue['queue'].get('updated_at')
        metadata = queue['queue'].get('metadata', {})
        monitor_body = {
            'name': self._name,
            'metadata': metadata,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
        }
        m = monitor.keys()[0]
        for mk in monitor[m].keys():
            monitor_body[mk] = monitor[m][mk]
        return monitor_body

    def metadata(self, new_meta=None):
        """Get metadata and return it

        :param new_meta: A dictionary containing
            an updated metadata object. If present
            the queue metadata will be updated in
            remote server. If the new_meta is empty,
            the metadata object will be cleared.
        :type new_meta: `dict`

        :returns: The queue metadata.
        """
        req, trans = self.client._request_and_transport()

        queue = core.queue_get(trans, req, self._name)

        self._metadata = queue['queue'].get('metadata', {})

        if new_meta is not None:
            changes = []
            for key, value in new_meta.items():
                # If key exists, replace it's value.
                if self._metadata.get(key, None):
                    changes.append({'op': 'replace',
                                    'path': '/metadata/%s' % key,
                                    'value': value})

            self._metadata = core.queue_update(trans, req, self._name,
                                               metadata=changes)

        return self._metadata


def create_object(parent):
    return lambda args: Queue(parent, args["name"], href=args.get("href"),
                              metadata=args.get("metadata"),
                              created_at=args.get("created_at"),
                              updated_at=args.get("updated_at"),
                              auto_create=False)

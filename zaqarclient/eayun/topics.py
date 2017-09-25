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

import re

from oslo_utils import timeutils
from zaqarclient.eayun import core

TOPIC_NAME_REGEX = re.compile('^[a-zA-Z0-9_\-]+$')


class Topic(object):

    def __init__(self, client, name, href=None, metadata=None,
                 created_at=None, updated_at=None,
                 auto_create=True, force_create=False):
        """Initialize topic object

        :param client: The client object of Zaqar.
        :type client: `object`
        :param name: Name of the topic.
        :type name: `six.string_type`
        :param auto_create: If create the topic automatically in database.
        :type auto_create: `boolean`
        :param force_create: If create the topic and skip the API version
            check, which is useful for command line interface.
        :type force_create: `boolean`
        :returns: The topic object.
        """
        self.client = client

        if name == "":
            raise ValueError(_('Topic name does not have a value'))

        if not TOPIC_NAME_REGEX.match(str(name)):
            raise ValueError(_('The topic name may only contain ASCII '
                               'letters, digits, underscores and dashes.'))

        self._name = name
        self._metadata = metadata
        self._href = href
        self._created_at = created_at
        self._updated_at = updated_at

        if auto_create or force_create:
            self.ensure_exists(force_create=force_create)

    def ensure_exists(self, force_create=False):
        """Ensures a topic exists

        This method is not race safe,
        the topic could've been deleted
        right after it was called.
        """
        req, trans = self.client._request_and_transport()
        if force_create:
            core.topic_create(trans, req, self._name)

    @property
    def name(self):
        return self._name

    @property
    def href(self):
        return self._href

    @property
    def metadata_dict(self):
        return dict(self.metadata())

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
        monitor = core.topic_get_monitor(trans, req, self._name)
        topic = core.topic_get(trans, req, self._name)
        if not topic:
            return {}
        self._created_at = topic['topic'].get('created_at')
        self._updated_at = topic['topic'].get('updated_at')
        metadata = topic['topic'].get('metadata', {})
        monitor_body = {
            'name': self._name,
            'metadata': metadata,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
        }
        for m in monitor.keys():
            for mk in monitor[m].keys():
                monitor_body[mk] = monitor[m][mk]
        return monitor_body

    def metadata(self, new_meta=None):
        """Get metadata and return it

        :param new_meta: A dictionary containing
            an updated metadata object. If present
            the topic metadata will be updated in
            remote server. If the new_meta is empty,
            the metadata object will be cleared.
        :type new_meta: `dict`

        :returns: The topic metadata.
        """
        req, trans = self.client._request_and_transport()

        topic = core.topic_get(trans, req, self._name)

        self._metadata = topic['topic'].get('metadata', {})

        if new_meta is not None:
            changes = []
            for key, value in new_meta.items():
                # If key exists, replace it's value.
                if self._metadata.get(key, None):
                    changes.append({'op': 'replace',
                                    'path': '/metadata/%s' % key,
                                    'value': value})

            self._metadata = core.topic_update(trans, req, self._name,
                                               metadata=changes)

        return self._metadata

    def delete(self):
        req, trans = self.client._request_and_transport()
        core.topic_delete(trans, req, self._name)


def create_object(parent):
    return lambda args: Topic(parent, args["name"], href=args.get("href"),
                              metadata=args.get("metadata"),
                              created_at=args.get("created_at"),
                              updated_at=args.get("updated_at"),
                              auto_create=False)

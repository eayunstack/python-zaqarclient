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

from zaqarclient.queues.v2 import api


class Es(api.V2):
    label = 'v2'
    schema = api.V2.schema.copy()


Es.schema.update({
    'queue_purge': {
        'ref': 'queues/{queue_name}/purge',
        'method': 'DELETE',
        'required': ['queue_name'],
        'properties': {
            'queue_name': {'type': 'string'}
        }
    },
    'queue_get_monitor': {
        'ref': 'monitors/queues/{queue_name}',
        'method': 'GET',
        'required': ['queue_name'],
        'properties': {
            'queue_name': {'type': 'string'}
        }
    },
})

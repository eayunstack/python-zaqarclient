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

"""
This module defines a lower level API for queues' v2. This level of the
API is responsible for packing up the final request, sending it to the server
and handling asynchronous requests.

Functions present in this module assume that:

    1. The transport instance is ready to `send` the
    request to the server.

    2. Transport instance holds the conf instance to use for this
    request.
"""

from zaqarclient.queues.v2 import core

queue_create = core.queue_create
queue_get = core.queue_get
queue_update = core.queue_update
queue_delete = core.queue_delete
queue_list = core.queue_list
queue_purge = core.queue_purge


def queue_get_monitor(transport, request, queue_name):
    """Gets a queue monitor data

    :param transport: Transport instance to use
    :type transport: `transport.base.Transport`
    :param request: Request instance ready to be sent.
    :type request: `transport.request.Request`
    :param queue_name: Queue reference name.
    :type queue_name: `six.text_type`

    """

    request.operation = 'queue_get_monitor'
    request.params['queue_name'] = queue_name

    resp = transport.send(request)
    return resp.deserialized_content

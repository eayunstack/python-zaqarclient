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

from zaqarclient.queues.v2 import client
from zaqarclient.eayun import queues


class Client(client.Client):
    """Client base class

    :param url: Zaqar's instance base url.
    :type url: `six.text_type`
    :param version: API Version pointing to.
    :type version: `int`
    :param options: Extra options:
        - client_uuid: Custom client uuid. A new one
        will be generated, if not passed.
        - auth_opts: Authentication options:
            - backend
            - options
    :type options: `dict`
    """

    queues_module = queues

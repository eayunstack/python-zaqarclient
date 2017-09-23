# Copyright (c) 2017 Eayun, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from osc_lib import utils

from zaqarclient.queues.v2 import cli


def _get_client(obj, parsed_args):
    obj.log.debug("take_action(%s)" % parsed_args)
    return obj.app.client_manager.messaging


class ListQueues(cli.ListQueues):
    """List available queues"""
    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)
        kwargs = {}
        columns = ["Name"]
        if parsed_args.marker is not None:
            kwargs["marker"] = parsed_args.marker
        if parsed_args.limit is not None:
            kwargs["limit"] = parsed_args.limit
        if parsed_args.detailed is not None and parsed_args.detailed:
            kwargs["detailed"] = parsed_args.detailed
            columns.extend(["Metadata_Dict", "Href",
                            "Created_at", "Updated_at"])
        data = client.queues(**kwargs)
        columns = tuple(columns)
        return (columns, (utils.get_item_properties(s, columns) for s in data))

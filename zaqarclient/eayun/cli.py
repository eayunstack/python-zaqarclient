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

import json
import os

from osc_lib import utils
from osc_lib.command import command
from oslo_log import log as logging

from zaqarclient._i18n import _
from zaqarclient.queues.v2 import cli
from zaqarclient.transport import errors


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


class CreateQueue(cli.CreateQueue):
    """Create a queue"""
    pass


class DeleteQueue(cli.DeleteQueue):
    """Delete a queue"""
    pass


class SetQueueMetadata(cli.SetQueueMetadata):
    """Set queue metadata"""
    pass


class PurgeQueue(cli.PurgeQueue):
    """Purge a queue"""
    pass


class GetQueueMonitor(command.ShowOne):
    """Get queue stats"""

    _description = _("Get queue monitor")
    log = logging.getLogger(__name__ + ".GetQueueMonitor")

    def get_parser(self, prog_name):
        parser = super(GetQueueMonitor, self).get_parser(prog_name)
        parser.add_argument(
            "queue_name",
            metavar="<queue_name>",
            help="Name of the queue")
        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)
        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name, auto_create=False)

        try:
            monitor = queue.monitor
        except errors.ResourceNotFound:
            raise RuntimeError('Queue(%s) does not exist.' % queue_name)

        columns = ("Name", "Metadata", "msg_counts", "msg_bytes",
                   "bulk_msg_counts", "bulk_msg_bytes",
                   "consume_msg_counts", "consume_msg_bytes",
                   "active_msgs", "inactive_msgs",
                   "delayed_msgs", "deleted_msgs",
                   "Created_at", "Updated_at"
                   )
        data = dict(monitor)
        return columns, utils.get_dict_properties(data, columns)


class ListTopics(command.Lister):
    """List available topics"""

    _description = _("List available topics")
    log = logging.getLogger(__name__ + ".ListTopics")

    def get_parser(self, prog_name):
        parser = super(ListTopics, self).get_parser(prog_name)
        parser.add_argument(
            "--marker",
            metavar="<topic_id>",
            help="Topic's paging marker")
        parser.add_argument(
            "--limit",
            metavar="<limit>",
            help="Page size limit")
        parser.add_argument(
            "--detailed",
            action="store_true",
            help="If show detailed information of topic")

        return parser

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
        data = client.topics(**kwargs)
        columns = tuple(columns)
        return (columns, (utils.get_item_properties(s, columns) for s in data))


class CreateTopic(command.ShowOne):
    """Create a topic"""

    _description = _("Create a topic")
    log = logging.getLogger(__name__ + ".CreateTopic")

    def get_parser(self, prog_name):
        parser = super(CreateTopic, self).get_parser(prog_name)
        parser.add_argument(
            "topic_name",
            metavar="<topic_name>",
            help="Name of the topic")
        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)
        topic_name = parsed_args.topic_name
        data = client.topic(topic_name, force_create=True)
        columns = ('Name',)
        return columns, utils.get_item_properties(data, columns)


class DeleteTopic(command.Command):
    """Delete a topic"""

    _description = _("Delete a topic")
    log = logging.getLogger(__name__ + ".DeleteTopic")

    def get_parser(self, prog_name):
        parser = super(DeleteTopic, self).get_parser(prog_name)
        parser.add_argument(
            "topic_name",
            metavar="<topic_name>",
            help="Name of the topic")
        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)
        topic_name = parsed_args.topic_name
        client.topic(topic_name).delete()


class GetTopicMonitor(command.ShowOne):
    """Get topic stats"""

    _description = _("Get topic monitor")
    log = logging.getLogger(__name__ + ".GetTopicMonitor")

    def get_parser(self, prog_name):
        parser = super(GetTopicMonitor, self).get_parser(prog_name)
        parser.add_argument(
            "topic_name",
            metavar="<topic_name>",
            help="Name of the topic")
        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)
        topic_name = parsed_args.topic_name
        topic = client.topic(topic_name, auto_create=False)

        try:
            monitor = topic.monitor
        except errors.ResourceNotFound:
            raise RuntimeError('Topic(%s) does not exist.' % topic_name)

        columns = ("Name", "Metadata", "msg_counts", "msg_bytes",
                   "bulk_msg_counts", "bulk_msg_bytes",
                   "sub_msg_counts", "sub_msg_bytes",
                   "total_sub_msg_counts", "total_sub_msg_bytes",
                   "Created_at", "Updated_at"
                   )
        data = dict(monitor)
        return columns, utils.get_dict_properties(data, columns)


class SetTopicMetadata(command.Command):
    """Set topic metadata"""

    _description = _("Set topic metadata")
    log = logging.getLogger(__name__ + ".SetTopicMetadata")

    def get_parser(self, prog_name):
        parser = super(SetTopicMetadata, self).get_parser(prog_name)
        parser.add_argument(
            "topic_name",
            metavar="<topic_name>",
            help="Name of the topic")
        parser.add_argument(
            "topic_metadata",
            metavar="<topic_metadata>",
            help="Topic metadata, All the metadata of "
                 "the topic will be replaced by topic_metadata")
        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)
        topic_name = parsed_args.topic_name
        topic_metadata = parsed_args.topic_metadata

        try:
            valid_metadata = json.loads(topic_metadata)
        except ValueError:
            raise RuntimeError("Topic metadata(%s) is not a valid json." %
                               topic_metadata)

        client.topic(topic_name, auto_create=False).\
            metadata(new_meta=valid_metadata)


class CreateSubscription(command.ShowOne):
    """Create a subscription for topic"""
    _description = _("Create a subscription for topic")
    log = logging.getLogger(__name__ + ".CreateSubscription")

    def get_parser(self, prog_name):
        parser = super(CreateSubscription, self).get_parser(prog_name)
        parser.add_argument(
            "topic_name",
            metavar="<topic_name>",
            help="Name of the topic to subscribe to")
        parser.add_argument(
            "subscriber",
            metavar="<subscriber>",
            help="Subscriber which will be notified")
        parser.add_argument(
            "--options",
            type=json.loads,
            default={},
            metavar="<options>",
            help="Metadata of the subscription in JSON format")

        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)
        kwargs = {'options': parsed_args.options}
        if parsed_args.subscriber:
            kwargs['subscriber'] = parsed_args.subscriber

        data = client.subscription(parsed_args.topic_name, **kwargs)

        if not data:
            raise RuntimeError('Failed to create subscription for (%s).' %
                               parsed_args.subscriber)

        columns = ('ID', 'Subscriber', 'Options')
        return columns, utils.get_item_properties(data, columns)


class ListSubscriptions(command.Lister):
    """List available subscriptions"""

    _description = _("List available subscriptions")
    log = logging.getLogger(__name__ + ".ListSubscriptions")

    def get_parser(self, prog_name):
        parser = super(ListSubscriptions, self).get_parser(prog_name)
        parser.add_argument(
            "topic_name",
            metavar="<topic_name>",
            help="Name of the topic to subscribe to")
        parser.add_argument(
            "--marker",
            metavar="<subscription_id>",
            help="Subscription's paging marker, "
            "the ID of the last subscription of the previous page")
        parser.add_argument(
            "--limit",
            metavar="<limit>",
            help="Page size limit, default value is 20")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        client = self.app.client_manager.messaging

        kwargs = {'topic_name': parsed_args.topic_name}
        if parsed_args.marker is not None:
            kwargs["marker"] = parsed_args.marker
        if parsed_args.limit is not None:
            kwargs["limit"] = parsed_args.limit

        data = client.subscriptions(**kwargs)
        columns = ('ID', 'Subscriber', 'Confirmed', 'Options')
        return (columns,
                (utils.get_item_properties(s, columns) for s in data))


class ShowSubscription(command.ShowOne):
    """Display subscription details"""

    _description = _("Display subscription details")
    log = logging.getLogger(__name__ + ".ShowSubscription")

    def get_parser(self, prog_name):
        parser = super(ShowSubscription, self).get_parser(prog_name)
        parser.add_argument(
            "topic_name",
            metavar="<topic_name>",
            help="Name of the topic to subscribe to"
        )
        parser.add_argument(
            "subscription_id",
            metavar="<subscription_id>",
            help="ID of the subscription"
        )
        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)
        kwargs = {'id': parsed_args.subscription_id}
        pool_data = client.subscription(parsed_args.topic_name,
                                        **kwargs)
        columns = ('ID', 'Subscriber', 'Confirmed', 'Options')
        return columns, utils.get_dict_properties(pool_data.__dict__, columns)


class UpdateSubscription(command.ShowOne):
    """Update a subscription"""

    _description = _("Update a subscription")
    log = logging.getLogger(__name__ + ".UpdateSubscription")

    def get_parser(self, prog_name):
        parser = super(UpdateSubscription, self).get_parser(prog_name)
        parser.add_argument(
            "topic_name",
            metavar="<topic_name>",
            help="Name of the topic to subscribe to")
        parser.add_argument(
            "subscription_id",
            metavar="<subscription_id>",
            help="ID of the subscription"
        )
        parser.add_argument(
            "--options",
            type=json.loads,
            default={},
            metavar="<options>",
            help="Metadata of the subscription in JSON format")

        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)
        data = {'options': parsed_args.options}

        kwargs = {'id': parsed_args.subscription_id}
        subscription = client.subscription(parsed_args.topic_name,
                                           auto_create=False, **kwargs)
        subscription.update(data)
        columns = ('ID', 'Options')
        return columns, utils.get_dict_properties(subscription.__dict__,
                                                  columns)


class DeleteSubscription(command.Command):
    """Delete a subscription"""

    _description = _("Delete a subscription")
    log = logging.getLogger(__name__ + ".DeleteSubscription")

    def get_parser(self, prog_name):
        parser = super(DeleteSubscription, self).get_parser(prog_name)
        parser.add_argument(
            "topic_name",
            metavar="<topic_name>",
            help="Name of the topic for the subscription")
        parser.add_argument(
            "subscription_id",
            metavar="<subscription_id>",
            help="ID of the subscription"
        )
        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)
        client.subscription(parsed_args.topic_name,
                            id=parsed_args.subscription_id,
                            auto_create=False).delete()


class PostMessages(cli.PostMessages):
    """Post messages for queue"""
    pass


class ListMessages(command.Lister):
    """List all messages for a given queue"""

    _description = _("List all messages for a given queue")
    log = logging.getLogger(__name__ + ".ListMessages")

    def get_parser(self, prog_name):
        parser = super(ListMessages, self).get_parser(prog_name)
        parser.add_argument(
            "queue_name",
            metavar="<queue_name>",
            help="Name of the queue")
        parser.add_argument(
            "--limit",
            metavar="<limit>",
            type=int,
            help="Maximum number of messages to get")
        parser.add_argument(
            "--marker",
            metavar="<message_id>",
            help="Message's paging marker, "
            "the ID of the last message of the previous page")
        parser.add_argument(
            "--echo",
            action="store_true",
            help="Whether to get this client's own messages")
        parser.add_argument(
            "--include-claimed",
            action="store_true",
            help="Whether to include claimed messages")
        parser.add_argument(
            "--include-delayed",
            action="store_true",
            help="Whether to include delayed messages")
        parser.add_argument(
            "--client-id",
            metavar="<client_id>",
            default=os.environ.get("OS_MESSAGE_CLIENT_ID"),
            help="A UUID for each client instance.")
        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)

        if not parsed_args.client_id:
            raise AttributeError("<--client-id> option is missing and "
                                 "environment variable OS_MESSAGE_CLIENT_ID "
                                 "is not set. Please at least either pass in "
                                 "the client id or set the environment "
                                 "variable")
        else:
            client.client_uuid = parsed_args.client_id

        kwargs = {}
        if parsed_args.limit is not None:
            kwargs["limit"] = parsed_args.limit
        if parsed_args.marker is not None:
            kwargs["marker"] = parsed_args.marker
        if parsed_args.echo is not None:
            kwargs["echo"] = parsed_args.echo
        if parsed_args.include_claimed is not None:
            kwargs["include_claimed"] = parsed_args.include_claimed
        if parsed_args.include_delayed is not None:
            kwargs["include_delayed"] = parsed_args.include_delayed

        queue = client.queue(parsed_args.queue_name)

        messages = queue.messages(**kwargs)

        columns = ("ID", "TTL", "Age", "Status", "Status_End_Time")
        return (columns,
                (utils.get_item_properties(s, columns) for s in messages))


class ConsumeMessages(command.Lister):
    """Consume messages from a queue."""

    _description = _("Consume messages and return a list of consume messages")
    log = logging.getLogger(__name__ + ".ConsumeMessages")

    def get_parser(self, prog_name):
        parser = super(ConsumeMessages, self).get_parser(prog_name)
        parser.add_argument(
            "queue_name",
            metavar="<queue_name>",
            help="Name of the queue to be claim")
        parser.add_argument(
            "--limit",
            metavar="<limit>",
            type=int,
            default=10,
            help="Consumes a set of messages, up to limit")
        parser.add_argument(
            "--auto-delete",
            type=int,
            help="Whether to auto delete the consume messages")

        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)

        kwargs = {}
        if parsed_args.limit is not None:
            kwargs["limit"] = parsed_args.limit
        if parsed_args.auto_delete is not None:
            kwargs["auto_delete"] = parsed_args.auto_delete

        queue = client.queue(parsed_args.queue_name, auto_create=False)
        keys = ("body", "handle", "id", "ttl", "age", "consume_count",
                "first_consumed_at", "next_consume_at", "created_at",)
        columns = ("Messages", "Handle", "Message_ID", "TTL", "Age", "Times",
                   "Initial Time", "Next Time", "Created At")
        data = queue.consume(**kwargs)
        return (columns,
                (utils.get_item_properties(s, keys) for s in data))


class ConsumeMessagesDelete(command.ShowOne):
    """Delete a consume messages"""

    _description = _("Delete consume messages")
    log = logging.getLogger(__name__ + ".ConsumeMessagesDelete")

    def get_parser(self, prog_name):
        parser = super(ConsumeMessagesDelete, self).get_parser(prog_name)
        parser.add_argument(
            "queue_name",
            metavar="<queue_name>",
            help="Name of the queue")
        parser.add_argument(
            "handles",
            metavar="<handles>",
            help="Handles for consume messages.")
        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)
        queue = client.queue(parsed_args.queue_name, auto_create=False)
        handles = []
        if parsed_args.handles:
            handles = parsed_args.handles.split(',')
        data = queue.consume_delete(handles)
        columns = ("Expired", "Invalid", "Success")
        if data:
            return columns, utils.get_dict_properties(data, columns)
        return columns, (None, None, handles)


class PublishMessages(command.Command):
    """Publish messages for to a given topic"""

    _description = _("Publish messages for a given topic")
    log = logging.getLogger(__name__ + ".PublishMessages")

    def get_parser(self, prog_name):
        parser = super(PublishMessages, self).get_parser(prog_name)
        parser.add_argument(
            "topic_name",
            metavar="<topic_name>",
            help="Name of the topic")
        parser.add_argument(
            "messages",
            type=json.loads,
            metavar="<messages>",
            help="Messages to be published.")
        parser.add_argument(
            "--client-id",
            metavar="<client_id>",
            default=os.environ.get("OS_MESSAGE_CLIENT_ID"),
            help="A UUID for each client instance.")
        return parser

    def take_action(self, parsed_args):
        client = _get_client(self, parsed_args)

        if not parsed_args.client_id:
            raise AttributeError("<--client-id> option is missing and "
                                 "environment variable OS_MESSAGE_CLIENT_ID "
                                 "is not set. Please at least either pass in "
                                 "the client id or set the environment "
                                 "variable")
        else:
            client.client_uuid = parsed_args.client_id

        topic = client.topic(parsed_args.topic_name)
        topic.publish(parsed_args.messages)

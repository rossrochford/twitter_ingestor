import hashlib
import os
import random

import redio
from trio_util.pqueue_workers.items import ControlItem
from trio_util.pqueue_workers.pqueue import PriorityQueue
from trio_util.pqueue_workers.worker_groups import BatchWorker
from twitter_pqueue_scraper.util.redis_util import RedisGroupStreamClient
from twitter_pqueue_scraper.util.db_util import create_sqlalchemy_session
from twitter_pqueue_scraper.util.items import (
    TwitterConversationWorkItem, TwitterProfileWorkItem
)


class TwitterWorker(BatchWorker):

    def __init__(
        self, worker_name, worker_config, redis_stream_name,
        consumer_group_name, worker_key, twitter_session,
        django_http_session=None
    ):
        self.redis_stream_name = redis_stream_name
        self.consumer_group_name = consumer_group_name

        self.worker_key = worker_key
        self.twitter_session = twitter_session
        self.django_http_session = django_http_session
        self.priority_queue = PriorityQueue()

        super(TwitterWorker, self).__init__(worker_name, worker_config)

    async def setup_worker_resources(self, global_ctx):
        # called from within self.worker_loop()

        # todo: should we place these on the group-level to lower number of connections?
        self.db_connection = await create_sqlalchemy_session()
        self.redis_stream = RedisGroupStreamClient(
            self.redis_stream_name, self.consumer_group_name, None
        )

    async def preprocess_item(self, item_dict):

        if item_dict.get('flush_group') or item_dict.get('exit'):
            succ, item_obj = ControlItem.create_from_dict(item_dict)
        elif item_dict['work_type'] == 'conversation':
            succ, item_obj = TwitterConversationWorkItem.create_from_dict(item_dict)
        else:
            succ, item_obj = TwitterProfileWorkItem.create_from_dict(item_dict)

        if not succ:
            print(f'error: invalid item_dict: {item_dict}')
            if item_dict.get('line_id'):
                await self.redis_stream.xack(item.line_id)

        return succ, item_obj

    async def _postprocess_batch(self, global_ctx, curr_batch):
        line_ids = [item.line_id for item in curr_batch if item.line_id]
        if not line_ids:
            return
        await self.redis_stream.xack(*line_ids)

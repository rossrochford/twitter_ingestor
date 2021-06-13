from collections import defaultdict, namedtuple
import datetime
import random

from eliot import to_file as eliot_init_file
import tractor
import trio
from trio_util.http_util import TrioHttpSession

from twitter_pqueue_scraper.batch_tasks.user_info import scrape_user_info
from twitter_pqueue_scraper.batch_tasks.conversation_tweets import scrape_conversation_tweets
from twitter_pqueue_scraper.batch_tasks.user_timeline import scrape_user_timeline
from twitter_pqueue_scraper.batch_tasks.user_likes import scrape_user_likes
from twitter_pqueue_scraper.batch_tasks.friend_ids import scrape_friend_ids
from twitter_pqueue_scraper.batch_tasks.follower_ids import scrape_follower_ids
from twitter_pqueue_scraper.util.redis_util import RedisGroupStreamClient
from twitter_pqueue_scraper.execution.worker import TwitterWorker
from twitter_pqueue_scraper.util.http_util import TwitterHttpSession


# a per process cache
WORKERS = {}


BatchWorkerConfig = namedtuple('WorkerConfig', [
    'func', 'batch_size', 'batch_delay', 'channel_size', 'num_workers_per_account'
])

WORKER_TYPES = {
    # 'func', 'batch_size', 'batch_delay', 'channel_size', 'num_workers_per_account'
    'user_info': BatchWorkerConfig(scrape_user_info, 100, 1.5, 130, 1),
    'user_timeline': BatchWorkerConfig(scrape_user_timeline, 4, 0, 6, 2),
    'user_likes': BatchWorkerConfig(scrape_user_likes, 3, 1, 4, 1),
    'friend_ids': BatchWorkerConfig(scrape_friend_ids, 1, 0, 3, 1),
    'follower_ids': BatchWorkerConfig(scrape_follower_ids, 1, 0, 3, 1),
    'conversation_tweets': BatchWorkerConfig(scrape_conversation_tweets, 2, 0, 5, 1),
}


async def _triage_item_to_worker(item, actor_name):
    global WORKERS

    work_type = item.get('work_type')
    account_key = item.get('account_key')
    priority = item.get('priority', 2)

    if item.get('exit'):
        for worker in WORKERS.values():
            worker.priority_queue.put_nowait(1, item)
        return

    if not work_type:
        print("error: missing work_type"); return

    if item.get('flush_group'):
        # push flush-item to the top of each queue
        # wait 0.8s for any recent items to be popped first
        await trio.sleep(0.8)
        for key, worker in WORKERS.items():
            if key[2] == work_type:
                print(f"flushing: {key}")
                worker.priority_queue.put_nowait(1, item)
        return

    if not account_key:
        print("error: missing account_key"); return

    i = 0
    relevant_keys = []
    while True:
        worker_key = (actor_name, account_key, work_type, i)
        if worker_key not in WORKERS:
            break
        relevant_keys.append(worker_key)
        i += 1

    if not relevant_keys:
        print(f"error: no relevant_keys found: {account_key}, {work_type}")
        return

    if len(relevant_keys) == 1:
        # most common case (where num_workers_per_account = 1)
        queue = WORKERS[relevant_keys[0]].priority_queue
    else:
        index = random.randint(0, len(relevant_keys)-1)
        queue = WORKERS[relevant_keys[index]].priority_queue

    queue.put_nowait(priority, item)


async def submit_items(items):
    actor_name = tractor.current_actor().name
    for item in items:
        await _triage_item_to_worker(item, actor_name)

'''
async def fetch_queue_sizes():
    global ACTOR_QUEUES

    actor_name = tractor.current_actor().name

    queue_sizes = {k: len(q) for (k, q) in ACTOR_QUEUES.items()}
    return (actor_name, queue_sizes)
'''


async def _pop_and_move_item(worker_obj):

    q = worker_obj.priority_queue
    if q.queue_size > 3000:
        # no point in proceeding, the worker's send_channel is most likely blocked
        return False

    try:
        priority, item = q.get_nowait()
    except trio.WouldBlock:
        return True

    try:
        worker_obj.send_channel.send_nowait(item)
    except trio.WouldBlock:
        # put back on queue
        q.put_nowait(priority, item)
        await trio.sleep(0.2)

    return False  # 'was_empty'


async def daemon__move_items_from_queues_to_channels():
    global WORKERS

    last_successful_get = {}
    while True:
        now = datetime.datetime.now()
        was_empty = {}
        for key, worker_obj in WORKERS.items():
            was_empty[key] = await _pop_and_move_item(worker_obj)
            if was_empty[key] is False:
                last_successful_get[key] = now

        for k, dt in last_successful_get.items():
            if dt is not None and (now-dt).seconds > 40:
                print(f"flushing: {k}")
                flush_item = {'flush_group': True, 'work_type': k[2]}
                await WORKERS[k].send_channel.send(flush_item)
                last_successful_get[k] = None

        if all(was_empty.values()):
            # all priority_queues were empty on this iteration, so wait
            await trio.sleep(0.3)


async def actor_main(
    redis_stream_name, consumer_group_name, api_keys
):

    actor_name = tractor.current_actor().name
    global_ctx = {}
    workers = []

    eliot_init_file(open(f"/tmp/eliot-{actor_name}.log", "w"))

    print(f"START: actor_daemon() in actor: {actor_name}")

    async with trio.open_nursery() as n:

        for work_type, worker_config in WORKER_TYPES.items():

            for account_key, account_details in api_keys.items():

                twitter_session = TwitterHttpSession.create_from_account_dict(account_details)

                num_workers_per_account = worker_config.num_workers_per_account
                if twitter_session.http_session.proxy_url is not None:
                    num_workers_per_account = 1  # always 1 when using a proxy

                for worker_num in range(num_workers_per_account):
                    worker_key = (actor_name, account_key, work_type, worker_num)
                    worker_fullname = str(worker_key)
                    #snd_chann, recv_chan = trio.open_memory_channel(500)
                    #WORKER_CHANNELS[worker_key] = (snd_chann, recv_chan)

                    #n.start_soon(
                    #    worker_main, redis_stream_name,
                    #    consumer_group_name, worker_key, sess
                    #)
                    django_http_session = None
                    if work_type == 'user_info':
                        django_http_session = TrioHttpSession(ssl=False, verify=False)

                    WORKERS[worker_key] = w = TwitterWorker(
                        worker_fullname, worker_config, redis_stream_name, consumer_group_name,
                        worker_key, twitter_session, django_http_session=django_http_session
                    )
                    n.start_soon(w.worker_loop, global_ctx)

        n.start_soon(
            daemon__move_items_from_queues_to_channels
        )

    print(f"END: actor_daemon() in actor: {actor_name}")

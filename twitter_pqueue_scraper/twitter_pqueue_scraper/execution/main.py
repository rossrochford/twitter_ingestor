from collections import defaultdict
import datetime
import json
import hashlib
import os
import random
import uuid

from eliot import to_file as eliot_init_file
import tractor
import trio

from twitter_pqueue_scraper.execution.actor import actor_main, submit_items
from twitter_pqueue_scraper.util.redis_util import RedisGroupStreamClient


REDIS_STREAM = 'twitter-items3'
CONSUMER_GROUP = f"{REDIS_STREAM}-cg"
CONSUMER_NAME = 'twitter-scraper-main'  # f"main-{uuid.uuid4().hex[:16]}"

API_KEYS_FILEPATH = '/app/api-keys.json'
NUM_ACCOUNTS_PER_ACTOR = 2  # adjusts the amount of async concurrency per actor-process


def _get_routing_string(msg):

    if msg['work_type'] == 'conversation_tweets':
        return msg['conversation_id']

    # longer string seems to distribute items more fairly, so concatenate all ids
    st = str(msg.get('obj_id') or '')
    user_id = str(msg.get('user_id') or '')
    screen_name = msg.get('screen_name') or ''
    if user_id:
        st = st + user_id
    if screen_name:
        st = st + screen_name
    return st.strip()


def _choose_account_key(routing_string, account_keys):

    num_workers = len(account_keys)
    if num_workers == 1:
        return account_keys[0]

    h = hashlib.md5(routing_string.encode())
    hsh = int(h.hexdigest(), 16)
    index = hsh % num_workers

    return account_keys[index]


async def daemon__forward_items(
    portals, actor_names_by_accountkey
):

    redis_stream = RedisGroupStreamClient(
        REDIS_STREAM, CONSUMER_GROUP, CONSUMER_NAME
    )
    await redis_stream.xgroup_create()

    print('start flush')
    await redis_stream.flush_old_lines()
    print('end flush')

    account_keys = [k for k in actor_names_by_accountkey.keys()]
    account_keys.sort()

    while True:
        line_dicts, failed_ids = await redis_stream.xreadgroup()
        if not line_dicts:
            await trio.sleep(0.3)
            continue

        to_submit = defaultdict(list)

        for line_id, msg_dict in line_dicts.items():
            msg_dict['line_id'] = line_id

            if msg_dict.get('flush_group') or msg_dict.get('exit'):
                # send flush/exit messages to all actors
                for portal_name in portals.keys():
                    to_submit[portal_name].append(msg_dict)
                continue

            routing_string = _get_routing_string(msg_dict)
            if not routing_string:
                print("warning: routing_string is blank, skipping item")
                await redis_stream.xack(line_id)
                continue
            account_key = _choose_account_key(routing_string, account_keys)
            msg_dict['account_key'] = account_key  # key used to route to worker-task within actor-process
            portal_name = actor_names_by_accountkey[account_key]
            to_submit[portal_name].append(msg_dict)  # collect items into per-portal batches

        for portal_name, items in to_submit.items():
            p = portals[portal_name]
            await p.run(submit_items, items=items)

    print("FINISHED: daemon__forward_items")


async def get_api_keys():

    def _get_api_keys():
        with open(API_KEYS_FILEPATH) as f:
            try:
                return json.load(f)
            except:
                return None

    api_keys = await trio.to_thread.run_sync(_get_api_keys)

    if api_keys is None:
        exit(f"failed to open api-keys.json file: {API_KEYS_FILEPATH}")
    if len(api_keys) == 0:
        exit(f"No valid twitter account credentials found in: {API_KEYS_FILEPATH}, bearer_token must be included")

    return api_keys


async def _launch_actor(actor_name, actor_nursery, proc_api_keys):
    portal = await actor_nursery.run_in_actor(
        actor_main, name=actor_name,
        redis_stream_name=REDIS_STREAM,
        consumer_group_name=CONSUMER_GROUP,
        api_keys=proc_api_keys
    )
    return portal


async def main():

    eliot_init_file(open(f"/tmp/eliot-main-actor.log", "w"))

    API_KEYS = await get_api_keys()
    TWITTER_API_KEYS = API_KEYS['twitter']

    portals, actor_names_by_accountkey = {}, {}

    async with tractor.open_nursery() as actor_nursery:
        async with trio.open_nursery() as task_nursery:

            main_actor_name = tractor.current_actor().name
            print(f"main actor: {main_actor_name}")

            _account_keys = [k for k in TWITTER_API_KEYS.keys()]

            while _account_keys:
                # assign API keys to actor-processes
                proc_api_keys = {}
                for i in range(NUM_ACCOUNTS_PER_ACTOR):
                    if not _account_keys:
                        break
                    k = _account_keys.pop(0)
                    proc_api_keys[k] =TWITTER_API_KEYS[k]

                actor_name = f"proc-{len(portals)}"
                portals[actor_name] = await _launch_actor(
                    actor_name, actor_nursery, proc_api_keys
                )

                for key in proc_api_keys.keys():
                    actor_names_by_accountkey[key] = actor_name

            task_nursery.start_soon(
                daemon__forward_items, portals, actor_names_by_accountkey
            )


if __name__ == '__main__':
    trio.run(main)

import json
import logging
import os

import msgpack
import redis

from twitter.models import TwitterProfile


logger = logging.getLogger(__name__)


SCRAPER_QUEUE_NAME = 'twint_twitter_items'
REDIS_STREAM = 'twitter-items3'
REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')

'''
def send_scrape_work__given_screen_names(
    redis_cli, screen_names, work_type, priority=2, flush=False
):
    profile_objects = TwitterProfile.objects.filter(screen_name__in=screen_names)

    if work_type.startswith('user_info'):
        screen_names_found = [obj.screen_name for obj in profile_objects]
        screen_names_to_query = [
            sn for sn in screen_names if sn not in screen_names_found
        ]
    else:
        screen_names_to_query = []

    send_scrape_work(
        redis_cli, profile_objects, screen_names_to_query,
        work_type, priority=priority, flush=flush
    )

    return len(profile_objects), len(screen_names_to_query)
'''

def _create_userinfo_item(profile_or_id, priority):

    item_dict = {
        'work_type': 'user_info',
        'priority': priority
    }

    if type(profile_or_id) is str:
        if profile_or_id.isdigit():
            item_dict['user_id'] = profile_or_id
        else:
            item_dict['screen_name'] = profile_or_id
    else:
        profile = profile_or_id
        if not (profile.user_id or profile.screen_name):
            print(f"error: profile missing user_id and screen_name, pk:: {profile.id}")
            return False, None

        item_dict['obj_id'] = profile.id
        item_dict['user_id'] = profile.user_id
        item_dict['screen_name'] = profile.screen_name

    for key, val in item_dict.items():
        try:
            val = msgpack.dumps(val)
        except:
            print(f"msgpack failed {profile.id} user_info")
            return False, None
        item_dict[key] = val

    return True, item_dict


def _create_item(profile_or_id, work_type, priority):

    if type(profile_or_id) is str:
        return False, None

    profile = profile_or_id

    if not profile.user_id:
        print(f"error: profile missing user_id, pk: {profile.id} {work_type}")
        return False, None

    if profile.is_available is False or not profile.user_info:
        # private/deleted account, or missing user_info, skip these
        return False, None

    if not profile.user_info:
        return False, None

    item_dict = {
        'obj_id': profile.id,
        'user_id': profile.user_id,
        'work_type': work_type,
        'priority': priority,
        'user_info': profile.user_info
    }

    if work_type == 'user_timeline' and profile.user_timeline_since_id:
        item_dict['since_id'] = profile.user_timeline_since_id

    elif work_type == 'user_likes' and profile.user_likes_since_id:
        item_dict['since_id'] = profile.user_likes_since_id

    for key, val in item_dict.items():
        try:
            val = msgpack.dumps(val)
        except:
            print(f"msgpack failed {profile.id} {work_type}")
            return False, None
        item_dict[key] = val

    return True, item_dict


def _chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

'''
def send_scrape_work_OLD(
    redis_cli, profiles, work_type, priority=2, flush=False
):
    if redis_cli is None:
        redis_cli = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT)

    items = []
    for profile_or_id in profiles:
        if work_type == 'user_info':
            succ, item_dict = _create_userinfo_item(profile_or_id, priority)
        else:
            succ, item_dict = _create_item(profile_or_id, work_type, priority)
        if succ:
            items.append(item_dict)

    if not items:
        return

    if flush:
        items.append({'flush_group': True})

    for item_chunk in _chunker(items, 50):
        msg = {'work_type': work_type, 'items': item_chunk}
        redis_cli.lpush(SCRAPER_QUEUE_NAME, json.dumps(msg))

    if flush:
        return len(items) - 1
    return len(items)
'''


def send_scrape_work(
    redis_cli, profiles, work_type, priority=2, flush=False
):

    if redis_cli is None:
        redis_cli = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT)

    items = []
    for profile_or_id in profiles:
        if work_type == 'user_info':
            succ, item_dict = _create_userinfo_item(profile_or_id, priority)
        else:
            succ, item_dict = _create_item(profile_or_id, work_type, priority)
        if succ:
            items.append(item_dict)

    if not items:
        return

    num_items = len(items)
    if flush:
        items.append({
            'flush_group': msgpack.dumps(True),
            'work_type': msgpack.dumps(work_type)
        })

    with redis_cli.pipeline() as pipe:
        pipe = redis_cli.pipeline()
        for item_dict in items:
            pipe.xadd(REDIS_STREAM, item_dict)
        pipe.execute()
    '''
    for item_dict in items:
        redis_cli.xadd(REDIS_STREAM, item_dict)'''

    return num_items


'''
def _send_for_userinfo_scrape(tag_slugs_list, flush=True):
    profiles = Tag.get_profiles_with_tags(tag_slugs_list)
    profiles = [o for o in profiles if o.is_due_userinfo_scrape]

    userinfo_items, item_count = [], 0

    for profile in profiles:
        if not (profile.user_id or profile.screen_name):
            continue
        userinfo_items.append({
            'obj_id': profile.id,
            'user_id': profile.user_id or None,
            'screen_name': profile.screen_name or None,
            'work_type': 'user_info',
            'priority': 1
        })
        item_count += 1

    if not userinfo_items:
        return 0

    redis_cli = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT)

    work_type = 'user_info'
    if flush:
        userinfo_items.append('flush-group')
    msg = {'work_type': work_type, 'items': userinfo_items}
    redis_cli.lpush(SCRAPER_QUEUE_NAME, json.dumps(msg))

    return item_count
'''

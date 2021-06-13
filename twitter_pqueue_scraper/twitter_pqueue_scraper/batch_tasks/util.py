import datetime
import os
import re
import uuid

import pytz
import trio

from twitter_pqueue_scraper.util.db_util import (
    get_or_create_by_key, get_or_create_by_multiple_keys
)
from util_shared.datetime_utils import get_utc_now


DB_FETCH_FIELDS = {
    'friend_ids': [
        'friend_ids', 'friend_ids_fully_scraped', 'friend_ids_cursor', 'friend_ids_last_scraped',
        'friend_ids_prev_status_code', 'user_info', 'is_available'
    ],
    'follower_ids': [
        'follower_ids', 'follower_ids_fully_scraped', 'follower_ids_cursor', 'follower_ids_last_scraped',
        'follower_ids_prev_status_code', 'user_info', 'is_available'
    ]
}

DJONGO_WEBSERVER_HOSTNAME = os.environ.get('DJONGO_WEBSERVER_HOSTNAME', 'localhost')
DJONGO_WEBSERVER_PORT = os.environ.get('DJONGO_WEBSERVER_PORT', '8000')
DJONGO_NOTIFY_URL = f"http://{DJONGO_WEBSERVER_HOSTNAME}:{DJONGO_WEBSERVER_PORT}/notify-new-data/"

MONTH_NAMES = r'(?P<month_name>jan|feb|march|mar|april|apr|may|june|jun|july|jul|aug|sept|sep|oct|nov|dec)'
DATE_REGEX = MONTH_NAMES + r' (?P<day_num>\d{1,2}) (?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}) .+ (?P<year>\d{4})$'
MONTH_NAME_TO_NUM = {
    'jan': 1,
    'feb': 2,
    'march': 3,
    'mar': 3,
    'april': 4,
    'apr': 4,
    'may': 5,
    'june': 6,
    'jun': 6,
    'july': 7,
    'jul': 7,
    'aug': 8,
    'sept': 9,
    'sep': 9,
    'oct': 10,
    'nov': 11,
    'dec': 12
}

MENTION_REGEX = r'@(?P<screen_name>\w+)'


def parse_date_str(date_string):

    if date_string.endswith('Z') and 'T' in date_string:
        # v2 api format, ISO 8601
        dt = datetime.datetime.fromisoformat(date_string[:-1])
        return dt.replace(tzinfo=pytz.UTC)

    match = re.search(DATE_REGEX, date_string, flags=re.I)

    if match is None:
        print(f"error: failed to parse created_at string: {date_string}")
        return None

    values = match.groupdict()

    return datetime.datetime(
        day=int(values['day_num']),
        month=MONTH_NAME_TO_NUM[values['month_name'].lower()],
        year=int(values['year']),
        hour=int(values['hour']),
        minute=int(values['minute']),
        tzinfo=pytz.UTC
    )


def _fetch_profiles(worker, object_ids):
    db_session, Base = worker.db_connection
    TwitterProfile = Base.classes.twitter_twitterprofile
    profiles = db_session.query(TwitterProfile).filter(TwitterProfile.id.in_(object_ids)).all()
    return {obj.id: obj for obj in profiles}


async def scrape_relationship_ids(worker, key, func, profile_batch):

    assert key in ('friend_ids', 'follower_ids')

    twitter_session = worker.twitter_session
    db_session, Base = worker.db_connection

    object_ids = [i.obj_id for i in profile_batch if i.obj_id is not None]

    current_profiles = await trio.to_thread.run_sync(
        _fetch_profiles, worker, object_ids
    )

    db_updates_all, rel_userids = {}, {}

    for item in profile_batch:
        if item.obj_id not in current_profiles:
            print(f'warning: profile {item.obj_id} doesnt exist')
            continue
        profile_obj = current_profiles[item.obj_id]

        if profile_obj.is_available is False:  # assume this is up-to-date
            continue

        initial_cursor = getattr(profile_obj, f'{key}_cursor')
        if initial_cursor == '0':
            initial_cursor = None  # if true, this is a re-scrape

        res, next_cursor, status_code = await func(
            twitter_session, item.user_id, initial_cursor=initial_cursor
        )

        db_update = {
            f'{key}_prev_status_code': status_code,
            f'{key}_prev_scrape_attempt': get_utc_now()
        }
        if status_code != 200:
            db_updates_all[item.obj_id] = db_update
            continue

        db_update[f'{key}_prev_scrape_success'] = get_utc_now()

        if initial_cursor and len(res) == 0:
            print('warning: cursor invalid')
            db_update[f'{key}_cursor'] = None  # cursor invalid
            db_update[f'{key}_fully_scraped'] = False
        else:
            if next_cursor == '0':
                db_update[f'{key}_cursor'] = None  # cursor exhausted
                db_update[f'{key}_fully_scraped'] = True
            else:
                db_update[f'{key}_fully_scraped'] = False
                db_update[f'{key}_cursor'] = next_cursor

            rel_userids[profile_obj.id] = res

        for field_name, value in db_update.items():
            if field_name != 'id':
                setattr(profile_obj, field_name, value)

    await db_session.commit_async()

    for profile_obj_id, user_ids in rel_userids.items():
        await _ingest_followers__by_userid(
            worker, profile_obj_id, key, user_ids
        )


async def _ingest_followers__by_userid(
    worker, profile_obj_id, friends_or_followers, user_ids
):
    db_session, Base = worker.db_connection
    TwitterProfile = Base.classes.twitter_twitterprofile
    ProfileFollowsProfileRel = Base.classes.twitter_profilefollowsprofilerel

    _, profiles_by_userid, new_profile_ids =  await get_or_create_by_key(
        db_session, TwitterProfile, 'user_id', user_ids,
        defaults={'manually_added': False}
    )

    self_key, other_key = 'source_id', 'dest_id'  # friend_ids
    if friends_or_followers == 'follower_ids':
        self_key, other_key = 'dest_id', 'source_id'  # swap

    rel_params = [
        {self_key: profile_obj_id, other_key: other.id}
        for other in profiles_by_userid.values()
    ]

    await get_or_create_by_multiple_keys(
        db_session, ProfileFollowsProfileRel, rel_params
    )


'''
async def notify_new_data(worker, profile_batch):

    print('NOTE: notify_new_data is disabled for now')
    return

    # notify djongo webserver of new data
    notifications = []
    for item in profile_batch:
        notifications.append({
            'twitter_profile_obj_id': item.profile_obj_id,
            'date_type': item.work_type
        })
    await worker.djongo_http_session.post(
        DJONGO_NOTIFY_URL, json={'updates': notifications}
    )'''


async def create_event(global_ctx):
    uid = uuid.uuid4().hex
    global_ctx['events'][uid] = [trio.Event(), None]
    return uid


async def set_event(global_ctx, event_uid, payload):
    if event_uid not in global_ctx['events']:
        print(f'warning: completion event {event_uid} not found')
        return
    event_and_payload = global_ctx['events'][event_uid]
    event_and_payload[1] = payload
    event_and_payload[0].set()


# for tips on how to preprocess tweet text for NLP, see page 4 here: https://arxiv.org/pdf/1708.03994.pdf
def get_mentions_from_string(text):
    text = text + ' '
    mentions = [
        sn.strip().lower() for sn in re.findall(MENTION_REGEX, text)
    ]
    return [s for s in set(mentions)]

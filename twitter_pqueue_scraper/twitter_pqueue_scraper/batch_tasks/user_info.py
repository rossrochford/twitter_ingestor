'''
scenarios:
----------------

# TODO: rewrite this pseudocode with the logic from the final soluation

get rid of these cases first:
    - user_info failed
    - user_info succeeded and existing profile has user_id set
    - item.obj_id is not set, attempt to fetch profiles by user_id and screen_name (prioritising user_id)

    - item.obj_id set
        - user_id being set for first time
            -
        - user_id already set
            -

    - item.obj_id not set
        - item.user_id is set
            - profile obj exists
                -
            - profile obj doesn't exist
                -
        - item.screen_name is set  (but not user_id)
'''
import json
import os

import trio

from twitter_pqueue_scraper.batch_tasks.user_info__profile_index import ProfileIndex
from twitter_pqueue_scraper.batch_tasks.util import get_mentions_from_string
from twitter_pqueue_scraper.scrapers.twitter_api_v1.user_info import get_user_info__chunk
from twitter_pqueue_scraper.util.db_util import create_sqlalchemy_session, get_or_create_by_multiple_keys
from twitter_pqueue_scraper.util.items import TwitterProfileWorkItem
from util_shared.datetime_utils import get_utc_now


DJANGO_HOSTNAME = os.environ['DJANGO_SERVER_HOSTNAME']
DJANGO_PORT = os.environ['DJANGO_SERVER_PORT']
DJANGO_BASE_URL = f"{DJANGO_HOSTNAME}:{DJANGO_PORT}"


async def scrape_user_info(worker, global_ctx, profile_batch):

    db_session, Base = worker.db_connection
    TwitterProfile = Base.classes.twitter_twitterprofile
    twitter_session = worker.twitter_session

    userids_or_screen_names = [
        item.user_id or item.screen_name for item in profile_batch
    ]

    status_codes = {}
    user_info_results = await get_user_info__chunk(
        twitter_session, userids_or_screen_names, status_codes
    )
    profile_index = ProfileIndex(worker, profile_batch, user_info_results)
    await profile_index.initialize_index()

    userinfo_by_userid = profile_index.get_userinfo_by_userid()
    userinfo_by_sn = profile_index.get_userinfo_by_sn()

    to_create = {}
    potential_duplicates = []
    failed_requests = []
    is_dirty = False

    for item in profile_batch:

        if not (item.user_id or item.screen_name):
            print("error: scrape_user_info received item with no user_id or screen_name")
            continue

        profile = profile_index.get_profile(item)
        status = status_codes.get(item.user_id) or status_codes.get(item.screen_name)  # may be None, I think

        errored = bool(status) and status != 200
        missing1 = item.user_id and item.user_id not in userinfo_by_userid
        missing2 = item.screen_name and item.screen_name not in userinfo_by_sn

        if errored or missing1 or missing2:
            if profile:
                profile.user_info_prev_status_code = status or -1
                profile.user_info_prev_scrape_attempt = get_utc_now()
                profile.is_available = False
            # else: nothing to update
            if item.user_id:
                failed_requests.append(item.user_id)
            if item.screen_name:
                failed_requests.append(item.screen_name)

            if profile is None and item.mentioned_by_user:
                # when request failed and the profile is missing, we create it only if there was a mention
                key = item.user_id or item. screen_name
                to_create[key] = (item, None, status)

            continue

        ui_dict = userinfo_by_userid.get(item.user_id) or userinfo_by_sn.get(item.screen_name)
        if ui_dict is None:
            continue  # should never get here?

        if profile:
            if profile.user_id is None:
                potential_duplicates.append((profile, item, ui_dict, 200))
            else:
                _set_profile_fields(profile, ui_dict, 200)
                is_dirty = True
        else:
            to_create[ui_dict['id_str']] = (item, ui_dict, 200)

    if is_dirty:
        await db_session.commit_async()

    await _resolve_potential_duplicates(worker, profile_index, potential_duplicates)  # todo: <-- this should update profile_index
    await _create_missing(worker, profile_index, to_create)
    await _ingest_profile_description_mentions(
        worker, profile_batch, profile_index, failed_requests
    )


def _set_profile_fields(profile, ui_dict, status_code):

    if ui_dict:
        profile.user_id = ui_dict['id_str']
        profile.screen_name = ui_dict['screen_name'].lower()
        profile.user_info =  json.dumps(ui_dict)
        profile.user_info_prev_scrape_success = get_utc_now()
        profile.is_available = not ui_dict['protected']
    else:
        if status_code == 401:
            profile.is_available = False
        else:
            profile.is_available = None  # is_available is unknown, does it ever get here?

    profile.user_info_prev_status_code = status_code or -1
    profile.user_info_prev_scrape_attempt = get_utc_now()


async def _resolve_potential_duplicates(worker, profile_index, potential_duplicates):

    db_session, Base = worker.db_connection
    TwitterProfile = Base.classes.twitter_twitterprofile

    def _get_duplicates(_obj_ids, _user_ids):
        dups = db_session.query(TwitterProfile).filter(TwitterProfile.id.notin_(obj_ids)).filter(TwitterProfile.user_id.in_(user_ids)).all()
        return {obj.user_id: obj for obj in dups}

    if not potential_duplicates:
        return

    obj_ids = [tup[0].id for tup in potential_duplicates]
    user_ids = [tup[2]['id_str'] for tup in potential_duplicates]

    duplicates = await trio.to_thread.run_sync(
        _get_duplicates, obj_ids, user_ids
    )

    for profile, item, ui_dict, status_code in potential_duplicates:
        user_id = ui_dict['id_str']

        if user_id in duplicates:
            old_profile = profile
            new_profile = duplicates[user_id]

            to_merge.append((new_profile.id, old_profile.id))
            _set_profile_fields(new_profile, ui_dict, status_code)
            profile_index.change_item_profile(item, new_profile, ui_dict)
        else:
            _set_profile_fields(profile, ui_dict, status_code)

    await db_session.commit_async()

    if to_merge:
        http_session = worker.django_http_session
        status, _, resp_obj = await http_session.post(
            f"{DJANGO_BASE_URL}/merge-twitter-profiles",
            json={'to_merge': to_merge, 'remove': True}
        )
        if status != 200:
            print("error: profile-merge request failed")


async def _create_missing(worker, profile_index, to_create):

    if not to_create:
        return

    db_session, Base = worker.db_connection
    TwitterProfile = Base.classes.twitter_twitterprofile

    new_profiles = []
    for userid_or_sn, (item, ui_dict, status_code) in to_create.items():

        if ui_dict:
            user_id = userid_or_sn  # this is always the user_id when ui_dict is not None
            profile = TwitterProfile(user_id=user_id)
        else:
            user_field = 'user_id' if userid_or_sn.isdigit() else 'screen_name'
            kwargs = {user_field: userid_or_sn}
            profile = TwitterProfile(**kwargs)

        _set_profile_fields(profile, ui_dict, status_code)
        print(f"created new profile: {profile.user_id} {profile.screen_name}")
        new_profiles.append(profile)

    if new_profiles:
        await db_session.add_and_commit(new_profiles)
        for new_obj in new_profiles:
            profile_index.add_profile(new_obj)


async def _ingest_profile_description_mentions(worker, profile_batch, profile_index, failed_requests):

    db_session, Base = worker.db_connection
    TwitterProfile = Base.classes.twitter_twitterprofile
    ProfileMentionedInProfileDescription = Base.classes.twitter_profilementionedinprofiledescription

    mention_rels, new_items= [], []

    for item in profile_batch:
        profile = profile_index.get_profile(item)
        if profile is None:
            if item.user_id and item.user_id in failed_requests:
                continue
            if item.screen_name and item.screen_name in failed_requests:
                continue
            import pdb; pdb.set_trace()  # should never get here?
            continue

        if item.mentioned_by_user:
            mention_rels.append({
                'profile_id': profile.id, 'mentioned_by_id':  item.mentioned_by_user
            })

        if profile.user_info is None:
            continue

        try:
            user_info = json.loads(profile.user_info)
        except:
            import pdb; pdb.set_trace()
        for screen_name in get_mentions_from_string(user_info['description']):
            if screen_name in profile_index.profiles_by_sn:
                mention_rels.append({
                    'profile_id': profile_index.profiles_by_sn[screen_name].id,
                    'mentioned_by_id': profile.id
                })
            else:
                new_items.append({
                    'work_type': 'user_info', 'screen_name': screen_name,
                    'mentioned_by_user': profile.id, 'priority': 3
                })

    if mention_rels:
        await get_or_create_by_multiple_keys(
            db_session, ProfileMentionedInProfileDescription, mention_rels
        )

    if new_items:
        print(f'adding {len(new_items)} new user-info items with mentioned_by_user set')
        await worker.redis_stream.xadd_bulk(new_items)

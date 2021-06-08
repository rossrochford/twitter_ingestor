import trio

from util_shared.datetime_utils import get_utc_now
from twitter_pqueue_scraper.ingestion.user_likes import ingest_user_likes
from twitter_pqueue_scraper.scrapers.twitter_api_v1.user_likes import get_user_likes


DEFAULT_USER_LIKES_PAGES = 4


def _fetch_profiles(worker, obj_ids):
    db_session, Base = worker.db_connection
    TwitterProfile = Base.classes.twitter_twitterprofile
    profiles = db_session.query(TwitterProfile).filter(TwitterProfile.id.in_(obj_ids)).all()
    return {obj.id: obj for obj in profiles}


# todo: add support for 'since_id'  (also rename since_id)
async def scrape_user_likes(worker, global_ctx, profile_batch):

    twitter_session = worker.twitter_session
    db_session, Base = worker.db_connection

    obj_ids = [item.obj_id for item in profile_batch if item.obj_id is not None]
    profiles_by_id = await trio.to_thread.run_sync(
        _fetch_profiles, worker, obj_ids
    )

    db_updates, user_likes_by_id = {}, {}
    for item in profile_batch:

        profile = profiles_by_id.get(item.obj_id)
        if profile is None:
            print(f'error: profile not found: {item.obj_id}')
            continue

        num_pages = 1 if item.since_id else DEFAULT_USER_LIKES_PAGES
        succ, res = await get_user_likes(
            twitter_session, item.user_id, num_pages, since_id=item.since_id
        )
        if succ:
            profile.user_likes_prev_scrape_attempt = get_utc_now()
            profile.user_likes_prev_scrape_success = get_utc_now()
            if res:
                profile.user_likes_since_id = res[0]['id_str']
                user_likes_by_id[item.user_id] = res
        else:
            profile.user_likes_since_id = None
            profile.user_likes_prev_scrape_attempt = get_utc_now()
            print(f"get_user_likes() failed with: {item.user_id}")

    await db_session.commit_async()

    for user_id, user_likes in user_likes_by_id.items():
        await ingest_user_likes(worker, user_id, user_likes)

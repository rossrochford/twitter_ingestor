import trio

from twitter_pqueue_scraper.batch_tasks.util import parse_date_str
from twitter_pqueue_scraper.ingestion.user_timeline import ingest_user_timeline
from twitter_pqueue_scraper.scrapers.twitter_api_v1.user_timeline import get_user_timeline
from util_shared.datetime_utils import get_utc_now


DEFAULT_TIMELINE_PAGES = 8


def _fetch_profiles(worker, obj_ids):
    db_session, Base = worker.db_connection
    TwitterProfile = Base.classes.twitter_twitterprofile
    profiles = db_session.query(TwitterProfile).filter(TwitterProfile.id.in_(obj_ids)).all()
    return {obj.id: obj for obj in profiles}


async def scrape_user_timeline(worker, global_ctx, profile_batch):

    twitter_session = worker.twitter_session
    db_session, Base = worker.db_connection

    obj_ids = [item.obj_id for item in profile_batch if item.obj_id is not None]
    profiles_by_id = await trio.to_thread.run_sync(
        _fetch_profiles, worker, obj_ids
    )

    timeline_data_all = {}

    for item in profile_batch:  # (scrape_job_id, profile_obj_id, user_id)

        profile = profiles_by_id.get(item.obj_id)
        if profile is None:
            print(f'error: profile not found: {item.obj_id}')
            continue

        res, _, status_code = await get_user_timeline(
            twitter_session, item.user_id, max_pages=DEFAULT_TIMELINE_PAGES,
            since_id=item.since_id
        )
        if status_code != 200:
            print(f"get_user_timeline() failed with: {item.user_id}")
        if item.since_id:
            print(f'repeat user_timeline scrape, got {len(res or [])} new tweets')

        profile.user_timeline_prev_scrape_attempt = get_utc_now()
        profile.user_timeline_prev_status_code = status_code

        if status_code == 200:
            profile.user_timeline_prev_scrape_success = get_utc_now()
        if res:
            latest_dt = parse_date_str(res[0]['created_at'])
            profile.user_timeline_latest_tweet_datetime = latest_dt
            profile.user_timeline_since_id = res[0]['id_str']
            timeline_data_all[item.user_id] = res
        else:
            profile.user_timeline_since_id = None

    await db_session.commit_async()

    for user_id, timeline_data in timeline_data_all.items():
        await ingest_user_timeline(worker, user_id, timeline_data)


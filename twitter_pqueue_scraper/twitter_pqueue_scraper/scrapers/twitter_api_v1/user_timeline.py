import sys

from eliot import start_action
import trio

from twitter_pqueue_scraper.scrapers.twitter_api_v1.util import get_cursored
from twitter_pqueue_scraper.util.http_util import TwitterHttpSession

DEFAULT_TIMELINE_PAGES = 8


# note: there is now a v2 endpoint: https://developer.twitter.com/en/docs/twitter-api/tweets/timelines/introduction
# it seems to have its own separate rate-limit but shares the same project-level monthly tweet quota


async def _get_user_timeline(twitter_session, user_id, cursor=None, since_id=None):

    status, resp_obj = await twitter_session.get_user_timeline(
        self, user_id, cursor, since_id
    )
    if status != 200:
        print('warning: /1.1/friends/list.json returned status: %s' % status)
        return None, None, status

    di = resp_obj.json()
    if not di:
        return None, None, status

    return di, di[-1]['id'], status


async def get_user_timeline(
    twitter_session, user_id, max_pages=DEFAULT_TIMELINE_PAGES,
    initial_cursor=None, delay_override=None, since_id=None
):
    if since_id:
        return await _get_user_timeline(
            twitter_session, user_id, cursor=None, since_id=since_id
        )

    action_args = dict(
        action_type="get_cursored", user_id=user_id,
        max_pages=max_pages, func_name='_get_user_timeline'
    )
    with start_action(**action_args):
        return await get_cursored(
            twitter_session, _get_user_timeline, user_id, max_pages,
            initial_cursor, delay_override
        )


async def _once(user_id, cursor=None):
    twitter_session = TwitterHttpSession.create_one()
    res, next_cursor, status_code = await get_user_timeline(
        twitter_session, user_id, 1, cursor, 0
    )
    print(f"status_code: {status_code}, num tweets: {len(res or [])}, next cursor: {next_cursor}")


if __name__ == '__main__':
    args = sys.argv[1:]
    trio.run(_once, args[0])

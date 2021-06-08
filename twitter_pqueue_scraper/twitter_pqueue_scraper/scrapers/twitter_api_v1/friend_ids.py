import sys

from eliot import start_action
import trio

from twitter_pqueue_scraper.scrapers.twitter_api_v1.util import get_cursored
from twitter_pqueue_scraper.util.http_util import TwitterHttpSession


DEFAULT_FRIEND_ID_PAGES = 3


async def _get_friend_ids(twitter_session, user_id, cursor=None):

    status, resp_obj = await twitter_session.get_friend_ids(user_id, cursor)

    if status != 200:
        print('warning: /1.1/friends/list.json returned status: %s' % status)
        return None, None, status

    di = resp_obj.json()
    ids = [str(i) for i in di['ids']]
    return ids, di['next_cursor_str'], status


async def get_friend_ids(
    twitter_session, user_id, max_pages=DEFAULT_FRIEND_ID_PAGES,
    initial_cursor=None, delay_override=None
):
    action_args = dict(
        action_type="get_cursored", user_id=user_id,
        max_pages=max_pages, func_name='_get_friend_ids'
    )
    with start_action(**action_args):
        return await get_cursored(
            twitter_session, _get_friend_ids, user_id, max_pages,
            initial_cursor, delay_override
        )


async def _once(user_id, cursor=None):
    twitter_session = TwitterHttpSession.create_one()
    res, next_cursor, status_code = await get_friend_ids(
        twitter_session.http_session, user_id, 1, cursor, 0
    )
    print(f"{len(set(res or []))} friends retrieved")
    print(f"next cursor: {next_cursor}, status_code: {status_code}")


if __name__ == '__main__':
    args = sys.argv[1:]
    assert len(args) > 0
    trio.run(_once, args[0])

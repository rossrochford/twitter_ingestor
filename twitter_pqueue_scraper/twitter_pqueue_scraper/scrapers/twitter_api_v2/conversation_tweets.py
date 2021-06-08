import sys

import trio

from twitter_pqueue_scraper.util.http_util import TwitterHttpSession


async def get_conversation_tweets(twitter_session, conversation_id):

    status, resp_obj = await twitter_session.v2__get_conversation_tweets(conversation_id)

    if status != 200:
        print('warning: /2/tweets/search/recent?query=conversation_id returned status: %s' % status)
        return status, None

    return status, resp_obj


async def _once(tweet_id):
    twitter_session = TwitterHttpSession.create_one()

    status_code, resp_obj = await get_conversation_tweets(
        twitter_session, tweet_id
    )
    print(f"status code: {status_code}")
    print(resp_obj)


if __name__ == '__main__':
    args = sys.argv[1:]
    trio.run(_once, args[0])

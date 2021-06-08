import sys

import trio

from twitter_pqueue_scraper.util.http_util import TwitterHttpSession


async def get_tweets(twitter_session, tweet_ids):

    tweet_data = {}
    for chunk in _chunker(tweet_ids, 100):

        status_code, resp_obj = await twitter_session.v2__get_tweets(chunk)

        if status_code != 200:
            import pdb; pdb.set_trace()
            print('warning: /2/tweets/ returned status: %s' % status_code)
            continue

        for di in resp_obj.json()['data']:
            tweet_data[di['id']] = di

    return tweet_data


async def _once(tweet_id):
    twitter_session = TwitterHttpSession.create_one()

    tweet_data = await get_tweets(twitter_session, [tweet_id])
    import pdb; pdb.set_trace()
    print()


if __name__ == '__main__':
    args = sys.argv[1:]
    trio.run(_once, args[0])

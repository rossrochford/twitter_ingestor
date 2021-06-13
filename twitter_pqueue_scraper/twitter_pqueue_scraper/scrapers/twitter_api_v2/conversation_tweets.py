import base64
import sys
import urllib

import trio

from twitter_pqueue_scraper.batch_tasks.util import parse_date_str
from twitter_pqueue_scraper.util.http_util import TwitterHttpSession
from twitter_pqueue_scraper.ingestion.create_deferred_models_v2 import create_deferred_models__conversation



'''
note: we can fetch conversation ids for a batch of aribtrary tweet-replies, to get their original threads: https://developer.twitter.com/en/docs/twitter-api/conversation-id

curl --request GET \
  --url 'https://api.twitter.com/2/tweets?ids=1225917697675886593&tweet.fields=author_id,conversation_id,created_at,in_reply_to_user_id,referenced_tweets&expansions=author_id,in_reply_to_user_id,referenced_tweets.id&user.fields=name,username' \
  --header 'Authorization: Bearer $BEARER_TOKEN'

'''

async def _get_conversation_tweets(twitter_session, conversation_id, cursor=None):

    status_code, resp_obj = await twitter_session.v2__get_conversation_tweets(
        conversation_id, cursor=cursor
    )

    if status_code != 200:
        import pdb; pdb.set_trace()
        print(f'warning: /2/tweets/search/recent?query=conversation_id returned status: {status_code}')
        return [], [], [], [], None, status_code

    resp_json = resp_obj.json()

    result_count = resp_json.get('meta', {}).get('result_count')
    if result_count == 0:
        # tweet_id is not a valid conversation_id
        return [], [], [], [], None, 404

    reply_tweets = resp_json['data']
    tweets_included = resp_json['includes']['tweets']
    users = resp_json['includes']['users']
    errors = resp_json.get('errors') or []

    next_token = resp_json['meta'].get('next_token') or '0'

    return reply_tweets, tweets_included, users, errors, next_token, status_code


async def get_cursored(
    twitter_http_session, conversation_id, max_pages
):

    request_delay = 1  # based on app limit of 450/15-minutes

    reply_tweets, tweets_included, users, errors = [], [], [], []
    next_cursor, status_code = None, None

    for i in range(max_pages):
        print(f"_get_conversation_tweets page_num: {i}")

        _reply_tweets, _tweets_included, _users, _errors, next_cursor, status_code = await _get_conversation_tweets(
            twitter_http_session, conversation_id, cursor=next_cursor
        )
        if not _reply_tweets:
            break
        await trio.sleep(request_delay)

        reply_tweets.extend(_reply_tweets)
        tweets_included.extend(_tweets_included)
        users.extend(_users)
        errors.extend(_errors)

        if next_cursor == '0':
            break

    return reply_tweets, tweets_included, users, errors, status_code


async def get_conversation_tweets(twitter_session, conversation_id):
    return await get_cursored(
        twitter_session, conversation_id, 3
    )


'''
https://developer.twitter.com/en/docs/twitter-api/conversation-id

Reconstructing the conversation can be done by ordering the Tweets with a matching conversation_id by timestamp,
and taking note of which Tweets are directly in reply to other Tweets in the conversation thread. This can be accomplished
by also requesting the in_reply_to_user_id field and referenced_tweets.id and in_reply_to_user_id expansions.
'''

async def _once(conversation_id):

    twitter_session = TwitterHttpSession.create_one()

    reply_tweets, tweets_included, users, errors, status_code = await get_conversation_tweets(
        twitter_session, conversation_id
    )

    '''
    if status_code != 200:
        print(f"status code: {status_code}")
        exit()
    resp_json = resp_obj.json()
    import pdb; pdb.set_trace()
    # def create_deferred_tweets__conversation(conversation_id, reply_tweets, tweets_included, users, errors):
    def_objects = create_deferred_tweets__conversation(
        conversation_id, resp_json['data'], resp_json['included']['tweets'],
        resp_json['included']['users'], resp_json.get('errors', [])
    )
    print(resp_obj)'''


'''
https://twitter.com/Altimor/status/1403132762610089984


resp_obj.json()['data'][0]
{'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'id': '1403147887035531279', 'entities': {'mentions': [{'start': 0, 'end': 8, 'username': 'Altimor'}, {'start': 9, 'end': 22, 'username': 'hamandcheese'}]}, 'text': '@Altimor @hamandcheese A more skeptical take on this paper. https://t.co/1OjVHWpjqo', 'referenced_tweets': [{'type': 'quoted', 'id': '1393246891685580801'}, {'type': 'replied_to', 'id': '1403132762610089984'}], 'lang': 'en', 'in_reply_to_user_id': '21125274', 'author_id': '537715934'}

# referenced tweets:  (includes any quoted tweets)
resp_obj.json()['includes']['tweets'][0]
{'public_metrics': {'retweet_count': 8, 'reply_count': 3, 'like_count': 35, 'quote_count': 7}, 'id': '1393246891685580801', 'text': 'I want to do a quick tweet storm about an interesting new paper out on WFH at an IT services company that found productivity declined. https://t.co/0FJKemsiZX', 'lang': 'en', 'author_id': '159904799'}

# referenced users:
resp_obj.json()['includes']['users'][0]
{'id': '537715934', 'name': 'Matt Clancy', 'username': 'mattsclancy'


main_tweet = [dict for dict in resp_obj.json()['includes']['tweets'] if dict['id'] == conversation_id][0]
'''


if __name__ == '__main__':
    args = sys.argv[1:]
    trio.run(_once, args[0])

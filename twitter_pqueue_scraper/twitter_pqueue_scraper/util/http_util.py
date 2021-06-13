import base64
import copy
from functools import partial
import json
import urllib

from eliot import log_call, start_action
import httpx
import rauth
import trio

from trio_util.http_util import TrioHttpSession


RETRY_LIMIT = 10


CONVERSATION_EXPANSIONS = [
    'author_id', 'referenced_tweets.id', 'referenced_tweets.id.author_id', 'entities.mentions.username', 'attachments.poll_ids', 'attachments.media_keys', 'in_reply_to_user_id', 'geo.place_id'
]
CONVERSATION_EXPANSIONS_joined = ','.join(CONVERSATION_EXPANSIONS)

TWEET_FIELDS = ['attachments', 'author_id', 'context_annotations', 'conversation_id', 'created_at', 'entities', 'geo', 'id', 'in_reply_to_user_id', 'lang', 'public_metrics', 'possibly_sensitive', 'referenced_tweets', 'reply_settings', 'source', 'text', 'withheld']   # non_public_metrics, 'organic_metrics', 'promoted_metrics'
TWEET_FIELDS_joined = ','.join(TWEET_FIELDS)

PLACE_FIELDS = ['contained_within', 'country', 'country_code', 'full_name', 'geo', 'name', 'place_type']
PLACE_FIELDS_joined = ','.join(PLACE_FIELDS)

USER_FIELDS = ['created_at', 'description', 'entities', 'id', 'location', 'name', 'pinned_tweet_id', 'profile_image_url', 'protected', 'public_metrics', 'url', 'username', 'verified', 'withheld']
USER_FIELDS_joined = ','.join(USER_FIELDS)


def _urlencode(string):
    # for some reason it wont accept strings without an argument!
    return urllib.parse.urlencode({'a': string})[2:]


class TwitterHttpSession(object):

    def __init__(self, account_dict, http_session):
        self.account_dict = account_dict
        self.http_session = http_session
        self.bearer_token = account_dict['bearer_token']
        self.consumer_key = account_dict.get('consumer_key')
        self.consumer_secret_key = account_dict.get('consumer_secret_key')
        self.app_bearer_token = None

    @staticmethod
    def create_from_account_dict(account_dict):
        """ create aTrioHttpSession and wrap it in a TwitterHttpSession """
        proxy_url, request_lock, limits = None, None, None
        if 'proxy_port' in account_dict:
            proxy_url = f"http://localhost:{account_dict['proxy_port']}"
            limits = httpx.Limits(
                # not sure about max_keepalive_connections value...
                max_connections=1, max_keepalive_connections=2, keepalive_expiry=10
            )
            request_lock = trio.Lock()  # prevent concurrent requests

        http_session = TrioHttpSession(
            ssl=False, verify=False, proxy_url=proxy_url, limits=limits
        )
        http_session.request_lock = request_lock

        return TwitterHttpSession(account_dict, http_session)

    @classmethod
    def create_one(cls):

        with open('/app/api-keys.json') as f:
            api_keys = json.load(f)

        for account_key, account_details in api_keys['twitter'].items():
            return TwitterHttpSession.create_from_account_dict(account_details)

        exit('error: no twitter api keys found')

    def _get_auth_headers(self):
        return {"Authorization": f"Bearer {self.bearer_token}"}

    @log_call(include_args=['method', 'url'], include_result=False)
    async def do_request(self, method, url, headers=None, data=None, auth=None):

        if headers is None:
            headers = self._get_auth_headers()

        for i in range(RETRY_LIMIT):
            try:
                return await _do_request(
                    self.http_session, method, url,
                    headers=headers, data=data, auth=auth
                )
            except (httpx.TimeoutException, httpx.NetworkError):
                if i != RETRY_LIMIT-1:
                    print(f'warning: connection issues, retrying: {method} {url}')
                    sleep_time = 1 if i < 3 else 3
                    await trio.sleep(1)
            except Exception as e:
                import pdb; pdb.set_trace()
                print()
        return 522, None  # connection timed out

    async def get_follower_ids(self, user_id, cursor=None):

        url = f'https://api.twitter.com/1.1/followers/ids.json?user_id={user_id}&stringify_ids=true&count=5000'
        if cursor:
            url = url + '&cursor=' + cursor

        status, resp_obj = await self.do_request('get', url)
        return status, resp_obj

    async def get_friend_ids(self, user_id, cursor=None):

        url = f'https://api.twitter.com/1.1/friends/ids.json?user_id={user_id}&stringify_ids=true&count=5000'
        if cursor:
            url = url + '&cursor=' + cursor

        status, resp_obj = await self.do_request('get', url)
        return status, resp_obj

    async def get_user_timeline(self, user_id, cursor=None, since_id=None):

        url = f"https://api.twitter.com/1.1/statuses/user_timeline.json?user_id={user_id}&count=200&include_rts=true&exclude_replies=false&trim_user=true"
        if cursor:
            url = url + '&max_id=' + str(cursor)
        if since_id:
            url = url + '&since_id=' + str(since_id)

        status, resp_obj = await self.do_request('get', url)
        return status, resp_obj

    async def get_user_likes(self, user_id, since_id=None):

        url = f"https://api.twitter.com/1.1/favorites/list.json?user_id={user_id}&count=200&include_entities=true"
        if since_id:
            url = url + '&since_id=' + str(since_id)

        status, resp_obj = await self.do_request('get', url)
        return status, resp_obj

    async def get_user_info(self, user_ids, screen_names):

        url = f'https://api.twitter.com/1.1/users/lookup.json?include_entities=true'

        if user_ids:
            user_idsJ = ','.join(user_ids)
            url = url + f'&user_id={user_idsJ}'
        if screen_names:
            screen_namesJ = ','.join(screen_names)
            url = url + f'&screen_name={screen_namesJ}'

        status_code, resp_obj = await self.do_request('post', url)
        return status_code, resp_obj

    async def get_app_bearer_token(self):

        if self.app_bearer_token:
            return self.app_bearer_token
        if self.consumer_key is None or self.consumer_secret_key is None:
            return None

        consumer_key = _urlencode(self.consumer_key)
        consumer_secret_key = _urlencode(self.consumer_secret_key)
        bearer_token = f"{consumer_key}:{consumer_secret_key}"
        bearer_token_encoded = base64.b64encode(bearer_token.encode())

        url = 'https://api.twitter.com/oauth2/token'
        headers = {
            "Authorization": b"Basic " + bearer_token_encoded,
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
        }
        status_code, resp_obj = await self.do_request(
            'post', url, headers=headers, data=b"grant_type=client_credentials"
        )
        if status_code != 200:
            return None

        self.app_bearer_token = resp_obj.json()['access_token']
        return self.app_bearer_token

    async def v2__get_conversation_tweets(self, conversation_id, cursor=None):

        url = f"https://api.twitter.com/2/tweets/search/recent?query=conversation_id:{conversation_id}&max_results=100&tweet.fields={TWEET_FIELDS_joined}&expansions={CONVERSATION_EXPANSIONS_joined}&place.fields={PLACE_FIELDS_joined}&user.fields={USER_FIELDS_joined}"

        #url = f"https://api.twitter.com/2/tweets/search/recent?query=conversation_id:{conversation_id}&max_results=100&tweet.fields=in_reply_to_user_id,author_id,text,id,public_metrics,lang&expansions=author_id,in_reply_to_user_id,referenced_tweets.id,entities.mentions.username,created_at,conversation_id"

        if cursor:
            url = url + f"&next_token={cursor}"

        # https://api.twitter.com/2/tweets/search/recent?query=conversation_id:1279940000004973111&tweet.fields=in_reply_to_user_id,author_id,created_at,conversation_id
        token = await self.get_app_bearer_token()
        headers = {"Authorization": f"Bearer {token}"}

        return await self.do_request('get', url, headers=headers)

    async def v2__get_tweets(self, tweet_ids):

        if len(tweet_ids) > 100:
            print(f"warning: too many tweets requested: {len(tweet_ids)}")
            tweet_ids = tweet_ids[:100]

        tweet_ids_str = ','.join(tweet_ids)
        url = f"https://api.twitter.com/2/tweets?ids={tweet_ids_str}&expansions=author_id,referenced_tweets.id,entities.mentions.username&tweet.fields=lang,public_metrics"
        return await self.do_request('get', url)


async def _do_request(http_session, method, url, headers=None, data=None, auth=None):
    # print('doing request: %s' % url[23:60])
    method = method.upper()

    if method == 'GET':
        print(f'GET: {url[:140]}')
        with start_action(action_type=u"get_request"):
            if isinstance(http_session, rauth.session.OAuth1Session):
                get_func = partial(http_session.get, headers=headers, auth=auth)
                resp_obj = await trio.to_thread.run_sync(get_func, url)
                status = resp_obj.status_code
            else:
                status, _, resp_obj = await http_session.get(
                    url, headers=headers, auth=auth
                )
    else:
        assert method == 'POST'
        print(f'POST: {url[:140]}')
        with start_action(action_type=u"post_request"):
            if isinstance(http_session, rauth.session.OAuth1Session):
                data = data or ""  # due to bug in rauth when posting without setting 'data'
                post_func = partial(http_session.post, headers=headers, data=data, auth=auth)
                resp_obj = await trio.to_thread.run_sync(post_func, url)
                status = resp_obj.status_code
            else:
                status, _, resp_obj = await http_session.post(
                    url, headers=headers, data=data, auth=auth
                )

    if status == 429:
        # wait and retry
        print('status 429, waiting 6 minutes: %s' % url)
        await trio.sleep(360)
        with start_action(action_type=u"request_retry"):
            return await _do_request(
                http_session, method, url, headers=headers, data=data, auth=auth
            )

    elif 'x-rate-limit-remaining' in resp_obj.headers:
        val = int(resp_obj.headers['x-rate-limit-remaining'])
        # note: these waits are in addition to the request delay in parent function (usually 60s)
        if val == 1:
            await trio.sleep(40)
        elif val == 0:
            print('x-rate-limit-remaining == 0, waiting 6 minutes: %s' % url)
            await trio.sleep(360)

    return status, resp_obj



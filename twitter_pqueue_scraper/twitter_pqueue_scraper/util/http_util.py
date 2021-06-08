import copy

import httpx
import trio

from trio_util.http_util import TrioHttpSession

from functools import partial

from eliot import log_call, start_action
import httpx
import rauth
import trio


RETRY_LIMIT = 10


class TwitterHttpSession(object):

    def __init__(self, account_dict, http_session):
        self.account_dict = account_dict
        self.http_session = http_session
        self.bearer_token = account_dict['bearer_token']

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

    async def v2__get_conversation_tweets(self, conversation_id):
        url = f"https://api.twitter.com/2/tweets/search/recent?query=conversation_id:{conversation_id}&tweet.fields=in_reply_to_user_id,author_id,text,id,public_metrics,lang&expansions=author_id,in_reply_to_user_id,referenced_tweets.id,entities.mentions.username"

        return await self.do_request('get', url)

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



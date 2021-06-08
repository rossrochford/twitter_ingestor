from collections import defaultdict

from twitter_pqueue_scraper.scrapers.twitter_api_v1.user_info import get_user_info__chunk


async def get_user_likes(twitter_session, user_id, num_pages, since_id=None):

    result_count = num_pages * 200

    likes = []
    while len(likes) <= result_count:

        status, resp_obj = await twitter_session.get_user_likes(user_id, since_id)

        if status != 200:
            print('warning: /1.1/favorites/list.json?user_id=%s gave status: %s' % (user_id, status))
            return False, likes

        _likes = resp_obj.json()
        if not _likes:
            break
        if since_id and since_id == _likes[-1]['id']:
            break
        likes.extend(_likes)
        since_id = likes[-1]['id']

    return True, likes


async def get_longtail_common_liked_users(twitter_session, user_id, follower_thresh=9000):
    """
    Get top 100 users whose tweets have been liked, then filter
    by those with < 10,000 followers
    """
    success, likes = await get_user_likes(twitter_session, user_id, 2000)

    user_counts = defaultdict(int)
    for di in likes:
        _user_id = di['user']['id_str'].strip().lower()
        user_counts[_user_id] += 1

    user_counts = [tup for tup in user_counts.items() if tup[1] >= 7]
    if not user_counts:
        return []
    user_counts.sort(key=lambda tup: tup[1], reverse=True)
    top_100_users = [tup[0] for tup in user_counts[:100]]
    user_info = await get_user_info__chunk(http_session, top_100_users)

    return [
        di['id_str'] for di in user_info if di['followers_count'] <= follower_thresh
    ]

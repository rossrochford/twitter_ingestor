import json

from twitter_pqueue_scraper.ingestion.create_deferred_models import remove_links_and_mentions, get_status_type
from twitter_pqueue_scraper.ingestion.deferred_models import (
    DeferredReplyRel, DeferredTweet, DeferredTwitterProfile,
    DeferredRetweetRel, DeferredProfileMentionedInTweet, dedup_def_objects
)
from twitter_pqueue_scraper.batch_tasks.util import parse_date_str


def _create_deferred_mentions(tweet_di, users_by_id):

    def_objects = []

    mentions = tweet_di.get('entities', {}).get('mentions') or []
    mentions_sn = [di['username'].lower() for di in mentions]

    for screen_name in mentions_sn:
        user_id = users_by_id[screen_name]['id']
        def_objects.append(
            DeferredTwitterProfile(user_id, None, screen_name)
        )
        def_objects.append(
            DeferredProfileMentionedInTweet(
                user_id, None, tweet_di['id'], None
            )
        )
    return def_objects


def _create_reply_rel(tweet_di, conversation_id, conversation_author, missing):

    for ref in tweet_di.get('referenced_tweets', []):

        if ref['type'] == 'replied_to':
            if ref['id'] in missing:
                # note: this means we lose the reply rel (another
                # option wold be to create missing tweets in the DB)
                return None
            return DeferredReplyRel(
                ref['id'], None, tweet_di['id'], None,
                parse_date_str(tweet_di['created_at'])
            )

    if 'in_reply_to_user_id' not in tweet_di:
        # not a reply, could a quoted tweet within a reply
        return None

    if tweet_di['in_reply_to_user_id'] == conversation_author:
        return DeferredReplyRel(
            conversation_id, None, tweet_di['id'], None,
            parse_date_str(tweet_di['created_at'])
        )
    else:
        import pdb; pdb.set_trace()
    return None


def _create_single_deferred_tweet(tweet_di, conversation_id, tweet_type):

    scrape_source = 'recent-search-conversation'

    has_link = bool(tweet_di.get('entities', {}).get('urls'))
    has_text = bool(remove_links_and_mentions(tweet_di))

    def_tweet = DeferredTweet(
        tweet_di['id'], json.dumps(tweet_di), scrape_source,
        tweet_type, has_link, has_text, conversation_id,
        tweet_di['author_id'], None, parse_date_str(tweet_di['created_at'])
    )
    return def_tweet


def create_deferred_profile(user_dict):
    user_id = user_dict['id']
    screen_name = user_dict['username'].lower()
    return DeferredTwitterProfile(user_id, None, screen_name)


def _create_deferred_objects_for_tweet(
    tweet_di, conversation_id, conversation_author, missing,
    tweets_by_id, users_by_id, is_reply
):
    author_id = tweet_di['author_id']
    def_objects = _create_deferred_mentions(tweet_di, users_by_id)

    if is_reply:
        reply_rel = _create_reply_rel(
            tweet_di, conversation_id, conversation_author, missing
        )
        if reply_rel:
            def_objects.append(reply_rel)

    quoted_status_id_str = None
    for di in tweet_di.get('referenced_tweets', []):
        if di['type'] == 'quoted':
            quoted_status_id_str = di['id']
            break

    if quoted_status_id_str is None:
        outer_tweet_type = 'reply' if is_reply else get_status_type(tweet_di)
    else:
        outer_tweet_type = 'reply-with-quote' if is_reply else 'retweet-with-quote'

    def_objects.append(
        _create_single_deferred_tweet(
            tweet_di, conversation_id, outer_tweet_type
        )
    )
    if author_id != conversation_author and author_id in users_by_id:
        def_objects.append(
            create_deferred_profile(users_by_id[author_id])
        )

    if quoted_status_id_str is None or quoted_status_id_str not in tweets_by_id:
        # sometimes inner tweet is missing (e.g. was removed by user)
        return def_objects

    inner_tweet_di = tweets_by_id[quoted_status_id_str]
    def_objects.append(
         _create_single_deferred_tweet(
            inner_tweet_di, conversation_id,
            get_status_type(inner_tweet_di)
        )
    )
    author_id2 = inner_tweet_di['author_id']
    if author_id2 != conversation_author and author_id2 in users_by_id:
        def_objects.append(
            create_deferred_profile(users_by_id[author_id2])
        )

    # note: this isn't quite accurate, modelling a quoted tweet inside a reply as a retweet
    def_objects.append(
        DeferredRetweetRel(
            inner_tweet_di['id'], None, tweet_di['author_id'], None,
            True, tweet_di['id'], parse_date_str(tweet_di['created_at'])
        )
    )
    return def_objects


def _create_def_models(
    conversation_id, tweets_by_id, users_by_id, missing
):
    main_tweet = tweets_by_id[conversation_id]
    conversation_author = tweets_by_id[main_tweet['id']]['author_id']

    def_objects = []

    if conversation_author in users_by_id:
        do = create_deferred_profile(users_by_id[conversation_author])
        def_objects.append(do)

    for id_str, tweet_di in tweets_by_id.items():
        if id_str in missing:
            continue
        is_reply = id_str != conversation_id

        def_objects.extend(
            _create_deferred_objects_for_tweet(
                tweet_di, conversation_id, conversation_author,
                missing, tweets_by_id, users_by_id, is_reply
            )
        )

    return dedup_def_objects(def_objects)


def create_deferred_models__conversation(
    conversation_id, reply_tweets, tweets_included, users, errors
):
    tweets_by_id = {di['id']: di for di in reply_tweets + tweets_included}

    missing_tweets = []
    for di in errors:
        if di['resource_type'] == 'tweet' and 'not-found' in di['type']:
            missing_tweets.append(di['resource_id'])

    for id_str, tweet_di in  tweets_by_id.items():
        for ref in tweet_di.get('referenced_tweets', []):
            if ref['id'] not in tweets_by_id:
                # the usual reason for this (I think?) is when max_pages
                # limit isn't high enough to get every reply
                missing_tweets.append(ref['id'])

    users_by_id = {}
    for di in users:
        di['username'] = sn = di['username'].lower()
        users_by_id[sn] = di
        users_by_id[di['id']] = di

    return _create_def_models(
        conversation_id, tweets_by_id, users_by_id, missing_tweets
    )

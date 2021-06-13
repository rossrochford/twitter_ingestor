from collections import defaultdict

from twitter_pqueue_scraper.ingestion.deferred_models import (
    DeferredTwitterProfile, DeferredTweet, DeferredReplyRel, DeferredRetweetRel,
    DeferredLikeRel, DeferredProfileMentionedInTweet, dedup_def_objects
)
from twitter_pqueue_scraper.util.db_util import (
    get_or_create_by_key, get_or_create_by_multiple_keys
)


TABLE_NAMES = {
    'retweet': 'twitter_retweetrel',
    'reply': 'twitter_replyrel',
    'like': 'twitter_likerel',
    'tweet_mention': 'twitter_profilementionedintweet',
    'profile_mention': 'twitter_profilementionedinprofiledescription'
}
REL_FIELDS = {
    'retweet': ('tweet_id', 'retweeted_by_id'),
    'reply': ('reply_id', 'reply_to_id'),
    'like': ('tweet_id', 'liked_by_id'),
    'tweet_mention': ('tweet_id', 'mentioned_profile_id'),
    'profile_mention': ('profile_id', 'mentioned_profile_id')
}


async def get_or_create_tweets__from_def_tweets(
    worker, def_tweets, authors_by_userid
):
    db_session, Base = worker.db_connection
    Tweet = Base.classes.twitter_tweet

    def_tweets_by_api_id = {dt.tweet_api_id: dt for dt in def_tweets}
    tweet_api_ids = [dt.tweet_api_id for dt in def_tweets]

    tweets_by_pk, tweets_by_api_id, _ = await get_or_create_by_key(
        db_session, Tweet, 'tweet_api_id', tweet_api_ids
    )

    for api_id, tweet_obj in tweets_by_api_id.items():
        def_tweet = def_tweets_by_api_id[api_id]
        update = def_tweet.get_update_values(authors_by_userid)
        for field, value in update.items():
            setattr(tweet_obj, field, value)

    await db_session.commit_async()

    return tweets_by_api_id


async def ingest_relationships__from_def_rels(
    worker, def_rels, rel_type, tweets_by_api_id, authors_by_userid
):
    assert rel_type in ('retweet', 'reply', 'like', 'tweet_mention')

    db_session, Base = worker.db_connection
    ModelClass = getattr(Base.classes, TABLE_NAMES[rel_type])

    value_dicts = {}
    field1, field2 = REL_FIELDS[rel_type]
    for dr in def_rels:
        di = dr.get_update_values(tweets_by_api_id, authors_by_userid)
        key = (di[field1], di[field2])  # use key to prevent duplicates
        value_dicts[key] = di

    value_dicts = [di for (key, di) in value_dicts.items()]

    rel_objects = await get_or_create_by_multiple_keys(
        db_session, ModelClass, value_dicts,
        key_fields=[field1, field2]
    )
    return rel_objects


async def ingest_deferred_models(
    worker, def_objects, parent_author_userid=None
):

    db_session, Base = worker.db_connection
    TwitterProfile = Base.classes.twitter_twitterprofile

    def_objects = dedup_def_objects(def_objects)

    def_profiles = [do for do in def_objects if isinstance(do, DeferredTwitterProfile)]
    def_tweets = [do for do in def_objects if isinstance(do, DeferredTweet)]
    def_tweet_mention_rels = [dr for dr in def_objects if isinstance(dr, DeferredProfileMentionedInTweet)]
    def_retweet_rels = [dr for dr in def_objects if isinstance(dr, DeferredRetweetRel)]
    def_reply_rels = [dr for dr in def_objects if isinstance(dr, DeferredReplyRel)]
    def_like_rels = [dr for dr in def_objects if isinstance(dr, DeferredLikeRel)]

    user_ids = [t.author_user_id for t in def_tweets if t.author_user_id]
    user_ids = user_ids + [p.profile_api_id for p in def_profiles]
    if parent_author_userid:
        user_ids.append(parent_author_userid)

    profiles_by_obj_id, profiles_by_userid, new_profile_ids = await get_or_create_by_key(
        db_session, TwitterProfile, 'user_id', user_ids,
        defaults={'manually_added': False}
    )
    tweets_by_api_id = await get_or_create_tweets__from_def_tweets(
        worker, def_tweets, profiles_by_userid
    )

    if def_retweet_rels:
        await ingest_relationships__from_def_rels(
            worker, def_retweet_rels, 'retweet',
            tweets_by_api_id, profiles_by_userid
        )

    if def_reply_rels:
        await ingest_relationships__from_def_rels(
            worker, def_reply_rels, 'reply', tweets_by_api_id,
            profiles_by_userid
        )

    if def_like_rels:
        await ingest_relationships__from_def_rels(
            worker, def_like_rels, 'like', tweets_by_api_id,
            profiles_by_userid
        )

    if def_tweet_mention_rels:
        await ingest_relationships__from_def_rels(
            worker, def_like_rels, 'tweet_mention',
            tweets_by_api_id, profiles_by_userid
        )

    # note: ProfileMentionedInProfileDescription are not ingested here, they are
    # created directly in: batch_tasks.user_info._ingest_profile_description_mentions

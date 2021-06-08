from twitter_pqueue_scraper.ingestion.create_deferred_models import create_deferred_models
from twitter_pqueue_scraper.ingestion.ingest_deferred_models import ingest_deferred_models


async def ingest_user_likes(worker, user_id, user_likes_data):

    def_profiles, def_tweets, def_rels = [], [], []
    for liked_tweet in user_likes_data:
        _profiles, _tweets, _rels = create_deferred_models(
            worker, user_id, liked_tweet, scenario='user-like'
        )
        def_profiles.extend(_profiles)
        def_tweets.extend(_tweets)
        def_rels.extend(_rels)

    await ingest_deferred_models(
        worker, def_profiles, def_tweets, def_rels, False,
        parent_author_userid=user_id
    )

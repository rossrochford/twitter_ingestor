from twitter_pqueue_scraper.ingestion.create_deferred_models import create_deferred_models
from twitter_pqueue_scraper.ingestion.ingest_deferred_models import ingest_deferred_models


async def ingest_user_timeline(worker, user_id, user_timeline_data):

    def_profiles, def_tweets, def_rels = [], [], []
    for tweet_di in user_timeline_data:
        _profiles, _tweets, _rels = create_deferred_models(
            worker, user_id, tweet_di, scenario=None
        )
        def_profiles.extend(_profiles)
        def_tweets.extend(_tweets)
        def_rels.extend(_rels)

    await ingest_deferred_models(
        worker, def_profiles, def_tweets, def_rels, True,
        parent_author_userid=user_id
    )

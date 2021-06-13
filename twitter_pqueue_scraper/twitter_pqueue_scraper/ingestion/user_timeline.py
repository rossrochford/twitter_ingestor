from twitter_pqueue_scraper.ingestion.create_deferred_models import create_deferred_models
from twitter_pqueue_scraper.ingestion.ingest_deferred_models import ingest_deferred_models


async def ingest_user_timeline(worker, user_id, user_timeline_data):

    def_objects = []
    for tweet_di in user_timeline_data:
        do = create_deferred_models(
            user_id, tweet_di, scenario=None
        )
        def_objects.extend(do)

    await ingest_deferred_models(
        worker, def_objects, parent_author_userid=user_id
    )

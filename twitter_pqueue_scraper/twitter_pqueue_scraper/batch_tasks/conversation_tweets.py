from twitter_pqueue_scraper.ingestion.create_deferred_models_v2 import create_deferred_models__conversation
from twitter_pqueue_scraper.ingestion.ingest_deferred_models import ingest_deferred_models
from twitter_pqueue_scraper.scrapers.twitter_api_v2.conversation_tweets import get_conversation_tweets


async def scrape_conversation_tweets(worker, global_ctx, conversation_item_batch):

    db_session, Base = worker.db_connection

    def_objects = []
    for item in conversation_item_batch:
        reply_tweets, tweets_included, users, errors, status_code = await get_conversation_tweets(
            worker.twitter_session, item.conversation_id
        )
        _def_objects = create_deferred_models__conversation(
            item.conversation_id, reply_tweets, tweets_included, users, errors
        )
        def_objects.extend(_def_objects)

    await ingest_deferred_models(worker, def_objects)

    await db_session.commit_async()

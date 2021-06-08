from twitter_pqueue_scraper.batch_tasks.util import scrape_relationship_ids
from twitter_pqueue_scraper.scrapers.twitter_api_v1.friend_ids import get_friend_ids


async def scrape_friend_ids(worker, global_ctx, profile_batch):
    return await scrape_relationship_ids(
        worker, 'friend_ids', get_friend_ids, profile_batch
    )

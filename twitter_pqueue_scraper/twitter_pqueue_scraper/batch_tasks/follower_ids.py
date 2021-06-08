from twitter_pqueue_scraper.batch_tasks.util import scrape_relationship_ids
from twitter_pqueue_scraper.scrapers.twitter_api_v1.follower_ids import get_follower_ids


async def scrape_follower_ids(worker, global_ctx, profile_batch):
    return await scrape_relationship_ids(
        worker, 'follower_ids', get_follower_ids, profile_batch
    )

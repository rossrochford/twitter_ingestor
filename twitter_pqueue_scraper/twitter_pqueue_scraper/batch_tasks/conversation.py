import re

from twitter_pqueue_scraper.scrapers.twitter_api_v2.conversation_tweets import get_conversation_tweets
from twitter_pqueue_scraper.scrapers.twitter_api_v2.tweets_lookup import get_tweets


MENTION_REGEX = r'@(?P<screen_name>\w+)'


async def scrape_conversation_data(worker, global_ctx, conversation_item_batch):

    twitter_session = worker.twitter_session
    mongodb_cli = worker.mongodb_cli

    # conversation API query only returns replies, we need to fetch the original tweet,
    # scraping conversations in batches will reduce the number of these requests
    tweet_ids = [item.conversation_api_id for item in conversation_item_batch]
    toplevel_tweets = get_tweets(twitter_session, tweet_ids)

    db_updates = {}
    for item in conversation_item_batch:
        resp_obj, _ = await get_conversation_tweets(
            twitter_session, item.conversation_api_id
        )
        if resp_obj is None:
            print(f"warning: failed to get tweets for conversation: {item.conversation_api_id}")
            continue
        json_data = resp_obj.json()
        tweet_data = json_data.get('data', [])
        if item.conversation_api_id in toplevel_tweets:
            tweet_data.insert(0, toplevel_tweets[item.conversation_api_id])

        if len(tweet_data) == 0:
            continue

        author_userids, mentions = _get_conversation_users(tweet_data)
        db_updates[item.conversation_obj_id] = {
            'tweets': tweet_data,
            'author_user_ids': author_userids,
            'mention_screen_names': mentions
        }

    print(f"saving {len(db_updates)} conversation results")
    await mongodb_cli.update_many__multi_field(
        'twitter_conversation', db_updates
    )


async def _get_conversation_users(tweet_data):
    author_userids = [di['author_id'] for di in tweet_data]
    author_userids = [s for s in set(author_userids)]

    # NOTE: mentions are now in tweet_data['entities']['mentions'] so we shouldn't be using regexps here
    mentions = []
    for di in tweet_data:
        for screen_name in re.findall(MENTION_REGEX, di['text']):
            mentions.append(screen_name.lower())
    mentions = [s for s in set(mentions)]

    return author_userids, mentions

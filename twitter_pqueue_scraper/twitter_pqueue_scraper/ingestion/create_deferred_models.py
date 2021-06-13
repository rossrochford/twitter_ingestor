import json
import re

from twitter_pqueue_scraper.ingestion.deferred_models import (
    DeferredTweet, DeferredReplyRel, DeferredRetweetRel, DeferredLikeRel,
    DeferredTwitterProfile, DeferredProfileMentionedInTweet
)
from twitter_pqueue_scraper.batch_tasks.util import parse_date_str


# for tips on how to preprocess tweet text for NLP, see page 4 here: https://arxiv.org/pdf/1708.03994.pdf
def remove_links_and_mentions(tweet_di):

    text = tweet_di['text'].strip()
    if text.startswith('RT @'):
        if text.rstrip() == 'RT @':
            return ''
        try:
            text = text.lstrip('RT @').split(' ', 1)[1]
        except:
            import pdb; pdb.set_trace()
            print()

    # remove mentions
    text = re.sub(r'@\w{1,15}', '', text, flags=re.I).strip()

    # what about hashtags?

    urls = tweet_di.get('entities', {}).get('urls') or []
    # urls = tweet_di['entities']['urls']

    text_no_links = text
    for url_di in urls:
        text_no_links = text_no_links.replace(url_di['url'], '')
    text_no_links = re.sub(re.escape('https://') + r'[^\s]+', ' ', text_no_links).strip()

    for punc in (',', ':', '.', '?', '!'):
        text_no_links = text_no_links.replace(punc, ' ')

    text_no_links = re.sub(r'\s+', ' ', text_no_links).strip()
    text_no_links = text_no_links.strip()

    return text_no_links


def get_status_type(tweet_di):

    text = tweet_di['text']
    urls = tweet_di.get('entities', {}).get('urls') or []

    if len(urls) == 1 and urls[0]['url'] == text:
        return 'link-only-status'

    for media_di in tweet_di.get('entities', {}).get('media', []):
        if media_di['url'] == text:
            return 'media-object-status'  # media_di['type']

    if not urls:
        return 'text-only-status'

    text_without_links = remove_links_and_mentions(tweet_di)

    if not text_without_links:
        return 'link-only-status'  # note: could be more than one link here
    return 'text-with-link'


def get_tweet_scenario(timeline_tweet):
    text = timeline_tweet['text'].strip()

    if timeline_tweet['is_quote_status']:
        return 'retweet-with-quote'
    if timeline_tweet.get('retweeted_status'):
        return 'retweet'     # if text.startswith('RT @'):
    if timeline_tweet['in_reply_to_status_id_str'] is not None:
        # don't use 'in_reply_to_user_id_str' because this gets set when a status
        # begins with an @ mention, even though it's not a reply on a thread.
        return 'reply'

    return get_status_type(timeline_tweet)


def _create_deferred_replyto_tweet(tweet_di):
    return DeferredTweet(
        tweet_di['in_reply_to_status_id_str'], None, None, None,
        None, None, None, tweet_di['in_reply_to_user_id_str'], None, None
    )


def _create_deferred_blank_quoted_tweet(id_str):
    return DeferredTweet(
        id_str, None, None, None, None, None, None, None, None, None
    )


def _create_deferred_tweet(tweet_di, scrape_source):

    if tweet_di['in_reply_to_status_id_str']:
        tweet_type = 'reply'
    elif tweet_di['is_quote_status']:
        tweet_type = 'quote'
    else:
        tweet_type = 'status'

    has_link = bool(tweet_di.get('entities', {}).get('urls'))
    has_text = bool(remove_links_and_mentions(tweet_di))

    def_tweet = DeferredTweet(
        tweet_di['id_str'], json.dumps(tweet_di), scrape_source,
        tweet_type, has_link, has_text, None, tweet_di['user']['id_str'],
        None, parse_date_str(tweet_di['created_at'])
    )
    return def_tweet


def _create_deferred_mentions(tweet_di):

    profiles, mention_rels = [], []

    for profile_di in tweet_di.get('entities', {}).get('user_mentions', []):
        profiles.append(
            DeferredTwitterProfile(profile_di['id_str'], None, None)
        )
        mention_rels.append(
            DeferredProfileMentionedInTweet(
                profile_di['id_str'], None, tweet_di['id_str'], None
            )
        )
    return profiles, mention_rels


def create_deferred_models(user_id, tweet_di, scenario=None):

    if scenario is None:
        scenario = get_tweet_scenario(tweet_di)

    def_profiles, def_mention_rels = _create_deferred_mentions(tweet_di)

    if scenario == 'user-like':
        def_tweet = _create_deferred_tweet(tweet_di, 'user-like')
        def_like_rel = DeferredLikeRel(
            def_tweet.tweet_api_id, None, user_id, None,
            tweet_di['id_str'], parse_date_str(tweet_di['created_at'])
        )
        return def_profiles + [def_tweet, def_like_rel] + def_mention_rels

    if scenario == 'retweet':
        def_tweet = _create_deferred_tweet(
            tweet_di['retweeted_status'], 'user-timeline-retweet'
        )
        def_retweet_rel = DeferredRetweetRel(
            def_tweet.tweet_api_id, None, user_id, None,
            tweet_di['is_quote_status'], tweet_di['id_str'],
            parse_date_str(tweet_di['created_at'])
        )
        return def_profiles + [def_tweet, def_retweet_rel] + def_mention_rels

    if scenario == 'retweet-with-quote':

        if 'quoted_status' in tweet_di:
            def_outer_tweet = _create_deferred_tweet(tweet_di, 'user-timeline')
            def_inner_tweet = _create_deferred_tweet(
                tweet_di['quoted_status'], 'user-timeline-quote'
            )
            #  "inner-tweet quote-tweeted by user_id"
            def_retweet_rel = DeferredRetweetRel(
                def_inner_tweet.tweet_api_id, None, user_id, None,
                tweet_di['is_quote_status'], tweet_di['id_str'],
                parse_date_str(tweet_di['created_at'])
            )
            return def_profiles + [def_outer_tweet, def_inner_tweet, def_retweet_rel] + def_mention_rels

        if 'quoted_status_id_str' in tweet_di:
            # occurs when quoted status is "unavailable" and I've also found it to
            # happen when the user (whose timeline we're scraping) is quoting themselves?
            # (in which case it this blank tweet should get merged during deduplication)
            def_outer_tweet = _create_deferred_tweet(tweet_di, 'user-timeline')
            def_inner_tweet = _create_deferred_blank_quoted_tweet(
                tweet_di['quoted_status_id_str']
            )
            #  "inner-tweet quote-tweeted by user_id"
            def_retweet_rel = DeferredRetweetRel(
                def_inner_tweet.tweet_api_id, None, user_id, None,
                tweet_di['is_quote_status'], tweet_di['id_str'],
                parse_date_str(tweet_di['created_at'])
            )
            return def_profiles + [def_outer_tweet, def_inner_tweet, def_retweet_rel] + def_mention_rels

        else:
            # when quoted tweet is unavailable, I'm not sure why quoted_status_id_str exists only sometimes?
            def_outer_tweet = _create_deferred_tweet(tweet_di, 'user-timeline')
            return def_profiles + [def_outer_tweet] + def_mention_rels

    if scenario == 'reply':

        if tweet_di['in_reply_to_status_id_str'] is None:
            import pdb; pdb.set_trace()  # should never get here?

        def_replyto_tweet = _create_deferred_replyto_tweet(tweet_di)
        def_reply_tweet = _create_deferred_tweet(tweet_di, 'user-timeline')
        def_reply_rel = DeferredReplyRel(
            tweet_di['in_reply_to_status_id_str'], None,
            tweet_di['id_str'], None, parse_date_str(tweet_di['created_at'])
        )
        return def_profiles + [def_replyto_tweet, def_reply_tweet, def_reply_rel] + def_mention_rels

    else:
        # standalone status
        def_tweet = _create_deferred_tweet(tweet_di, 'user-timeline')
        return def_profiles + [def_tweet] + def_mention_rels

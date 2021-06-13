from collections import defaultdict
import dataclasses
import datetime
from typing import Union
import uuid


@dataclasses.dataclass
class DeferredTwitterProfile:
    profile_api_id: Union[str, None]  # user_id
    profile_id: Union[str, None]  # db obj id
    screen_name: Union[str, None]

    @classmethod
    def get_fields(cls):
        return [dim.name for dim in dataclasses.fields(cls)]

    @property
    def user_id(self):
        return self.profile_api_id

    @property
    def id_key(self):
        return 'DeferredTwitterProfile:' + (self.profile_api_id or self.screen_name)


@dataclasses.dataclass
class DeferredTweet:
    tweet_api_id: Union[str, None]
    json_data: Union[str, None]
    scrape_source: Union[str, None]
    tweet_type: Union[str, None]
    has_link: Union[bool, None]
    has_text: Union[bool, None]
    conversation_id: Union[str, None]
    author_user_id: Union[str, None]
    author_id: Union[int, None]  # author: Union[TwitterProfile, None]
    publish_datetime: Union[datetime.datetime, None]

    @classmethod
    def get_fields(cls):
        return [dim.name for dim in dataclasses.fields(cls)]

    @property
    def id_key(self):
        return 'DeferredTweet:' + self.tweet_api_id

    def get_update_values(self, authors_by_userid):
        di = {fn: getattr(self, fn) for fn in self.get_fields()}
        di = {k: v for (k, v) in di.items() if v is not None}
        if di.get('author_user_id'):
            di['author_id'] = authors_by_userid[di['author_user_id']].id
        if 'author_user_id' in di:
            del di['author_user_id']
        return di


@dataclasses.dataclass
class DeferredRetweetRel:
    tweet_api_id: Union[str, None]
    tweet_id: Union[int, None]  # tweet: Union[Tweet, None]
    retweeted_by_user_id: Union[str, None]
    retweeted_by_id: Union[int, None]  # retweeted_by: Union[TwitterProfile, None]
    is_quote: Union[bool, None]
    retweet_api_id: Union[str, None]  # id of the tweet object, could be for example a reply that quotes a tweet
    retweet_datetime: Union[datetime.datetime, None]

    @classmethod
    def get_fields(cls):
        return [dim.name for dim in dataclasses.fields(cls)]

    @property
    def id_key(self):
        return 'DeferredRetweetRel:' + self.tweet_api_id + self.retweeted_by_user_id + (self.retweet_api_id or '')

    def get_update_values(self, tweets_by_api_id, authors_by_userid):
        di = {fn: getattr(self, fn) for fn in self.get_fields()}
        di = {k: v for (k, v) in di.items() if v is not None}

        di['tweet_id'] = tweets_by_api_id[di['tweet_api_id']].id
        del di['tweet_api_id']

        di['retweeted_by_id'] = authors_by_userid[di['retweeted_by_user_id']].id
        del di['retweeted_by_user_id']
        return di


@dataclasses.dataclass
class DeferredProfileMentionedInTweet:
    mentioned_profile_api_id: str
    mentioned_profile_id: [str, None]
    tweet_api_id: str
    tweet_id: Union[int, None]

    @classmethod
    def get_fields(cls):
        return [dim.name for dim in dataclasses.fields(cls)]

    @property
    def id_key(self):
        return self.mentioned_profile_api_id + self.tweet_api_id

    def get_update_values(self, tweets_by_api_id, authors_by_userid):
        di = {fn: getattr(self, fn) for fn in self.get_fields()}
        di = {k: v for (k, v) in di.items() if v is not None}

        di['tweet_id'] = tweets_by_api_id[di['tweet_api_id']].id
        del di['tweet_api_id']

        di['mentioned_profile_id'] = authors_by_userid[di['mentioned_profile_api_id']].id
        del di['mentioned_profile_api_id']
        return di


# NOTE: this never gets used, see: _ingest_profile_description_mentions()
@dataclasses.dataclass
class DeferredProfileMentionedInProfileDescription:
    profile_api_id: str
    profile_id: Union[int, None]
    mentioned_by_api_id: str
    mentioned_by_id: [str, None]

    @classmethod
    def get_fields(cls):
        return [dim.name for dim in dataclasses.fields(cls)]

    @property
    def id_key(self):
        return 'DeferredProfileMentionedInProfileDescription:' + self.mentioned_profile_api_id + self.profile_api_id

    def get_update_values(self, tweets_by_api_id, authors_by_userid):
        di = {fn: getattr(self, fn) for fn in self.get_fields()}
        di = {k: v for (k, v) in di.items() if v is not None}

        di['profile_id'] = authors_by_userid[di['profile_api_id']].id
        del di['profile_api_id']

        di['mentioned_by_id'] = authors_by_userid[di['mentioned_by_api_id']].id
        del di['mentioned_by_api_id']
        return di


@dataclasses.dataclass
class DeferredReplyRel:
    reply_to_api_id: Union[str, None]
    reply_to_id: Union[int, None]  # reply_to: Union[Tweet, None]
    reply_api_id: Union[str, None]
    reply_id: Union[int, None]  # reply: Union[Tweet, None]
    reply_datetime: Union[datetime.datetime, None]

    @classmethod
    def get_fields(cls):
        return [dim.name for dim in dataclasses.fields(cls)]

    @property
    def id_key(self):
        return 'DeferredReplyRel:' + self.reply_to_api_id + self.reply_api_id # + (self.reply_id or '')

    def get_update_values(self, tweets_by_api_id, authors_by_userid):
        di = {fn: getattr(self, fn) for fn in self.get_fields()}
        di = {k: v for (k, v) in di.items() if v is not None}

        di['reply_to_id'] = tweets_by_api_id[di['reply_to_api_id']].id
        del di['reply_to_api_id']
        di['reply_id'] = tweets_by_api_id[di['reply_api_id']].id
        del di['reply_api_id']
        return di


@dataclasses.dataclass
class DeferredLikeRel:
    tweet_api_id: Union[str, None]
    tweet_id: Union[int, None]  # tweet: Union[Tweet, None]
    liked_by_user_id: Union[str, None]
    liked_by_id: Union[int, None]  # liked_by: Union[TwitterProfile, None]
    like_api_id: Union[str, None]
    like_datetime: Union[datetime.datetime, None]

    @classmethod
    def get_fields(cls):
        return [dim.name for dim in dataclasses.fields(cls)]

    @property
    def id_key(self):
        return 'DeferredLikeRel:' + self.tweet_api_id + self.liked_by_user_id # + (self.like_api_id or '')

    def get_update_values(self, tweets_by_api_id, authors_by_userid):
        di = {fn: getattr(self, fn) for fn in self.get_fields()}
        di = {k: v for (k, v) in di.items() if v is not None}

        di['tweet_id'] = tweets_by_api_id[di['tweet_api_id']].id
        del di['tweet_api_id']

        di['liked_by_id'] = authors_by_userid[di['liked_by_user_id']].id
        del di['liked_by_user_id']
        return di


def dedup_def_objects(def_objects):

    def _merge_objects(def_objects_list):
        Cls = def_objects_list[0].__class__
        args = []
        for field in Cls.get_fields():
            value = None
            for dt in def_objects_list:
                if getattr(dt, field) is not None:
                    value = getattr(dt, field)
                    break
            args.append(value)
        return Cls(*args)

    objects_by_id = defaultdict(list)
    for def_obj in def_objects:
        objects_by_id[def_obj.id_key].append(def_obj)

    objects_deduped = []
    for id, _def_objects in objects_by_id.items():
        if len(_def_objects) > 1:
            objects_deduped.append(_merge_objects(_def_objects))
            continue
        objects_deduped.append(_def_objects[0])

    return objects_deduped

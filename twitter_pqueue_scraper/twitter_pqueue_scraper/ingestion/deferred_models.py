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


@dataclasses.dataclass
class DeferredTweet:
    tweet_api_id: Union[str, None]
    json_data: Union[str, None]
    scrape_source: Union[str, None]
    tweet_type: Union[str, None]
    has_link: Union[bool, None]
    has_text: Union[bool, None]
    author_user_id: Union[str, None]
    author_id: Union[int, None]  # author: Union[TwitterProfile, None]
    publish_datetime: Union[datetime.datetime, None]

    @classmethod
    def get_fields(cls):
        return [dim.name for dim in dataclasses.fields(cls)]

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
    retweet_api_id: Union[str, None]
    retweet_datetime: Union[datetime.datetime, None]

    @classmethod
    def get_fields(cls):
        return [dim.name for dim in dataclasses.fields(cls)]

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

    def get_update_values(self, tweets_by_api_id, authors_by_userid):
        di = {fn: getattr(self, fn) for fn in self.get_fields()}
        di = {k: v for (k, v) in di.items() if v is not None}

        di['tweet_id'] = tweets_by_api_id[di['tweet_api_id']].id
        del di['tweet_api_id']

        di['mentioned_profile_id'] = authors_by_userid[di['mentioned_profile_api_id']].id
        del di['mentioned_profile_api_id']
        return di


@dataclasses.dataclass
class DeferredProfileMentionedInProfileDescription:
    mentioned_profile_api_id: str
    mentioned_profile_id: [str, None]
    profile_api_id: str
    profile_id: Union[int, None]

    @classmethod
    def get_fields(cls):
        return [dim.name for dim in dataclasses.fields(cls)]

    def get_update_values(self, tweets_by_api_id, authors_by_userid):
        di = {fn: getattr(self, fn) for fn in self.get_fields()}
        di = {k: v for (k, v) in di.items() if v is not None}

        di['profile_id'] = authors_by_userid[di['profile_api_id']].id
        del di['profile_api_id']

        di['mentioned_profile_id'] = authors_by_userid[di['mentioned_profile_api_id']].id
        del di['mentioned_profile_api_id']
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

    def get_update_values(self, tweets_by_api_id, authors_by_userid):
        di = {fn: getattr(self, fn) for fn in self.get_fields()}
        di = {k: v for (k, v) in di.items() if v is not None}

        di['tweet_id'] = tweets_by_api_id[di['tweet_api_id']].id
        del di['tweet_api_id']

        di['liked_by_id'] = authors_by_userid[di['liked_by_user_id']].id
        del di['liked_by_user_id']
        return di

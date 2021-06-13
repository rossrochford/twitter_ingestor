from collections import defaultdict
from datetime import timedelta
import json
import uuid

from django.db.models import Q
from django.db import models, transaction
from util_shared.datetime_utils import get_utc_now

from .util.model_util import merge_uniquetogether_rels


WORK_TYPE_CHOICES = [
    'user_info',
    'user_timeline',
    'user_likes',
    'friend_ids',
    'follower_ids',
    'conversion_tweets',
]
WORK_TYPE_CHOICES = [(s, s) for s in WORK_TYPE_CHOICES]


TWEET_SOURCES = [
    'user-timeline',  # this may be a status or reply
    'user-timeline-retweet',
    'user-timeline-quote',
    'user-like'
]
TWEET_SOURCES = [(s, s) for s in TWEET_SOURCES]

TWEET_TYPES = ['status', 'reply', 'quote']
TWEET_TYPES = [(s, s) for s in TWEET_TYPES]

MONTH_NAMES = {
    1: 'jan',
    2: 'feb',
    3: 'mar',
    4: 'apr',
    5: 'may',
    6: 'jun',
    7: 'jul',
    8: 'aug',
    9: 'sep',
    10: 'oct',
    11: 'nov',
    12: 'dec'
}

#API_SERVICES = ['azure', 'twitter']
#_API_SERVICE_CHOICES = [(s, s) for s in API_SERVICES]

#ENDPOINTS = [
#     'azure:named-entity-recognition'
#]
#_ENDPOINT_CHOICES = [(s, s) for s in ENDPOINTS]

PERIOD_DURATIONS = {
    'minute': timedelta(seconds=60),
    'quarter-hour': timedelta(minutes=15),
    'hour': timedelta(hours=1),
    'day': timedelta(days=1),
    'week': timedelta(days=7),
    'month': timedelta(days=28)
}
_PERIOD_DURATION_CHOICES = [(k, k) for k in PERIOD_DURATIONS.keys()]


ENDPOINT_QUOTAS = {
    'azure': [
        ('azure:named-entity-recognition', 'day', 500)
    ],
    'twitter': [
        ('twitter:user-info', 'quarter-hour', 300),
        ('twitter:user-timeline', 'quarter-hour', 1500),
        ('twitter:user-timeline', 'day', 100000),  # user-timeline has two simultaneously quota-limits

        # note: the v2 API has a project-level cap of tweets, shared by multiple endpoints, of 500,000/month
    ]
}

_ENDPOINT_CHOICES = []
_API_SERVICE_CHOICES = []

for service_slug, endpoint_tuples in ENDPOINT_QUOTAS.items():
    _API_SERVICE_CHOICES.append((service_slug, service_slug))
    for (endpoint_slug, duration, units) in endpoint_tuples:
        _ENDPOINT_CHOICES.append((endpoint_slug, endpoint_slug))



class ApiQuotaPeriod(models.Model):

    service_slug = models.CharField(max_length=99, choices=_API_SERVICE_CHOICES)
    endpoint_slug = models.CharField(max_length=99, choices=_ENDPOINT_CHOICES)

    account_slug = models.CharField(max_length=99)

    start_datetime = models.DateTimeField()
    end_datetime = models.DateTimeField()
    duration_slug = models.CharField(
        max_length=99, choices=_PERIOD_DURATION_CHOICES
    )

    units_remaining = models.IntegerField()

    def get_dict(self):
        return {
            'pk': self.pk,
            'service_slug': self.service_slug,
            'endpoint_slug': self.endpoint_slug,
            'account_slug': self.account_slug,

            'start_datetime': self.start_datetime,
            'end_datetime': self.end_datetime,
            'duration': self.period_duration,

            'units_remaining': self.units_remaining
        }


class TagCategory(models.Model):

    slug = models.CharField(max_length=199, unique=True)
    tags = models.ManyToManyField('Tag', related_name='categories')

    def __str__(self):
        return f'TagCategory: {self.slug}'


class Tag(models.Model):

    slug = models.CharField(max_length=199, unique=True)

    def __str__(self):
        return f'Tag: {self.slug}'

    @classmethod
    def get_profiles_with_tags(cls, tag_slugs, available_only=True):
        tag_slugs = [s.strip().lower() for s in tag_slugs]
        profiles = []
        for tag in Tag.objects.filter(slug__in=tag_slugs).prefetch_related('profiles'):
            if available_only:
                _profiles = [p for p in tag.profiles.all() if p.is_available in (None, True)]
            else:
                _profiles = [p for p in tag.profiles.all()]

            profiles.extend(_profiles)
        return profiles


class TwitterProfileTagRel(models.Model):

    twitter_profile = models.ForeignKey('TwitterProfile', on_delete=models.CASCADE)
    tag = models.ForeignKey('Tag', on_delete=models.CASCADE)

    class Meta:
        unique_together = ('twitter_profile', 'tag')

    def __str__(self):
        return f"{self.tag.slug} -> {self.twitter_profile.screen_name_or_userid}"

    @classmethod
    def remove_profiles(cls, profiles_to_remove):
        TwitterProfileTagRel.objects.filter(twitter_profile_id__in=to_remove).delete()

    @classmethod
    def merge_profiles(cls, profiles_to_merge):
        merge_uniquetogether_rels(
            cls, 'twitter_profile_id', 'tag_id', profiles_to_merge
        )


class ProfileFollowsProfileRel(models.Model):

    source = models.ForeignKey(
        'TwitterProfile', on_delete=models.CASCADE,
        related_name='following_rels'
    )
    dest = models.ForeignKey(
        'TwitterProfile', on_delete=models.CASCADE,
        related_name='followed_by_rels'
    )

    class Meta:
        unique_together = ('source', 'dest')

    def __str__(self):
        return f"{self.source.screen_name_or_userid} -> {self.dest.screen_name_or_userid}"

    @classmethod
    def remove_profiles(cls, profiles_to_remove):
        ProfileFollowsProfileRel.objects.filter(source_id__in=to_remove).delete()
        ProfileFollowsProfileRel.objects.filter(dest_id__in=to_remove).delete()

    @classmethod
    def merge_profiles(cls, profiles_to_merge):
        merge_uniquetogether_rels(
            cls, 'source_id', 'dest_id', profiles_to_merge
        )
        merge_uniquetogether_rels(
            cls, 'dest_id', 'source_id', profiles_to_merge
        )


class TwitterProfile(models.Model):

    screen_name = models.CharField(max_length=60, blank=True, null=True)
    user_id = models.CharField(
        max_length=60, blank=True, null=True, unique=True, primary_key=False
    )

    is_available = models.BooleanField(blank=True, null=True)
    manually_added = models.BooleanField(blank=True, null=True)

    user_info = models.TextField(blank=True, null=True)
    user_info_prev_scrape_attempt = models.DateTimeField(blank=True, null=True)
    user_info_prev_scrape_success = models.DateTimeField(blank=True, null=True)
    user_info_prev_status_code = models.IntegerField(blank=True, null=True)

    user_timeline_since_id = models.CharField(max_length=60, blank=True, null=True)
    user_timeline_latest_tweet_datetime = models.DateTimeField(blank=True, null=True)
    user_timeline_prev_scrape_attempt = models.DateTimeField(blank=True, null=True)
    user_timeline_prev_scrape_success = models.DateTimeField(blank=True, null=True)
    user_timeline_prev_status_code = models.IntegerField(blank=True, null=True)

    user_likes_since_id = models.CharField(max_length=60, blank=True, null=True)
    user_likes_prev_scrape_attempt = models.DateTimeField(blank=True, null=True)
    user_likes_prev_scrape_success = models.DateTimeField(blank=True, null=True)

    follower_ids_fully_scraped = models.BooleanField(blank=True, null=True)
    follower_ids_cursor = models.CharField(max_length=60, blank=True, null=True)
    follower_ids_prev_scrape_attempt = models.DateTimeField(blank=True, null=True)
    follower_ids_prev_scrape_success = models.DateTimeField(blank=True, null=True)
    follower_ids_prev_status_code = models.IntegerField(blank=True, null=True)

    friend_ids_fully_scraped = models.BooleanField(blank=True, null=True)
    friend_ids_cursor = models.CharField(max_length=60, blank=True, null=True)
    friend_ids_prev_scrape_attempt = models.DateTimeField(blank=True, null=True)
    friend_ids_prev_scrape_success = models.DateTimeField(blank=True, null=True)
    friend_ids_prev_status_code = models.IntegerField(blank=True, null=True)

    # tags_json = models.TextField(default='[]')
    tags = models.ManyToManyField(
        Tag, through=TwitterProfileTagRel, related_name='profiles'
    )
    follows = models.ManyToManyField(
        'TwitterProfile', through=ProfileFollowsProfileRel
    )

    @property
    def admin_choice_display(self):
        user_str = self.screen_name or self.user_id or 'UNKNOWN_USER'
        id_short = str(self._id)[:6]
        return '%s - %s' % (id_short, user_str)

    def __str__(self):
        return f'TwitterProfile {self.screen_name_or_userid}'

    @property
    def screen_name_or_userid(self):  # note: only for display purposes
        if self.screen_name:
            return f'@{self.screen_name}'
        return self.user_id or 'UNKNOWN_USER'

    @property
    def display_name(self):
        if self.user_info:
            di = json.loads(self.user_info)
            return di['name']
        return self.screen_name_or_userid

    @property
    def profile_image_url(self):
        if self.user_info:
            di = json.loads(self.user_info)
            return di['profile_image_url_https']
        return None

    @property
    def is_due_userinfo_scrape(self):

        if self.user_info_prev_scrape_attempt:
            now = get_utc_now()
            if (now - self.user_info_prev_scrape_attempt).days > 14:
                return True

        if self.user_info is None:
            return True
        return False

    def is_valid_for_workload(self, work_type, dates):
        now, week_ago, three_months_ago = dates

        if not work_type.startswith('user_info'):
            if self.is_available is False:
                # todo: if user_info_prev_scrape_success < three_months_ago, schedule a recheck
                return False
            if self.user_info is None:
                return False

        if work_type == 'user_timeline':
            if self.user_timeline is None:
                return True
            if self.user_timeline_prev_scrape_success is None or self.user_timeline_prev_scrape_success < week_ago:
                return True
            return False

        if work_type == 'user_likes':
            if self.user_likes is None:
                return True
            if self.user_likes_prev_scrape_success is None or self.user_likes_prev_scrape_success < week_ago:
                return True
            return False

        if work_type == 'friend_ids':
            if self.friend_ids is None:
                return True
            if self.friend_ids_prev_scrape_success is None or self.friend_ids_prev_scrape_success < three_months_ago:
                return True
            return False

        if work_type == 'follower_ids':
            if self.follower_ids is None:
                return True
            if self.follower_ids_prev_scrape_success is None or self.follower_ids_prev_scrape_success < three_months_ago:
                return True
            return False

        print('warning: is_valid_for_workload() unknown scenario')
        return False
    '''
            # original queries:
            'user_timeline': profiles.filter(
                Q(user_timeline__isnull=True) | Q(user_timeline_prev_scrape_success__lt=week_ago)
            ),
            'user_likes': profiles.filter(
                Q(user_likes__isnull=True) | Q(user_likes_last_scraped__lt=week_ago)
            ),
            'friend_ids': profiles.filter(
                Q(friend_ids__isnull=True) |
                Q(friend_ids_last_scraped__lt=three_months_ago)
            ),
            'friend_ids2': profiles.filter(
                friend_ids__isnull=False, friend_ids_fully_scraped=False
            ),
            'follower_ids': profiles.filter(
                Q(follower_ids__isnull=True) |
                Q(friend_ids_last_scraped__lt=three_months_ago)
            ),
            'follower_ids2': profiles.filter(
                follower_ids__isnull=False, follower_ids_fully_scraped=False
            )
    '''

class Tweet(models.Model):

    tweet_api_id = models.CharField(max_length=40)
    json_data = models.TextField(blank=True, null=True)
    scrape_source = models.CharField(  # describes where json_data was fetched from
        max_length=40, blank=True, null=True, choices=TWEET_SOURCES
    )
    tweet_type = models.CharField(  # nullable because reply-to tweets only have a tweet_api_id and an author
        max_length=40, choices=TWEET_TYPES, blank=True, null=True
    )
    has_link = models.BooleanField(null=True, blank=True)
    has_text = models.BooleanField(null=True, blank=True)

    author = models.ForeignKey(  # probably will never be null in practice?
        TwitterProfile, on_delete=models.CASCADE, blank=True, null=True
    )
    conversation_id = models.CharField(max_length=40, blank=True, null=True)
    publish_datetime = models.DateTimeField(blank=True, null=True)

    @classmethod
    def remove_profiles(cls, profiles_to_remove):
        Tweet.objects.filter(author_id__in=profiles_to_remove).delete()

    @classmethod
    def merge_profiles(self, profiles_to_merge):
        to_remove = [tup[1] for tup in profiles_to_merge]

        tweets = Tweet.objects.filter(author_id__in=to_remove)
        tweets_by_author_id = defaultdict(list)
        for obj in tweets:
            tweets_by_author_id[obj.author_id].append(obj)

        with transaction.atomic():
            for id_to_keep, id_to_remove in profiles_to_merge:
               for  tweet_obj in tweets_by_author_id[id_to_remove]:
                    tweet_obj.author_id = id_to_keep
                    tweet_obj.save()

    @property
    def text(self):
        if self.json_data is None:
            return 'NONE'
        try:
            di = json.loads(self.json_data)
            return di['text'].strip()
        except:
            import pdb; pdb.set_trace()
            return 'EXCEPTION-PARSING-JSON-DATA'

    def __str__(self):
        author_name = self.author.display_name if self.author else 'UNKNOWN'
        return f"{author_name}: {self.text}"

    @property
    def published_datetime_display(self):
        if not self.publish_datetime:
            return 'UNKNOWN_PUBLISHED_DATE'
        dt = self.publish_datetime
        month_name = MONTH_NAMES[dt.month]
        month_name = month_name[0].upper() + month_name[1:]
        year = str(dt.year)[2:]
        return f'{month_name} {year}'

    @property
    def favorite_count(self):
        if not self.json_data:
            return '?'
        di = json.loads(self.json_data)
        return di['favorite_count']

    @property
    def retweet_count(self):
        if not self.json_data:
            return '?'
        di = json.loads(self.json_data)
        return di['retweet_count']


class ProfileMentionedInTweet(models.Model):

    mentioned_profile = models.ForeignKey(
        'TwitterProfile', on_delete=models.CASCADE,
        related_name='mentioned_by_tweets'
    )
    tweet = models.ForeignKey(
        'Tweet', on_delete=models.CASCADE,
        related_name='tweet_mentions'
    )

    class Meta:
        unique_together = ('mentioned_profile', 'tweet')

    @classmethod
    def remove_profiles(cls, profiles_to_remove):
        ProfileMentionedInTweet.objects.filter(mentioned_profile_id__in=profiles_to_remove).delete()

    @classmethod
    def merge_profiles(cls, profiles_to_merge):
        merge_uniquetogether_rels(
            cls, 'mentioned_profile_id', 'tweet_id', profiles_to_merge
        )


class ProfileMentionedInProfileDescription(models.Model):

    profile = models.ForeignKey(
        'TwitterProfile', on_delete=models.CASCADE,
        related_name='mentioned_by_profile_rels'
    )
    mentioned_by = models.ForeignKey(
        'TwitterProfile', on_delete=models.CASCADE,
        related_name='profile_description_mention_rels'
    )

    class Meta:
        unique_together = ('profile', 'mentioned_by')

    @classmethod
    def remove_profiles(cls, profiles_to_remove):
        ProfileMentionedInProfileDescription.objects.filter(
            profile_id__in=profiles_to_remove
        ).delete()
        ProfileMentionedInProfileDescription.objects.filter(
            mentioned_by_id__in=profiles_to_remove
        ).delete()

    @classmethod
    def merge_profiles(cls, profiles_to_merge):
        merge_uniquetogether_rels(
            cls, 'profile_id', 'mentioned_by_id', profiles_to_merge
        )
        merge_uniquetogether_rels(
            cls, 'mentioned_by_id', 'profile_id', profiles_to_merge
        )


class RetweetRel(models.Model):

    tweet = models.ForeignKey('Tweet', on_delete=models.CASCADE)

    # note: we're only storing a few fields here, not the full api json
    retweeted_by = models.ForeignKey('TwitterProfile', on_delete=models.CASCADE)
    is_quote = models.BooleanField()
    retweet_api_id = models.CharField(max_length=40, blank=True, null=True)
    retweet_datetime = models.DateTimeField(blank=True, null=True)

    @classmethod
    def remove_profiles(cls, profiles_to_remove):
        RetweetRel.objects.filter(retweeted_by_id__in=profiles_to_remove).delete()

    @classmethod
    def merge_profiles(self, profiles_to_merge):
        to_remove = [tup[1] for tup in profiles_to_merge]

        retweet_rels = RetweetRel.objects.filter(retweeted_by_id__in=to_remove)
        rels_by_retweeter_id = defaultdict(list)
        for rel in retweet_rels:
            rels_by_retweeter_id[rel.retweeted_by_id].append(rel)

        with transaction.atomic():
            for id_to_keep, id_to_remove in profiles_to_merge:
                for rel in rels_by_retweeter_id[id_to_remove]:
                    rel.retweeted_by_id = id_to_keep
                    rel.save()


class ReplyRel(models.Model):

    reply_to = models.ForeignKey(
        'Tweet', on_delete=models.CASCADE, related_name='reply_rels'
    )
    reply = models.ForeignKey(
        'Tweet', on_delete=models.CASCADE, related_name='thread_rels'
    )

    reply_datetime = models.DateTimeField()


class LikeRel(models.Model):

    tweet = models.ForeignKey('Tweet', on_delete=models.CASCADE)

    # note: we're only storing a few fields here, the json data will get stored on the Tweet itself
    liked_by = models.ForeignKey('TwitterProfile', on_delete=models.CASCADE)
    like_api_id = models.CharField(max_length=40, blank=True, null=True)
    like_datetime = models.DateTimeField(blank=True, null=True)

    @classmethod
    def remove_profiles(cls, profiles_to_remove):
        LikeRel.objects.filter(liked_by_id__in=profiles_to_remove).delete()

    @classmethod
    def merge_profiles(self, profiles_to_merge):
        to_remove = [tup[1] for tup in profiles_to_merge]

        like_rels = LikeRel.objects.filter(liked_by_id__in=to_remove)
        rels_by_likedby_id = defaultdict(list)
        for rel in like_rels:
            rels_by_likedby_id[rel.liked_by_id].append(rel)

        with transaction.atomic():
            for id_to_keep, id_to_remove in profiles_to_merge:
                for rel in rels_by_likedby_id[id_to_remove]:
                    rel.liked_by_id = id_to_keep
                    rel.save()

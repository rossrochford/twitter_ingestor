import datetime
import json
import logging
import os
import time

from django.contrib import messages
from django.core.exceptions import SuspiciousOperation
from django.http import HttpResponseBadRequest, HttpResponse, JsonResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from django.views.decorators.http import require_http_methods
from django.views.generic.edit import FormView

import redis
from util_shared.datetime_utils import get_utc_now

from .forms import (
    SelectTagsForm, SelectUserInfoActionForm, SelectScrapeTasksForm,
    SendOneForm, ImportProfilesForm
)

from .models import (
    ApiQuotaPeriod,
    Tag,
    TwitterProfile,
    Tweet,
    TwitterProfileTagRel,
    ProfileFollowsProfileRel,
    Tweet,
    ProfileMentionedInTweet,
    ProfileMentionedInProfileDescription,
    RetweetRel,
    LikeRel
)
from twitter.util.ingestion import ingest_spreadsheet
from twitter.util.redis_util import send_scrape_work, send_scrape_work__conversation


logger = logging.getLogger(__name__)


SCRAPER_QUEUE_NAME = 'twint_twitter_items'
REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')

WORK_TYPES = [
    # note: order is important because of SelectScrapeTasksView.form_valid()
    'user_timeline',
    'user_likes',
    'friend_ids',
    'follower_ids',
    'user_info',
    'tweet_likes',
    'tweet_retweets',
    'conversation_tweets'
]

PROFILE_RELATED_MODELS = [
    TwitterProfileTagRel,
    ProfileFollowsProfileRel,
    Tweet,
    ProfileMentionedInTweet,
    ProfileMentionedInProfileDescription,
    RetweetRel,
    LikeRel,
]


@require_http_methods(["POST"])
def merge_profiles_view(request):
    try:
        data = json.loads(request.body)
    except:
        return HttpResponseBadRequest('invalid request body, json parse failed')

    to_merge = data['to_merge']
    remove_profiles = data['remove']

    bf = datetime.datetime.now()
    for cls in PROFILE_RELATED_MODELS:
        cls.merge_profiles(to_merge)
    af = datetime.datetime.now()

    time_taken = (af - bf).total_seconds()
    print(f"merged {len(to_merge)} profiles, took: {time_taken}")

    if remove_profiles:
        to_remove = [tup[1] for tup in to_merge]
        for cls in PROFILE_RELATED_MODELS:
            cls.remove_profiles(to_remove)
        TwitterProfile.objects.filter(id__in=to_remove).delete()

    return HttpResponse('ok')


class ImportProfilesView(FormView):
    template_name = 'twitter/import_profiles_spreadsheet.html'
    form_class = ImportProfilesForm

    def form_valid(self, form):
        form_data = form.cleaned_data
        num_profiles_before = TwitterProfile.objects.count()
        ingest_spreadsheet(file_contents=form_data['spreadsheet_file_content'])
        num_profiles_after = TwitterProfile.objects.count()
        num_new = num_profiles_after - num_profiles_before
        messages.success(self.request, f"{num_new} profiles created")
        return super().form_valid(form)

    def get_success_url(self):
        return reverse('import-spreadsheet')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['total_tags_count'] = Tag.objects.count()
        return context


class SendOneView(FormView):

    template_name = 'twitter/send_one.html'
    form_class = SendOneForm

    def form_valid(self, form):
        work_type = form.cleaned_data['work_type']

        if work_type == 'conversation_tweets':
            conversation_ids = form.cleaned_data['conversation_ids']
            items_sent = send_scrape_work__conversation(
                None, conversation_ids, priority=1, flush=True
            )
        else:
            profiles = form.cleaned_data['selected_profiles']
            items_sent = send_scrape_work(
                None, profiles, work_type, priority=1, flush=True
            )
        messages.success(self.request, f"{work_type} requested, sent: {items_sent} items")
        return super().form_valid(form)

    def form_invalid(self, form):
        # todo: improve message detail
        messages.error(self.request, f"profile not found or missing user_info")  # assuming this is the case here
        return redirect('send-one')

    def get_success_url(self):
        return reverse('send-one')


class SelectTagsView(FormView):

    template_name = 'twitter/select_tags.html'
    form_class = SelectTagsForm

    def form_valid(self, form):
        tag_slugs = form.cleaned_data['tag_slugs']

        profiles = Tag.get_profiles_with_tags(tag_slugs)
        if len(profiles) == 0:
            messages.error(self.request, f"no profiles found with tags: {tag_slugs}")
            return redirect('select-tags')

        self.num_userinfo_missing = len([o for o in profiles if o.user_info is None])
        self.request.session['selected_tags'] = ','.join(tag_slugs)
        return super().form_valid(form)

    def get_form_kwargs(self):
        kwargs = super(SelectTagsView, self).get_form_kwargs()
        # kwargs['user'] = user = self.request.user
        # if 'initial' not in kwargs:
        #    kwargs['initial'] = {}
        return kwargs

    def get_success_url(self):
        if self.num_userinfo_missing > 0:
            return reverse('select-user-info-action')
        return reverse('select-scrape-tasks')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['total_tags_count'] = Tag.objects.count()
        return context


def flush_group_view(request, work_type):

    if work_type not in WORK_TYPES:
        raise SuspiciousOperation(f'unexpected work_type: {work_type}')

    work_types = [work_type]

    redis_cli = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT)
    for wt in work_types:
        msg = {'work_type': wt, 'items': ['flush-group']}
        redis_cli.lpush(SCRAPER_QUEUE_NAME, json.dump(msg))

    messages.success(request, f'{work_type} group flushed')

    return redirect('select-tags')


def feed_view(request, screen_name_or_userid):
    if screen_name_or_userid.isdigit():
        user_id = screen_name_or_userid
        profile = get_object_or_404(TwitterProfile, user_id=user_id)
    else:
        screen_name = screen_name_or_userid.lower()
        profile = get_object_or_404(TwitterProfile, screen_name=screen_name)

    tweets = Tweet.objects.filter(author=profile, json_data__isnull=False).order_by('-publish_datetime')[:20]
    context = {'tweets': tweets}
    return render(request, "twitter/twitter_feed.html", context)  # todo: change to not use base html file


class SelectUserInfoActionView(FormView):

    template_name = 'twitter/select_userinfo_action.html'
    form_class = SelectUserInfoActionForm

    def get_form_kwargs(self):
        kwargs = super(SelectUserInfoActionView, self).get_form_kwargs()

        profiles_with_ui, _ = self._get_profiles()
        kwargs['num_with_user_info'] = len(profiles_with_ui)

        # kwargs['user'] = user = self.request.user
        # if 'initial' not in kwargs:
        #    kwargs['initial'] = {}
        return kwargs

    def form_valid(self, form):
        form_data = form.cleaned_data

        tags = self.request.session['selected_tags']
        if not tags:  # should never get here
            messages.error(self.request, 'no selected_tags found in session')
            return redirect('select-tags')

        cancel = bool(form_data.get('cancel'))
        fetch_userinfo = bool(form_data.get('fetch_userinfo'))

        if cancel:
            return redirect('select-tags')

        if fetch_userinfo:
            profiles = Tag.get_profiles_with_tags(tags.split(','), available_only=False)
            num_items = send_scrape_work(
                None, profiles, 'user_info', priority=1, flush=True
            )
            time.sleep(1)  # delay the user a little so items get processed
            messages.info(self.request, f"requested user-info for {len(profiles)} profiles")

        return super().form_valid(form)

    def get_success_url(self):
        return reverse('select-scrape-tasks')

    def _get_profiles(self):
        tag_slugs_list = self.request.session['selected_tags'].split(',')
        profiles = Tag.get_profiles_with_tags(tag_slugs_list)
        profiles_no_ui = [p for p in profiles if p.user_info is None]
        profiles_with_ui = [p for p in profiles if p.user_info is not None]
        return profiles_with_ui, profiles_no_ui

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        profiles_with_ui, profiles_no_ui = self._get_profiles()
        context['num_without_user_info'] = len(profiles_no_ui)
        context['num_with_user_info'] = len(profiles_with_ui)
        context['tags_joined'] = self.request.session['selected_tags']

        return context


class SelectScrapeTasksView(FormView):

    template_name = 'twitter/select_scrape_tasks.html'
    form_class = SelectScrapeTasksForm

    def __init__(self, *args, **kwargs):
        super(SelectScrapeTasksView, self).__init__(*args, **kwargs)

    def form_invalid(self, form):
        return super(SelectScrapeTasksView, self).form_invalid(form)

    def form_valid(self, form):
        '''
        {
            'scrape_user_timeline': True, 'scrape_user_likes': False,
            'scrape_friend_ids': False,
            'scrape_follower_ids': False,
            'user_timeline_priority': '2', 'user_likes_priority': '2',
            'friend_ids_priority': '2',
            'follower_ids_priority': '2', 'user_likes_limit': 19, 'user_timeline_limit': 19,
            'friend_ids_limit': 19, 'follower_ids_limit': 19,
        }
        '''
        form_data = form.cleaned_data

        tag_slugs_list = self.request.session['selected_tags'].split(',')
        profiles = Tag.get_profiles_with_tags(tag_slugs_list)

        profiles = [p for p in profiles if p.user_info is not None]
        profiles_no_ui = [p for p in profiles if p.user_info is None]
        if profiles_no_ui:
            print(f"warning: skipping {len(profiles_no_ui)} profiles without user-info")

        if not profiles:
            messages.error(self.request, 'no profiles with user-info found')
            return super(SelectScrapeTasksView, self).form_valid(form)

        workload_keys = ['user_timeline', 'user_likes', 'friend_ids', 'follower_ids']

        priority = int(form_data['priority'])
        limit = int(form_data['limit'])

        for wt in workload_keys:
            do_scrape = form_data[f'scrape_{wt}']
            if not do_scrape:
                continue

            _profiles = profiles
            _profiles = profiles[:limit]
            if not _profiles:
                continue   # limit was set to < 1

            flush = form_data.get('flush_queues', False)
            send_scrape_work(
                None, _profiles, wt, priority=priority, flush=flush
            )

        self.request.session['selected_tags'] = None
        messages.success(self.request, f'profiles sent for scrape, {len(profiles_no_ui)} skipped')
        return super(SelectScrapeTasksView, self).form_valid(form)

    def get_form_kwargs(self):
        kwargs = super(SelectScrapeTasksView, self).get_form_kwargs()

        tag_slugs_list = self.request.session['selected_tags'].split(',')
        profiles = Tag.get_profiles_with_tags(tag_slugs_list)
        kwargs['num_profiles'] = len(profiles)

        return kwargs

    def get_success_url(self):
        return reverse('select-tags')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        tag_slugs_list = self.request.session['selected_tags'].split(',')
        profiles = Tag.get_profiles_with_tags(tag_slugs_list)
        context['num_profiles'] = len(profiles)

        return context

    def get(self, *args, **kwargs):

        tags_li = self.request.session['selected_tags'].split(',')
        profiles = Tag.get_profiles_with_tags(tags_li)

        if len(profiles) == 0:
            messages.error(self.request, f'no profiles found for tags: {tags_li}')
            return redirect('select-tags')

        return super(SelectScrapeTasksView, self).get(*args, *kwargs)


def get_current_quota_periods__view(request, service_slug, endpoint_slug, account_slug):

    now = get_utc_now()
    quota_periods = ApiQuotaPeriod.objects.filter(
            service_slug=service_slug, endpoint_slug=endpoint_slug,
            account_slug=account_slug,
            start_datetime__gte=now, end_datetime__lte=now
    )
    object_dicts = [obj.get_dict() for obj in quota_periods]
    return HttpResponse(
        msgpack.dumps(object_dicts), content_type='application/msgpack'
    )

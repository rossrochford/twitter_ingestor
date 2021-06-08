"""twitter_webserver URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path

from twitter.views import (
    ImportProfilesView, SelectTagsView, SendOneView,
    SelectScrapeTasksView, SelectUserInfoActionView, flush_group_view,
    merge_profiles_view, get_current_quota_periods__view
)

urlpatterns = [
    path('admin/', admin.site.urls),
    path('import-profiles/', ImportProfilesView.as_view(), name='import-profiles'),
    path('send-one/', SendOneView.as_view(), name='send-one'),

    path('select-tags/', SelectTagsView.as_view(), name='select-tags'),
    path('select-userinfo-action/', SelectUserInfoActionView.as_view(), name='select-user-info-action'),
    path('flush-group/<str:work_type>', flush_group_view, name='flush-group'),
    path('select-scrape-tasks/', SelectScrapeTasksView.as_view(), name='select-scrape-tasks'),

    path('current-quota-periods/<str:service_slug>/<str:endpoint_slug>/<str:account_slug>', get_current_quota_periods__view),

    path('merge-twitter-profiles/', merge_profiles_view),
]


'''
    path('select-tags/', SelectTagsView.as_view(), name='select-tags'),

    path('flush-group/<str:work_type>', flush_group_view, name='flush-group'),
    path('send-one/', SendOneView.as_view(), name='send-one'),
    # path('notify-new-data/', notify_new_data__view),
    path('feed/<str:screen_name_or_userid>', feed_view)
'''


''''
urlpatterns = [
    path('admin/', admin.site.urls),
    path('import-spreadsheet/', ImportProfilesView.as_view(), name='import-spreadsheet'),

    path('select-userinfo-action/', SelectUserInfoActionView.as_view(), name='select-user-info-action'),
    path('select-scrape-tasks/', SelectScrapeTasksView.as_view(), name='select-scrape-tasks'),
    path('flush-group/<str:work_type>', flush_group_view, name='flush-group'),

    # path('notify-new-data/', notify_new_data__view),
    path('feed/<str:screen_name_or_userid>', feed_view)
]'''

from django import forms
from django.contrib import admin

from .models import (
    TwitterProfile, TwitterProfileTagRel, ProfileFollowsProfileRel,
    TagCategory, Tag, Tweet, ApiQuotaPeriod
    # Tweet, RetweetRel, ReplyRel, Tag, WorkerGroup
)

'''
AUTHOR_CHOICES = (
   (o.id, o.admin_choice_display) for o in TwitterProfile.objects.all()
)

TAG_CHOICES = (
   (o.id, o.slug) for o in Tag.objects.all()
)'''

class TweetAdminForm(forms.ModelForm):
    class Meta:
        model = Tweet
        # widgets = {
        #  'author_model_id': forms.Select(choices=AUTHOR_CHOICES),
        # }
        fields = '__all__'


@admin.register(Tweet)
class TweetAdmin(admin.ModelAdmin):
    form = TweetAdminForm


@admin.register(TwitterProfile)
class TwitterProfileAdmin(admin.ModelAdmin):
    exclude = ('user_timeline', 'user_likes')
    readonly_fields = ('user_timeline_is_set', 'user_likes_is_set')

    # django admin is very slow when loading this content, so just show
    # whether these fields are set or not
    def user_timeline_is_set(self, obj):
        return bool(obj.user_timeline)

    def user_likes_is_set(self, obj):
        return bool(obj.user_timeline)


admin.site.register(ApiQuotaPeriod)
admin.site.register(TwitterProfileTagRel)
admin.site.register(ProfileFollowsProfileRel)
admin.site.register(TagCategory)
admin.site.register(Tag)

# Generated by Django 3.2.4 on 2021-06-13 21:54

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('twitter', '0006_alter_tagcategory_tags'),
    ]

    operations = [
        migrations.AddField(
            model_name='tweet',
            name='conversation_id',
            field=models.CharField(blank=True, max_length=40, null=True),
        ),
        migrations.AlterField(
            model_name='likerel',
            name='like_api_id',
            field=models.CharField(blank=True, max_length=40, null=True),
        ),
        migrations.AlterField(
            model_name='retweetrel',
            name='retweet_api_id',
            field=models.CharField(blank=True, max_length=40, null=True),
        ),
        migrations.AlterField(
            model_name='tweet',
            name='scrape_source',
            field=models.CharField(blank=True, choices=[('user-timeline', 'user-timeline'), ('user-timeline-retweet', 'user-timeline-retweet'), ('user-timeline-quote', 'user-timeline-quote'), ('user-like', 'user-like')], max_length=40, null=True),
        ),
        migrations.AlterField(
            model_name='tweet',
            name='tweet_api_id',
            field=models.CharField(max_length=40),
        ),
        migrations.AlterField(
            model_name='tweet',
            name='tweet_type',
            field=models.CharField(blank=True, choices=[('status', 'status'), ('reply', 'reply'), ('quote', 'quote')], max_length=40, null=True),
        ),
        migrations.AlterField(
            model_name='twitterprofile',
            name='follower_ids_cursor',
            field=models.CharField(blank=True, max_length=60, null=True),
        ),
        migrations.AlterField(
            model_name='twitterprofile',
            name='friend_ids_cursor',
            field=models.CharField(blank=True, max_length=60, null=True),
        ),
        migrations.AlterField(
            model_name='twitterprofile',
            name='screen_name',
            field=models.CharField(blank=True, max_length=60, null=True),
        ),
        migrations.AlterField(
            model_name='twitterprofile',
            name='user_id',
            field=models.CharField(blank=True, max_length=60, null=True, unique=True),
        ),
        migrations.AlterField(
            model_name='twitterprofile',
            name='user_likes_since_id',
            field=models.CharField(blank=True, max_length=60, null=True),
        ),
        migrations.AlterField(
            model_name='twitterprofile',
            name='user_timeline_since_id',
            field=models.CharField(blank=True, max_length=60, null=True),
        ),
    ]

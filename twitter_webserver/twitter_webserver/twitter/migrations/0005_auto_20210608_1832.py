# Generated by Django 3.2.4 on 2021-06-08 18:32

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('twitter', '0004_auto_20210608_1828'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='tag',
            name='category',
        ),
        migrations.AddField(
            model_name='tagcategory',
            name='tags',
            field=models.ManyToManyField(to='twitter.Tag'),
        ),
    ]
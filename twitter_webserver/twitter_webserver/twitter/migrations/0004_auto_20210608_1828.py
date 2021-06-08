# Generated by Django 3.2.4 on 2021-06-08 18:28

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('twitter', '0003_auto_20210606_1338'),
    ]

    operations = [
        migrations.CreateModel(
            name='TagCategory',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('slug', models.CharField(max_length=199, unique=True)),
            ],
        ),
        migrations.AlterField(
            model_name='apiquotaperiod',
            name='endpoint_slug',
            field=models.CharField(choices=[('azure:named-entity-recognition', 'azure:named-entity-recognition'), ('twitter:user-info', 'twitter:user-info'), ('twitter:user-timeline', 'twitter:user-timeline'), ('twitter:user-timeline', 'twitter:user-timeline')], max_length=99),
        ),
        migrations.AddField(
            model_name='tag',
            name='category',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='twitter.tagcategory'),
        ),
    ]
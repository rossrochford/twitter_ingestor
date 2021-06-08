from collections import defaultdict
import datetime
import json

from django.core.management.base import BaseCommand
import xlrd3 as xlrd

from twitter.models import ApiQuotaPeriod, PERIOD_DURATIONS, ENDPOINT_QUOTAS
from util_shared.datetime_utils import get_utc_now



class Command(BaseCommand):

    def handle(self, *args, **options):

        with open("/app/api-keys.json") as f:
            API_KEYS = json.load(f)

        now = get_utc_now()
        today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        eight_days_in_future = today_midnight + datetime.timedelta(days=8)

        for service_slug, api_keys in API_KEYS.items():
            if service_slug not in ENDPOINT_QUOTAS:
                continue
            for account_slug, account_dict in api_keys.items():
                for endpoint_slug, duration_slug, units in ENDPOINT_QUOTAS[service_slug]:
                    duration = PERIOD_DURATIONS[duration_slug]

                    start_datetime = today_midnight
                    while start_datetime < eight_days_in_future:

                        period_midpoint = start_datetime + (duration / 2)

                        existing_period = ApiQuotaPeriod.objects.filter(
                            account_slug=account_slug,
                            service_slug=service_slug,
                            endpoint_slug=endpoint_slug,
                            start_datetime__gt=period_midpoint,
                            end_datetime__lt=period_midpoint,
                            duration_slug=duration_slug
                        ).first()

                        end_datetime = start_datetime + duration

                        if existing_period is None:
                            ApiQuotaPeriod.objects.create(
                                account_slug=account_slug,
                                service_slug=service_slug,
                                endpoint_slug=endpoint_slug,
                                start_datetime=start_datetime,
                                end_datetime=end_datetime,
                                duration_slug=duration_slug,
                                units_remaining=units
                            )

                        start_datetime = end_datetime

        # for quota_type, length, units in QUOTA_PERIOD_LENGTHS:
        #   for account_slug in API_ACCOUNT_SLUGS[quota_type]:

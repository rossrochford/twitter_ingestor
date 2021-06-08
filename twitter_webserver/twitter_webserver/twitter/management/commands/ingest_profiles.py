from collections import defaultdict

from django.core.management.base import BaseCommand
import xlrd3 as xlrd

from twitter.models import TwitterProfile, Tag, TwitterProfileTagRel




def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def _update_tags(profile_obj, tag_slugs, tag_objects_by_slug, is_new):

    if is_new:
        existing_tag_slugs = []
    else:
        rels = TwitterProfileTagRel.objects.filter(
            twitter_profile=profile_obj
        )
        existing_tag_slugs = [r.tag.slug for r in rels]

    for slug in tag_slugs:
        if slug not in existing_tag_slugs:
            if slug not in tag_objects_by_slug:
                tag_objects_by_slug[slug] = Tag.objects.get_or_create(slug=slug)[0]
            tag_obj = tag_objects_by_slug[slug]
            TwitterProfileTagRel.objects.create(
                twitter_profile=profile_obj, tag=tag_obj
            )


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('workbook_filepath', action='store', type=str)

    def handle(self, *args, **options):

        if TwitterProfile.objects.all().count() > 8000:
            print('skipping ingest')
            exit(0)

        # options['workbook_filepath']



        for screen_name, tag_slugs in entries.items():
            if screen_name in existing_profiles:
                profile_obj = existing_profiles[screen_name]
                is_new = False
            else:
                profile_obj = TwitterProfile.objects.create(
                    screen_name=screen_name, manually_added=True
                )
                is_new = True
            _update_tags(profile_obj, tag_slugs, tag_objects_by_slug, is_new)

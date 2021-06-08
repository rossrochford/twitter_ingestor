import xlrd3 as xlrd

from twitter.models import TwitterProfile, TwitterProfileTagRel, Tag
from .model_util import get_or_create_by_key, get_or_create_by_multiple_keys


def _get_username_from_url(url):
    url = url.strip().split('?')[0].lower()
    try:
        return url.rsplit('/', 1)[1]
    except:
        import pdb; pdb.set_trace()
        return None


def _open_spreadsheet_file(filename=None, file_contents=None):

    assert filename or file_contents

    book = xlrd.open_workbook(filename=filename, file_contents=file_contents)
    worksheet = book.sheet_by_index(0)

    tag_slugs_all = set()
    tag_slugs_by_screen_name = {}

    for rx in range(worksheet.nrows):
        row = worksheet.row(rx)
        twitter_url = (row[0].value or '').strip()
        if not twitter_url:
            continue

        screen_name = _get_username_from_url(twitter_url)
        tag_slugs = [c.value.strip().lower() for c in row[1:] if c.value.strip()]

        if screen_name not in tag_slugs_by_screen_name:
            tag_slugs_by_screen_name[screen_name] = []

        for slug in tag_slugs:
            tag_slugs_by_screen_name[screen_name].append(slug)
            tag_slugs_all.add(slug)

    tag_slugs_all = [slug for slug in tag_slugs_all]

    return tag_slugs_by_screen_name, tag_slugs_all


'''
def _get_or_create_profiles__from_key(db, key, key_values):

    existing_profiles = TwitterProfile.objects.filter(screen_name__in=screen_names)
    existing_screen_names = [obj.screen_name for obj in existing_profiles]

    new_screen_names = set(screen_names) - set(existing_screen_names)
    new_docs = [
        {'screen_name': sn} for sn in new_screen_names
    ]

    profile_ids_by_screen_name = {}
    for profile in existing_profiles:
        profile_ids_by_screen_name[profile.screen_name] = profile.id

    if not new_docs:
        return profile_ids_by_screen_name

    docs_by_id = create_many(db, 'twitter_twitterprofile', new_docs)
    for id, doc in docs_by_id.items():
        profile_ids_by_screen_name[doc['screen_name']] = id

    return profile_ids_by_screen_name
'''


def ingest_spreadsheet(filename=None, file_contents=None):

    tag_slugs_by_sn, tag_slugs = _open_spreadsheet_file(
        filename=filename, file_contents=file_contents
    )
    screen_names = [k for k in tag_slugs_by_sn.keys()]

    profiles_by_sn = get_or_create_by_key(
        TwitterProfile, 'screen_name', screen_names,
        defaults={'manually_added': True}
    )
    tags_by_slug = get_or_create_by_key(Tag, 'slug', tag_slugs)

    profile_tag_rels = []
    for screen_name, tag_slugs in tag_slugs_by_sn.items():
        profile_pk = profiles_by_sn[screen_name].pk
        for slug in tag_slugs:
            profile_tag_rels.append({
                'twitter_profile_id': profile_pk, 'tag_id': tags_by_slug[slug].pk
            })

    if profile_tag_rels:
        objects = get_or_create_by_multiple_keys(TwitterProfileTagRel, profile_tag_rels)


    '''

    if not profile_tag_rels:
        return

    get_or_create_rels(db, 'twitter_twitterprofiletagrel', profile_tag_rels)



    profiles = [
        obj for obj in TwitterProfile.objects.filter(screen_name__in=screen_names)
    ]
    existing_screen_names = [obj.screen_name for obj in profiles]
    new_screen_names = set(screen_names) - set(existing_screen_names)

    if new_screen_names:
        new_profiles = TwitterProfile.objects.bulk_create([
            TwitterProfile(screen_name=sn) for sn in new_screen_names
        ])
        profiles.extend([obj for obj in new_profiles])

    profiles_by_sn = {obj.screen_name: obj for obj in profiles}



    new_docs = [
        {'screen_name': sn} for sn in new_screen_names
    ]
    if


    existing_profiles = TwitterProfile.objects.filter(screen_name__in=screen_names)
    existing_screen_names
    new_screen_names = [sn for sn in screen_names if sn not in

    profile_docs_by_id, profile_docs_by_sn, _ = get_or_create_from_key(
        db, 'twitter_twitterprofile', 'screen_name', screen_names,
        defaults={'manually_added': True}
    )

    _, tag_docs_by_slug, _ = get_or_create_from_key(
        db, 'twitter_tag', 'slug', tag_slugs
    )

    _get_or_create_tag_rels(
        db, tag_slugs_by_sn, profile_docs_by_sn, tag_docs_by_slug
    )'''


def _get_or_create_tag_rels(db, tag_slugs_by_sn, profile_docs_by_sn, tag_docs_by_slug):
    profile_tag_rels = []
    for screen_name, tag_slugs in tag_slugs_by_sn.items():
        profile_obj_id = profile_docs_by_sn[screen_name]['_id']
        for slug in tag_slugs:
            profile_tag_rels.append({
                'twitter_profile_id': profile_obj_id,
                'tag_id': tag_docs_by_slug[slug]['_id']
            })

    if not profile_tag_rels:
        return

    get_or_create_rels(db, 'twitter_twitterprofiletagrel', profile_tag_rels)

    '''
    existing_rels = find_many__or(db, 'twitter_twitterprofiletagrel', profile_tag_rels)
    existing_rel_id_pairs = [
        (doc['twitter_profile_id'], doc['tag_id']) for doc in existing_rels.values()
    ]
    profile_tag_rels_new = []
    for doc in profile_tag_rels:
        pair = (doc['twitter_profile_id'], doc['tag_id'])
        if pair not in existing_rel_id_pairs:
            profile_tag_rels_new.append(doc)

    if profile_tag_rels_new:
        create_many(db, 'twitter_twitterprofiletagrel', profile_tag_rels_new)
    '''

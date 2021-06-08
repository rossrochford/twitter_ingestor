from django.db.models import Q


def get_or_create_by_key(ModelClass, key_name, key_values, defaults=None):

    kwargs = {f"{key_name}__in": key_values}
    objects = [o for o in ModelClass.objects.filter(**kwargs)]
    existing_values = [getattr(obj, key_name) for obj in objects]
    new_values = set(key_values) - set(existing_values)

    if new_values:

        new_objects = []
        for val in new_values:
            kwargs = {key_name: val}
            if defaults:
                kwargs.update(defaults)
            new_objects.append(ModelClass(**kwargs))

        new_objects = ModelClass.objects.bulk_create(new_objects)
        objects.extend([obj for obj in new_objects])

    objects_by_value = {getattr(obj, key_name): obj for obj in objects}
    return objects_by_value


def _chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def fetch_by_multiple_keys(ModelClass, value_dicts, key_fields=None):

    if key_fields is None:
        key_fields = tuple([k for k in value_dicts[0].keys() if k.endswith('_id')])
    key_fields_str = ', '.join(key_fields)
    table_name = ModelClass.objects.model._meta.db_table

    pair_tuples = []
    for val_dict in value_dicts:
        curr = tuple([val_dict[k] for k in key_fields])
        pair_tuples.append(curr)

    objects = {}
    for chunk in _chunker(pair_tuples, 2000):  # limit query size
        # based on: http://www.sqlfiddle.com/#!1/9cdb4/6
        query_str = f"SELECT * FROM {table_name} WHERE ({key_fields_str}) IN {tuple(chunk)};"
        query_str = query_str.replace("),);", "));")
        for obj in ModelClass.objects.raw(query_str):
            tuple_key = tuple([getattr(obj, k) for k in key_fields])
            objects[tuple_key] = obj
    return objects


def get_or_create_by_multiple_keys(ModelClass, value_dicts, defaults=None, key_fields=None):

    if key_fields is None:
        key_fields = tuple([k for k in value_dicts[0].keys() if k.endswith('_id')])

    existing_objects = fetch_by_multiple_keys(
        ModelClass, value_dicts, key_fields=key_fields
    )

    existing_tuples, new_objects = [], []
    for val_dict in value_dicts:
        tup = tuple([val_dict[k] for k in key_fields])
        if tup in existing_tuples:
            continue
        existing_tuples.append(tup)  # just in case, prevents duplicates
        if defaults:
            val_dict.update(defaults)
        new_objects.append(ModelClass(**val_dict))

    if new_objects:
        new_objects = [o for o in ModelClass.objects.bulk_create(new_objects)]

    existing_objects = [obj for obj in existing_objects.values()]

    return existing_objects + new_objects



def merge_uniquetogether_rels(ModelClass, profile_id_field, other_id_field, profiles_to_merge):
    profile_ids = []
    for id_to_keep, id_to_remove in profiles_to_merge:
        profile_ids.append(ids_to_keep)
        profile_ids.append(id_to_remove)

    rel_objects_by_profile_id = defaultdict(list)
    kwargs = {f"{profile_id_field}__": profile_ids}
    for obj in ModelClass.objects.filter(**kwargs):
        profile_id = getattr(obj, profile_id_field)
        rel_objects_by_profile_id[profile_id].append(obj)

    with transaction.atomic():
        # atomic() defers save calls so they will be aggregated after the block
        for profile_to_keep, profile_to_remove in profiles_to_merge:
            if profile_to_remove not in rel_objects_by_profile_id:
                continue  # no rels to transfer found

            # change twitter_profile_ids but ensure unique_together constraint isn't violated
            rels_to_transfer = rel_objects_by_profile_id[profile_to_remove]
            existing_rels = rel_objects_by_profile_id[profile_to_keep]
            existing_other_ids = [getattr(r, other_id_field) for r in existing_rels]

            for rel_obj in rels_to_transfer:
                if getattr(rel_obj, other_id_field) in existing_other_ids:
                    rel_obj.delete()
                    continue
                setattr(rel_obj, profile_id_field, profile_to_keep)
                rel_obj.save()


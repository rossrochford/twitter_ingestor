import sys

import trio

from twitter_pqueue_scraper.util.http_util import TwitterHttpSession


async def _handle_40x(twitter_session, user_id_strings, status_codes):
    if len(user_id_strings) == 1:
        return []  # skip this user
    if len(user_id_strings) <= 4:
        results = []
        for id_str in user_id_strings:
            results.extend(
                await get_user_info__chunk(twitter_session, [id_str], status_codes)
            )
        return results
    else:
        midpoint = int(round(len(user_id_strings) / 2))
        res1 = await get_user_info__chunk(
            twitter_session, user_id_strings[:midpoint], status_codes
        )
        res2 = await get_user_info__chunk(
            twitter_session, user_id_strings[midpoint:], status_codes
        )
        return res1 + res2


async def get_user_info__chunk(
    twitter_session, user_ids_or_screen_names, status_codes
):

    screen_names, user_ids = set(), set()
    for id_str in user_ids_or_screen_names:
        if id_str is not None:
            if id_str.isdigit():
                user_ids.add(id_str)
            else:
                screen_names.add(id_str)

    status_code, resp_obj = await twitter_session.get_user_info(user_ids, screen_names)

    if status_code in (404, 401):
        if len(user_ids_or_screen_names) == 1:
            status_codes[user_ids_or_screen_names[0]] = status_code
        return await _handle_40x(
            twitter_session, user_ids_or_screen_names, status_codes
        )
    else:
        for id_sn in user_ids_or_screen_names:
            status_codes[id_sn] = status_code

    if status_code != 200:
        print(f'warning: {url} gave unexpected status_code: {status_code}')
        return []

    results = resp_obj.json()
    for di in results:
        di['screen_name'] = di['screen_name'].lower()
        di['is_available'] = not di['protected']
        # note: absence isn't checked here (meaning, an invalid/removed account)
        # this is checked in: scrape_user_info()

    # remove any duplicates (e.g. when a screen_name and user_id for the same user are requested)
    results = {di['id_str']: di for di in results}
    results = [di for di in results.values()]

    return results


async def _once(screen_name, cursor=None):
    twitter_session = TwitterHttpSession.create_one()
    results = await get_user_info__chunk(
        twitter_session, [screen_name], []
    )
    import pdb; pdb.set_trace()



if __name__ == '__main__':
    args = sys.argv[1:]
    assert len(args) > 0

    screen_nm = args[0]
    curs = None
    if len(args) > 1:
        curs = args[1]

    trio.run(_once, screen_nm, curs)


from eliot import start_action
import trio



async def get_cursored(
    twitter_http_session, func, user_id, max_pages,
    initial_cursor=None, delay_override=None
):

    if delay_override is None:
        request_delay = 59
        if func.__name__ == '_get_user_timeline':
            request_delay = 0
    else:
        request_delay = delay_override

    request_delay = 0.2
    print("warning: setting request_delay to 0.2")

    items, next_cursor = [], initial_cursor
    status_code = None

    for i in range(max_pages):

        print(f"{func.__name__} page_num: {i}")

        _items, next_cursor, status_code = await func(
            twitter_http_session, user_id, cursor=next_cursor
        )
        if not _items:
            break
        with start_action(action_type='sleep', request_delay=request_delay):
            await trio.sleep(request_delay)
        items.extend(_items)
        if next_cursor == '0':
            break

    return items, next_cursor, status_code

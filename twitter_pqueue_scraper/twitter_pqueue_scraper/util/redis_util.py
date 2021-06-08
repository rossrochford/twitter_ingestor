import os

import msgpack
import redio
import trio


REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
REDIS_URL = f"redis://{REDIS_HOSTNAME}:{REDIS_PORT}"


class RedisGroupStreamClient(object):

    def __init__(self, stream_name, group_name, consumer_name):
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.redis_cli = redio.Redis(REDIS_URL)

    async def xgroup_create(self):
        return await self.redis_cli().xgroup('CREATE', self.stream_name, self.group_name, '$', 'MKSTREAM')

    async def xack(self, *line_ids):
        await self.redis_cli().xack(self.stream_name, self.group_name, *line_ids)

    @staticmethod
    def _serialize_line(msg_dict):

        field_values_unwrapped = []
        for key, val in msg_dict.items():
            if key == 'max_length':  # hacky, I know...
                continue
            if type(val) is not bytes:
                try:
                    val = msgpack.dumps(val)
                except:
                    return None
            field_values_unwrapped.append(key)
            field_values_unwrapped.append(val)

        return field_values_unwrapped

    async def xadd(self, max_length=None, **kwargs):

        if not kwargs:
            print('error: xadd received no kwargs')
            return False, None

        field_values_unwrapped = self._serialize_line(kwargs)

        if max_length:
            assert type(max_length) is int or max_length.isdigit()
            max_length = str(max_length)
            result = await self.redis_cli().xadd(self.stream_name, 'MAXLEN', '~', max_length, '*', *field_values_unwrapped)
        else:
            result = await self.redis_cli().xadd(self.stream_name, '*', *field_values_unwrapped)

        return True, result

    async def xadd_bulk(self, list_of_msgdicts):

        cooroutine = self.redis_cli().multi()
        for msg in list_of_msgdicts:
            field_values_unwrapped = self._serialize_line(msg)
            if field_values_unwrapped is None:
                return False, None
            cooroutine = cooroutine.xadd(self.stream_name, '*', *field_values_unwrapped)

        result = await cooroutine.exec()

        return True, result

    async def xreadgroup(self, count=200, start_id='>'):
        # start_id='>' means that the data has not been read by the members of the group so far.
        # Setting this to '0' returns data that has been read but unacknowedged (I think)

        count = str(count)
        args = ['GROUP', self.group_name, self.consumer_name, 'COUNT', count, 'BLOCK', '0',  'STREAMS', self.stream_name, start_id]

        result = await self.redis_cli().xreadgroup(*args)

        if result is False:
            return None
        if type(result) is redio.exc.ServerError:
            import pdb; pdb.set_trace()

        _, lines = result[0][:2]

        line_dicts = {}
        failed_ids = []
        for raw_line in lines:
            parse_success, line_id, value_dict = self.parse_line(raw_line)
            if not parse_success:
                print(f"failed to parse line: {raw_line}")
                failed_ids.append(line_id)
                continue
            line_dicts[line_id] = value_dict

        return line_dicts, failed_ids

    @staticmethod
    def parse_line(raw_line):
        value_dict = {}
        line_id, values = raw_line
        line_id = line_id.decode()
        while values:
            key, val = values[:2]
            try:
                value_dict[key.decode()] = msgpack.loads(val)
            except:
                return False, line_id, None
            values = values[2:]

        return True, line_id, value_dict

    async def flush_old_lines(self):
        pending_ids = []
        async for id in self.fetch_pending_ids(False):
            pending_ids.append(id)
            if len(pending_ids) > 200:
                # await self.xreadgroup(start_id=pending_ids_chunk[0], count=len(pending_ids_chunk))  # ids need to be re-read before ack
                print(f"xack {pending_ids[0]} to {pending_ids[-1]} ({len(pending_ids)})")
                res = await self.xack(*pending_ids)
                pending_ids.clear()

    async def get_pending_summary(self):
        # note: this gives us delivery counts, which can help detect and remove messages
        # that are repeatedly failing without being acknowledged (e.g. due to crash) i.e. "dead letter"
        summary = await self.redis_cli().xpending(
            self.stream_name, self.group_name
        )
        return summary

    async def fetch_pending_ids(self, chunk=False):

        min_idle_time = 0
        next_min_id = '-'

        prev_result = None
        while True:
            pending_line_details = await self.redis_cli().xpending(
                self.stream_name, self.group_name,
                'IDLE', min_idle_time, next_min_id, '+', '800'
            )
            #if next_min_id != '-':
             #   pending_line_details = pending_line_details[1:]

            if not pending_line_details:
                break
            prev_result = pending_line_details
            next_min_id = pending_line_details[-1][0].decode()
            if chunk:
                pending_ids_chunk = [tup[0].decode() for tup in pending_line_details]
                yield pending_ids_chunk
            else:
                for tup in pending_line_details:
                    yield tup[0].decode()

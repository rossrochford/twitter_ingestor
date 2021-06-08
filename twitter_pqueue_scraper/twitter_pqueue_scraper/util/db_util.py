import os

from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.pool import NullPool
import trio


def _create_db_connection_str():
    username = os.environ['POSTGRESQL_USERNAME']
    password = os.environ['POSTGRESQL_PASSWORD']
    hostname = os.environ['POSTGRESQL_HOSTNAME']
    port = 5432
    db_name = os.environ['POSTGRESQL_DATABASE']

    # 'postgresql://scott:tiger@localhost:5432/mydatabase'
    return f"postgresql://{username}:{password}@{hostname}:{port}/{db_name}"


class DBSession(Session):

    async def add_and_commit(self, objects):
        # do both together to save on thread overhead of two separate calls
        return await trio.to_thread.run_sync(
            self._add_and_commit, objects
        )

    def _add_and_commit(self, objects):
        self.add_all(objects)
        self.commit()

    async def commit_async(self):
        return await trio.to_thread.run_sync(self.commit)

    async def add_all_async(self, objects):
        return await trio.to_thread.run_sync(self.add_all, objects)


def _create_sqlalchemy_session():

    Base = automap_base()
    engine = create_engine(
        # disable connection pooling until asyncio support improves
        _create_db_connection_str(), poolclass=NullPool
    )
    # reflect the tables
    Base.prepare(engine, reflect=True)
    session = DBSession(engine)
    return session, Base


async def create_sqlalchemy_session():
    return await trio.to_thread.run_sync(_create_sqlalchemy_session)


async def get_or_create_by_key(db_session, ModelClass, key_name, key_values, defaults=None):

    def _get_objects(MC, key, values):
            return db_session.query(MC).filter(getattr(MC, key).in_(values)).all()

    # objects = db_session.query(ModelClass).filter(getattr(ModelClass, key_name).in_(key_values)).all()
    objects = await trio.to_thread.run_sync(_get_objects, ModelClass, key_name, key_values)
    objects = [o for o in objects]
    existing_values = [getattr(obj, key_name) for obj in objects]
    new_values = set(key_values) - set(existing_values)

    new_objects = []
    for val in new_values:
        kwargs = {key_name: val}
        if defaults:
            kwargs.update(defaults)
        new_objects.append(ModelClass(**kwargs))

    if new_objects:
        await db_session.add_and_commit(new_objects)
        objects.extend(new_objects)

    # gather results
    objects_by_id = {obj.id: obj for obj in objects}
    objects_by_key = {getattr(obj, key_name): obj for obj in objects}
    new_obj_ids = [obj.id for obj in new_objects]

    return objects_by_id, objects_by_key, new_obj_ids


def _chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def _run_sql(db_session, ModelClass, query_str):
        query_str = query_str.replace("),);", "));")
        query = db_session.query(ModelClass).from_statement(text(query_str))
        # note: when asyncio support improves we should load
        # objects lazily but returning 'query' and iterating this.
        return [o for o in query.all()]


async def get_or_create_by_multiple_keys(
    db_session, ModelClass, value_dicts, defaults=None, key_fields=None
):

    if key_fields is None:
        key_fields = [k for k in value_dicts[0].keys() if k.endswith('_id')]
    key_fields_str = ', '.join(key_fields)

    table_name = ModelClass.__table__.name.title().lower()

    pair_tuples = []
    for val_dict in value_dicts:
        curr = tuple([val_dict[k] for k in key_fields])
        pair_tuples.append(curr)

    existing_objects, existing_tuples = [], []
    for chunk in _chunker(pair_tuples, 2000):  # limit query size
        query_str = f"SELECT * FROM {table_name} WHERE ({key_fields_str}) IN {tuple(chunk)};"
        objects = await trio.to_thread.run_sync(
            _run_sql, db_session, ModelClass, query_str
        )
        for obj in objects:
            existing_objects.append(obj)
            existing_tuples.append(
                tuple([getattr(obj, k) for k in key_fields])
            )

    new_objects = []
    for val_dict in value_dicts:
        tup = tuple([val_dict[k] for k in key_fields])
        if tup in existing_tuples:
            continue
        existing_tuples.append(tup)  # just in case, prevents duplicates
        if defaults:
            val_dict.update(defaults)
        new_objects.append(ModelClass(**val_dict))

    if new_objects:
        await db_session.add_and_commit(new_objects)

    return existing_objects + new_objects


def _db_shell():
    session, Base = _create_sqlalchemy_session()
    TwitterProfile = Base.classes.twitter_twitterprofile
    import pdb; pdb.set_trace()
    print()


if __name__ == '__main__':
    _db_shell()

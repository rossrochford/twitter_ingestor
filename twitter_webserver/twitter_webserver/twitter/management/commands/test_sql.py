from collections import defaultdict
import os

from django.core.management.base import BaseCommand
import xlrd3 as xlrd

from twitter.models import TwitterProfile, Tag, TwitterProfileTagRel


def _create_db_connection_str():
    db_name = os.environ['POSTGRESQL_DATABASE']
    username = os.environ['POSTGRESQL_USERNAME']
    password = os.environ['POSTGRESQL_PASSWORD']
    hostname = os.environ['POSTGRESQL_HOSTNAME']
    port = 5432
    # 'postgresql://scott:tiger@localhost:5432/mydatabase'
    return f"postgresql://{username}:{password}@{hostname}:{port}/{db_name}"


def _create_sqlalchemy_session():
    from sqlalchemy.orm import Session
    from sqlalchemy import create_engine
    from sqlalchemy.ext.automap import automap_base

    Base = automap_base()

    # engine, suppose it has two tables 'user' and 'address' set up
    # sqlite_filepath = '/home/ross/code/twitter_scraper/twitter_webserver/twitter_webserver/db.sqlite3'
    # engine = create_engine(f"sqlite:///{sqlite_filepath}")
    engine = create_engine(_create_db_connection_str())

    # reflect the tables
    Base.prepare(engine, reflect=True)
    session = Session(engine)
    return session, Base

    # rudimentary relationships are produced
    # session.add(Address(email_address="foo@bar.com", user=User(name="foo")))
    # session.commit()

    # collection-based relationships are by default named
    # "<classname>_collection"
    # print (u1.address_collection)


class Command(BaseCommand):

    # def add_arguments(self, parser):
    #    parser.add_argument('workbook_filepath', action='store', type=str)

    def handle(self, *args, **options):

        #for profile in TwitterProfile.objects.all():
        #    print(profile.screen_name)

        db_session, Base = _create_sqlalchemy_session()
        TwitterProfile2 = Base.classes.twitter_twitterprofile

        print("from sqlalchemy:")
        # profile1 = session.query(TwitterProfile2).first()
        #for profile in session.query(TwitterProfile2).all():
        #   print(profile.screen_name)
        screen_names = ['patkua']
        profiles_without_userid = db_session.query(TwitterProfile2).filter(TwitterProfile2.user_id.is_(None)).filter(TwitterProfile2.screen_name.in_(screen_names)).all()
        profiles_without_userid = [o for o in profiles_without_userid]
        import pdb; pdb.set_trace()

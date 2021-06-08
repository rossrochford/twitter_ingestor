import trio

from twitter_pqueue_scraper.batch_tasks.util import get_mentions_from_string


class ProfileIndex(object):
    """
    Because the logic of scrape_user_info() is quite complex and
    profiles can be fetched by obj_id, user_id or screen_name, this
    class helps tidy up much of the code.
    """
    def __init__(self, worker, items, user_info_results):
        self.worker = worker
        self.items = items
        self.user_info_results = user_info_results

        self.profiles_by_id = {}
        self.profiles_by_userid = {}
        self.profiles_by_sn = {}

    def get_userinfo_by_userid(self):
        return {di['id_str']: di for di in self.user_info_results}

    def get_userinfo_by_sn(self):
        return {di['screen_name']: di for di in self.user_info_results}

    async def initialize_index(self):

        db_session, Base = self.worker.db_connection
        TwitterProfile = Base.classes.twitter_twitterprofile

        obj_ids, user_ids, screen_names = set(), set(), set()

        to_skip = []
        for item in self.items:
            if item.obj_id:
                obj_ids.add(item.obj_id)
                if item.user_id:
                    # avoid excess lookups and accidentally getting a duplicate
                    to_skip.append(item.user_id)
                if item.screen_name:
                    to_skip.append(item.screen_name)

        for ui_dict in self.user_info_results:
            user_id = ui_dict['id_str']
            screen_name = ui_dict['screen_name']
            if user_id not in to_skip:
                user_ids.add(user_id)
            if screen_name not in to_skip:
                screen_names.add(screen_name)
            for sn in get_mentions_from_string(ui_dict['description']):
                screen_names.add(sn)

        userinfo_by_userid = self.get_userinfo_by_userid()
        userinfo_by_sn = self.get_userinfo_by_sn()

        profiles = await trio.to_thread.run_sync(
           self. _db_fetch_profiles, obj_ids, user_ids, screen_names
        )

        for profile in profiles:
            self.profiles_by_id[profile.id] = profile
            ui_dict = userinfo_by_userid.get(profile.user_id) or userinfo_by_sn.get(profile.screen_name)

            if profile.user_id:
                self.profiles_by_userid[profile.user_id] = profile
            elif ui_dict:
                self.profiles_by_userid[ui_dict['id_str']] = profile

            if profile.screen_name:
                self.profiles_by_sn[profile.screen_name] = profile
            elif ui_dict:
                self.profiles_by_sn[ui_dict['screen_name']] = profile

    def _db_fetch_profiles(self, obj_ids, user_ids, screen_names):

        db_session, Base = self.worker.db_connection
        TwitterProfile = Base.classes.twitter_twitterprofile

        profiles1, profiles2, profiles3 = [], [], []
        if obj_ids:
            profiles1 = db_session.query(TwitterProfile).filter(TwitterProfile.id.in_(obj_ids)).all()
            profiles1 = [p for p in profiles1]
        if user_ids:
            profiles2 = db_session.query(TwitterProfile).filter(TwitterProfile.user_id.in_(user_ids)).all()
            profiles2 = [p for p in profiles2]
        if screen_names:
            profiles3 = db_session.query(TwitterProfile).filter(TwitterProfile.screen_name.in_(screen_names)).all()
            profiles3 = [p for p in profiles3]

        return profiles1 + profiles2 + profiles3

    def get_profile(self, item):
        profile = None
        if item.obj_id and item.obj_id in self.profiles_by_id:
            profile = self.profiles_by_id[item.obj_id]
        else:
            if item.user_id and item.user_id in self.profiles_by_userid:
                profile = self.profiles_by_userid[item.user_id]
            elif item.screen_name and item.screen_name in self.profiles_by_sn:
                profile = self.profiles_by_sn[item.screen_name]
        # todo: implement fetch_if_missing to issue a DB query?
        return profile

    def add_profile(self, new_profile):
        self.profiles_by_id[new_profile.id] = new_profile

        if new_profile.user_id:
            self.profiles_by_userid[new_profile.user_id] = new_profile
        if new_profile.screen_name:
            self.profiles_by_sn[new_profile.screen_name] = new_profile

    def change_item_profile(self, item, profile, ui_dict=None):
        """
        When a duplicate profile is found in the database, we want to modify
        the TwitterProfileItem and update the ProfileIndex
        """
        ui_dict = ui_dict or {}
        item.obj_id = profile.id

        user_id = item.user_id or profile.user_id or ui_dict.get('id_str')
        screen_name = (item.screen_name or profile.screen_name or ui_dict.get('screen_name') or '').lower()

        if user_id:
            self.profiles_by_userid[user_id] = profile
        if screen_name:
            self.profiles_by_sn[screen_name] = profile

        if new_profile.id:
            self.profiles_by_id[profile.id] = profile
        else:
            print(f"warning: change_item_profile() called when profile.id is None: {screen_name} {user_id}")


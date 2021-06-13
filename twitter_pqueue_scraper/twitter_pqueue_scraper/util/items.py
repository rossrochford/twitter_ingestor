
class TwitterConversationWorkItem(object):

    def __init__(self, line_id=None, conversation_id=None, work_type=None, **kwargs):
        self.line_id = line_id
        self.conversation_id = conversation_id
        self.work_type = work_type

    @staticmethod
    def create_from_dict(msg_di):
        if not (msg_di.get('work_type') and msg_di.get('conversation_id')):
            return False, None
        # todo: add more value checks
        if not msg_di.get('line_id'):
            return False, None
        return True, TwitterConversationWorkItem(**msg_di)


class TwitterProfileWorkItem(object):

    def __init__(
        self, line_id=None, obj_id=None, work_type=None, user_id=None,
        screen_name=None, user_info=None, since_id=None,
        mentioned_by_user=None, completion_event_uid=None, **kwargs
    ):
        self.line_id = line_id
        self.obj_id = obj_id
        self.work_type = work_type
        self.user_id = str(user_id) if user_id else None
        self.screen_name = screen_name.lower() if screen_name else None
        self.user_info = user_info
        self.since_id = since_id
        self.mentioned_by_user = mentioned_by_user
        self.completion_event_uid = completion_event_uid

    @staticmethod
    def create_from_dict(msg_di):
        work_type = msg_di.get('work_type')
        line_id = msg_di.get('line_id')

        obj_id = msg_di.get('obj_id')  # now mandatory
        user_id = msg_di.get('user_id')
        screen_name = msg_di.get('screen_name')

        user_info = msg_di.get('user_info')

        if not work_type:
            return False, None
        if not line_id:
            return False, None

        if work_type == 'user_info':
            if not (user_id or screen_name):
                return False, None
        else:
            if not (obj_id and type(obj_id) is int):
                return False, None
            # user_info required to check is_available & number of followers/friends
            if not (user_id and user_info):
                return False, None

        return True, TwitterProfileWorkItem(**msg_di)

    @property
    def profile_obj_id(self):
        return self.obj_id

    @property
    def profile_string(self):
        st = self.obj_id or ''
        if self.user_id:
            st = st + self.user_id
        if self.screen_name:
            st = st + self.screen_name
        return st

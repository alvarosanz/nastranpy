import json
from nastranpy.bdf.misc import get_hash


class Sessions(object):

    def __init__(self, sessions_file, admin_password=None):
        self.file = sessions_file
        self.sessions = dict()

        if admin_password:
            self.add_session('admin', admin_password, is_admin=True)
            self.add_session('guest', 'guest', is_admin=False)
        else:

            with open(self.file) as f:
                self.sessions = json.load(f)

    def flush(self):

        with open(self.file, 'w') as f:
            json.dump(self.sessions, f)

    def get_session(self, user, password):
        return dict(self.sessions[get_hash(f'{user}:{password}')])

    def add_session(self, user, password=None, session_hash=None,
                    is_admin=False, create_allowed=False, databases=None):

        if password:
            session_hash = get_hash(f'{user}:{password}')

        self.sessions[session_hash] = {'user': user,
                                       'is_admin': is_admin,
                                       'create_allowed': True if is_admin else create_allowed,
                                       'databases': databases}
        self.flush()

    def remove_session(self, user):

        for session_hash in self.sessions:

            if self.sessions[session_hash]['user'] == user:
                del self.sessions[session_hash]
                self.flush()
                return

        raise ValueError(f"User '{user}' does not exist!")

import os
import argparse


parser = argparse.ArgumentParser(description='Start central server', epilog="You will be asked for the 'admin' password")
parser.add_argument('--path', dest='root_path', metavar='ROOT_PATH', default=os.getcwd(),
                    help='folder containing the databases (by default is the current working directory)')
parser.add_argument('--sessions', dest='sessions_file', metavar='SESSIONS_FILE',
                    help="JSON formatted file holding the user sessions. By default 'sessions.json' is loaded. If not pressent, a new session file is created with both 'admin' and 'guest' (password 'guest') users")
parser.add_argument('--debug', dest='debug', action='store_const',
                    const=True, default=False,
                    help='activate debug mode')
args = parser.parse_args()

import nastranpy


server = nastranpy.results.CentralServer(args.root_path, args.debug)
server.start(args.sessions_file)

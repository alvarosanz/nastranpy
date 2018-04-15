import os
import argparse


parser = argparse.ArgumentParser(description='Start node server', epilog='You will be asked to login as an admin')
parser.add_argument('host',
                    help='IP address of the central server')
parser.add_argument('--port',
                    help='port being used by the central server')
parser.add_argument('--path', dest='root_path', metavar='ROOT_PATH', default=os.getcwd(),
                    help='folder containing the databases (by default is the current working directory)')
parser.add_argument('--backup', dest='backup', action='store_const',
                    const=True, default=False,
                    help='activate backup mode. In backup mode, the node will perform a backup of all the databases present at the central server')
parser.add_argument('--debug', dest='debug', action='store_const',
                    const=True, default=False,
                    help='activate debug mode')
args = parser.parse_args()

import nastranpy

if not args.port:
    args.port = nastranpy.results.server.SERVER_PORT

nastranpy.results.start_node((args.host, args.port), args.root_path, args.backup, args.debug)

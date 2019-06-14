#!/usr/bin/env python3
import argparse
import sys
sys.path.append("..")

parser = argparse.ArgumentParser(description='ALERCE Flask API.')
parser.add_argument('--debug', action='store_true', help='Debug Logging level')
parser.add_argument('--port', default=8085, type=int, help='Local server port')
args = parser.parse_args()

from psql_api import app
if __name__ == "__main__":
    app.run(debug=args.debug, port=args.port)

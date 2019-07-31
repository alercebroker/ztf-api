from psycopg2 import pool
from .app import config
from .query import parse_filters
from flask import Response, request, Blueprint, current_app, g, jsonify
from io import StringIO
import time

import logging
logger = logging.getLogger(__name__)

download_blueprint = Blueprint(
    'download', __name__, template_folder='templates')

psql_pool = pool.SimpleConnectionPool(0, 20, user=config["DATABASE"]["User"],
                                      password=config["DATABASE"]["Pass"],
                                      host=config["DATABASE"]["Host"],
                                      port=config["DATABASE"]["Port"],
                                      database=config["DATABASE"]["Database"])


@download_blueprint.route("/download", methods=("POST",))
def download():
    def gen():
        for c in 'Hello world!':
            yield c
            time.sleep(0.5)
    return Response(gen(),mimetype='text/csv')

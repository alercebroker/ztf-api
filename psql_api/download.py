from psycopg2 import pool
from .app import config
from .query import parse_filters
from flask import Response, request, Blueprint, current_app, g, jsonify
from io import StringIO

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
    data = [[i for i in range(10)] for j in range(1000000)]
    generator = (cell for row in data
                 for cell in row)
    return Response(generator, mimetype='text/plain', headers={"Content-Disposition":
                                                             "attachment;filename=test.txt"})

from psycopg2 import pool
from .app import config
from .query import parse_filters
from flask import Response, request, Blueprint, current_app, g, jsonify, stream_with_context
import math

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
    data = request.get_json(force=True)
    if "query_parameters" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    if "sortDesc" in data:
        sort_desc = "DESC" if data["sortDesc"] else "ASC"
    else:
        sort_desc = "DESC"

    connection  = psql_pool.getconn()
    cur = connection.cursor(name="ALERCE Big Query Cursor")
    _, sql_query,sql_params = parse_filters(data)
    sql_query = sql_query.as_string(connection)
    cur.execute(sql_query + " limit 1000")
    first_row = cur.fetchone()
    colnames = [desc[0] for desc in cur.description]
    def generateResp():
        csv_headers = ""
        for colname in colnames:
            csv_headers += colname
            csv_headers += ", "
        yield csv_headers + "\n"
        csv_row = ""
        for col in first_row:
            if col == "id":
                continue
            if type(col) is float and col == float("inf"):
                csv_row += "None"
                csv_row += ", "
            elif type(col) is float and math.isnan(col):
                csv_row += "None"
                csv_row += ", "
            else:
                csv_row += str(col)
                csv_row += ", "
        yield csv_row + "\n"
        for row in cur:
            csv_row = ""
            for col in row:
                if col == "id":
                    continue
                if type(col) is float and col == float("inf"):
                    csv_row += "None"
                    csv_row += ", "
                elif type(col) is float and math.isnan(col):
                    csv_row += "None"
                    csv_row += ", "
                else:
                    csv_row += str(col)
                    csv_row += ", "
            yield csv_row + "\n"
        cur.close()
    return Response(stream_with_context(generateResp()), mimetype='text/csv')
    # return Response(gen(), mimetype='text/csv', content_type="text/event-stream")

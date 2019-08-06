from .app import cache, config, psql_pool
from flask import Blueprint, Response, current_app, request, jsonify, stream_with_context
import math
from datetime import datetime, timedelta
from psycopg2 import sql

objects_blueprint = Blueprint('objects', __name__, template_folder='templates')


@objects_blueprint.route("/get_detections", methods=("POST",))
def get_detections():
    #  Check query_parameters
    data = request.get_json(force=True)
    if "oid" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    oid = data["oid"]
    query = sql.SQL("SELECT cast(candid as text) as candid_str, * FROM detections WHERE oid = %s ORDER BY mjd ASC")
    try:
        conn = psql_pool.getconn()
        cur = conn.cursor()
        cur.execute(query, [oid])
        result = {
            "oid": oid,
            "result": {}
        }
        resp = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        alerts = []
        for row in resp:
            row = list(row)
            for j in range(len(row)):
                if type(row[j]) is float and math.isnan(row[j]):
                    row[j] = None
            alert = dict(zip(colnames, row)) if row else None
            alerts.append(alert)
        result["result"]["detections"] = alerts
        cur.close()
        psql_pool.putconn(conn)
        return jsonify(result)

    except:
        current_app.logger.exception(
            "Error getting detections from ({})".format(oid))
        return Response("Something went wrong quering the database", 500)


@objects_blueprint.route("/get_non_detections", methods=("POST",))
def get_non_detections():
    #  Check query_parameters
    data = request.get_json(force=True)
    if "oid" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)
    oid = data["oid"]
    query = sql.SQL("SELECT * FROM non_detections WHERE oid = %s ORDER BY mjd ASC")
    try:
        conn = psql_pool.getconn()
        cur = conn.cursor()
        cur.execute(query, [oid])
        result = {
            "oid": oid,
            "result": {}
        }
        resp = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        alerts = []
        for row in resp:
            alert = dict(zip(colnames, row)) if row else None
            alerts.append(alert)
        result["result"]["non_detections"] = alerts
        cur.close()
        psql_pool.putconn(conn)
        return jsonify(result)

    except:
        current_app.logger.exception(
            "Error getting detections from ({})".format(oid))
        return Response("Something went wrong quering the database", 500)


@objects_blueprint.route("/get_stats", methods=("POST",))
def get_stats():
    #  Check query_parameters
    data = request.get_json(force=True)
    if "oid" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    oid = data["oid"]
    query = sql.SQL("SELECT * FROM objects WHERE oid = %s")
    try:
        conn = psql_pool.getconn()
        cur = conn.cursor()
        cur.execute(query, [oid])
        result = {
            "oid": oid,
            "result": {}
        }
        resp = cur.fetchone()
        colnames = [desc[0] for desc in cur.description]
        colmap = dict(zip(list(range(len(colnames))), colnames))

        obj = {}
        for j, col in enumerate(resp):
            if col == "id":
                continue
            if type(col) is float and col == float("inf"):
                obj[colmap[j]] = None  # 99.0
            elif type(col) is float and math.isnan(col):
                obj[colmap[j]] = None
            else:
                obj[colmap[j]] = col
        result["result"]["stats"] = obj
        cur.close()
        psql_pool.putconn(conn)
        return jsonify(result)
    except:
        current_app.logger.exception(
            "Error getting detections from ({})".format(oid))
        return Response("Something went wrong quering the database", 500)


@objects_blueprint.route("/get_probabilities", methods=("POST",))
def get_probabilities():
    #  Check query_parameters
    data = request.get_json(force=True)
    if "oid" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    oid = data["oid"]
    query_prob = sql.SQL("SELECT * FROM probabilities WHERE oid = %s")
    query_stamp = sql.SQL("SELECT * FROM stamp_classification WHERE oid = %s")
    result = {
        "oid": oid,
        "result": {
            "probabilities": {
                "random_forest": {},
                "early_classifier": {}
            }
        }
    }
    try:
        conn = psql_pool.getconn()
        cur = conn.cursor()
        cur.execute(query_prob, [oid])
        resp_prob = cur.fetchone()
        column_prob = [desc[0] for desc in cur.description]
        cur.close()
        psql_pool.putconn(conn)
    except:
        current_app.logger.exception(
            "Error getting detections from ({})".format(oid))
        resp_prob = "fail"
    try:
        conn = psql_pool.getconn()
        cur = conn.cursor()
        cur.execute(query_stamp, [oid])
        resp_stamp = cur.fetchone()
        column_stamp = [desc[0] for desc in cur.description]
        cur.close()
        psql_pool.putconn(conn)
    except:
        current_app.logger.exception(
            "Error getting detections from ({})".format(oid))
        resp_stamp = "fail"
    if resp_prob == "fail" and resp_stamp == "fail":
        return Response("Something went wrong quering the database", 500)
    else:
        if resp_prob is None or resp_prob == "fail":
            result["result"]["probabilities"]["random_forest"] = {}
        else:
            probs_prob = dict(zip(column_prob, resp_prob))
            result["result"]["probabilities"]["random_forest"] = probs_prob
        if resp_stamp is None or resp_stamp == "fail":
            result["result"]["probabilities"]["early_classifier"] = {}
        else:
            probs_stamp = dict(zip(column_stamp, resp_stamp))
            result["result"]["probabilities"]["early_classifier"] = probs_stamp
        return jsonify(result)


@objects_blueprint.route("/get_features", methods=("POST",))
def get_features():
    #  Check query_parameters
    data = request.get_json(force=True)
    if "oid" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    oid = data["oid"]
    query = sql.SQL("SELECT periodls_1, periodls_2,n_samples_1,n_samples_2 FROM {} WHERE oid = %s".format(
        config["TABLES"]["Features"]))
    try:
        conn = psql_pool.getconn()
        cur = conn.cursor()
        cur.execute(query, [oid])
        result = {
            "oid": oid,
            "result": {}
        }
        resp = cur.fetchone()
        colnames = [desc[0] for desc in cur.description]
        if resp is None:
            result["result"]["period"] = {}
            return jsonify(result)
        features = dict(zip(colnames, resp))
        if features["n_samples_1"] > features["n_samples_2"]:
            features["periodls_2"] = features["periodls_1"]
        else:
            features["periodls_1"] = features["periodls_2"]
        result["result"]["period"] = features
        cur.close()
        psql_pool.putconn(conn)
        return jsonify(result)
    except:
        current_app.logger.exception(
            "Error getting detections from ({})".format(oid))
        return Response("Something went wrong quering the database", 500)


@objects_blueprint.route("/recent_alerts", methods=("POST",))
@cache.memoize(3600)
def recent_alerts():
    data = request.get_json(force=True)
    if "hours" not in data:
        hours = 24
    else:
        hours = data["hours"]
    if "mjd" not in data:
        return Response('{"status": "error", "text": "MJD Needed"}\n', 400)
    else:
        mjd = data["mjd"]
        mjd = mjd - int(hours/24)
    query = sql.SQL("SELECT count(oid) from detections where mjd >= %s")
    try:
        conn = psql_pool.getconn()
        cur = conn.cursor()
        cur.execute(query, [mjd])
        result = {
            "result": {}
        }
        resp = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        count = 0
        for row in resp:
            row = list(row)
            for j in range(len(row)):
                if type(row[j]) is float and math.isnan(row[j]):
                    row[j] = None
            count = dict(zip(colnames, row)) if row else None
        result["result"] = count
        cur.close()
        psql_pool.putconn(conn)
        return jsonify(result)

    except:
        current_app.logger.exception("Error getting recent alerts ")
        return Response("Something went wrong quering the database", 500)


@objects_blueprint.route("/recent_objects", methods=("POST",))
@cache.memoize(3600)
def recent_objects():
    data = request.get_json(force=True)
    if "hours" not in data:
        hours = 24
    else:
        hours = data["hours"]
    if "mjd" not in data:
        return Response('{"status": "error", "text": "MJD Needed"}\n', 400)
    else:
        mjd = data["mjd"]
        mjd = mjd - int(hours/24)
    query = sql.SQL("SELECT count(oid) from objects where lastmjd >= %s")
    try:
        conn = psql_pool.getconn()
        cur = conn.cursor()
        cur.execute(query, [mjd])
        result = {
            "result": {}
        }
        resp = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        count = 0
        for row in resp:
            row = list(row)
            for j in range(len(row)):
                if type(row[j]) is float and math.isnan(row[j]):
                    row[j] = None
            count = dict(zip(colnames, row)) if row else None
        result["result"] = count
        cur.close()
        psql_pool.putconn(conn)
        return jsonify(result)

    except:
        current_app.logger.exception("Error getting recent alerts ")
        return Response("Something went wrong quering the database", 500)


@objects_blueprint.route("/classified_objects", methods=("POST",))
@cache.memoize(3600)
def classified_objects():
    result = {
        "result": {}
    }
    query = sql.SQL(
        "SELECT count(oid) from objects where classxmatch is not null")
    try:
        conn = psql_pool.getconn()
        cur = conn.cursor()
        cur.execute(query)
        resp = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        count = 0
        for row in resp:
            row = list(row)
            for j in range(len(row)):
                if type(row[j]) is float and math.isnan(row[j]):
                    row[j] = None
            count = dict(zip(colnames, row)) if row else None
        result["result"]["xmatch"] = count["count"]
        cur.close()
        psql_pool.putconn(conn)
    except:
        current_app.logger.exception("Error getting classified xmatch objects")
        return Response("Something went wrong quering the database", 500)

    query = sql.SQL("SELECT count(oid) from objects where classrf is not null")
    try:
        conn = psql_pool.getconn()
        cur = conn.cursor()
        cur.execute(query)
        resp = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        count = 0
        for row in resp:
            row = list(row)
            for j in range(len(row)):
                if type(row[j]) is float and math.isnan(row[j]):
                    row[j] = None
            count = dict(zip(colnames, row)) if row else None
        result["result"]["rf"] = count["count"]
        cur.close()
        psql_pool.putconn(conn)
    except:
        current_app.logger.exception(
            "Error getting classified random forest objects")
        return Response("Something went wrong quering the database", 500)

    query = sql.SQL(
        "SELECT count(oid) from objects where classearly is not null")
    try:
        conn = psql_pool.getconn()
        cur = conn.cursor()
        cur.execute(query)
        resp = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        count = 0
        for row in resp:
            row = list(row)
            for j in range(len(row)):
                if type(row[j]) is float and math.isnan(row[j]):
                    row[j] = None
            count = dict(zip(colnames, row)) if row else None
        result["result"]["early"] = count["count"]
        cur.close()
        psql_pool.putconn(conn)
    except:
        current_app.logger.exception(
            "Error getting classified random forest objects")
        return Response("Something went wrong quering the database", 500)

    return jsonify(result)

from .app import cur
from flask import Blueprint,Response,current_app,request,jsonify, stream_with_context
import math

objects_blueprint = Blueprint('objects', __name__, template_folder='templates')

@objects_blueprint.route("/get_detections", methods=("POST",))
def get_detections():
    #Check query_parameters
    data = request.get_json(force=True)
    if "oid" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    oid = data["oid"]
    query = "SELECT * FROM detections WHERE oid = '{}'".format(oid)
    try:
        cur.execute(query,[oid])
        result = {
            "oid" : oid,
            "result" : {}
        }
        resp = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        alerts = []
        for row in resp:
            row = list(row)
            for j in range(len(row)):
                if type(row[j]) is float and math.isnan(row[j]):
                    row[j] = None
            alert = dict(zip(colnames,row)) if row else None
            alerts.append(alert)
        result["result"]["detections"] = alerts
        return jsonify(result)

    except:
        current_app.logger.exception("Error getting detections from ({})".format(oid))
        return Response("Something went wrong quering the database",500)


@objects_blueprint.route("/get_non_detections", methods=("POST",))
def get_non_detections():
    #Check query_parameters
    data = request.get_json(force=True)
    if "oid" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    oid = data["oid"]
    query = "SELECT * FROM non_detections WHERE oid = '{}'".format(oid)
    try:
        cur.execute(query,[oid])
        result = {
            "oid" : oid,
            "result" : {}
        }
        resp = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        alerts = []
        for row in resp:
            alert = dict(zip(colnames,row)) if row else None
            alerts.append(alert)
        result["result"]["non_detections"] = alerts
        return jsonify(result)

    except:
        current_app.logger.exception("Error getting detections from ({})".format(oid))
        return Response("Something went wrong quering the database",500)

@objects_blueprint.route("/get_stats", methods=("POST",))
def get_stats():
    #Check query_parameters
    data = request.get_json(force=True)
    if "oid" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    oid = data["oid"]
    query = "SELECT * FROM objects WHERE oid = '{}'".format(oid)
    try:
        cur.execute(query,[oid])
        result = {
            "oid" : oid,
            "result" : {}
        }
        resp = cur.fetchone()
        colnames = [desc[0] for desc in cur.description]
        colmap = dict(zip(list(range(len(colnames))),colnames))

        obj = {}
        for j,col in enumerate(resp):
            if col == "id":
                continue
            if type(col) is float and col == float("inf"):
                obj[colmap[j]] = None#99.0
            elif type(col) is float and math.isnan(col):
                obj[colmap[j]] = None
            else:
                obj[colmap[j]] = col
        result["result"]["stats"] = obj
        return jsonify(result)

        return Response(stream_with_context(generateResp()), content_type='application/json')
    except:
        current_app.logger.exception("Error getting detections from ({})".format(oid))
        return Response("Something went wrong quering the database",500)


@objects_blueprint.route("/get_probabilities", methods=("POST",))
def get_probabilities():
    #Check query_parameters
    data = request.get_json(force=True)
    if "oid" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    oid = data["oid"]
    query = "SELECT * FROM probabilities WHERE oid = '{}'".format(oid)
    try:
        cur.execute(query,[oid])
        result = {
            "oid" : oid,
            "result" : {}
        }
        resp = cur.fetchone()
        colnames = [desc[0] for desc in cur.description]
        if resp is None:
            result["result"]["probabilities"] = {}
            return jsonify(result)
        probs = dict(zip(colnames,resp))
        result["result"]["probabilities"] = probs
        return jsonify(result)
    except:
        current_app.logger.exception("Error getting detections from ({})".format(oid))
        return Response("Something went wrong quering the database",500)

@objects_blueprint.route("/get_features", methods=("POST",))
def get_features():
    #Check query_parameters
    data = request.get_json(force=True)
    if "oid" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    oid = data["oid"]
    query = "SELECT periodls_1, periodls_2,n_samples_1,n_samples_2 FROM features WHERE oid = '{}'".format(oid)
    try:
        cur.execute(query,[oid])
        result = {
            "oid" : oid,
            "result" : {}
        }
        resp = cur.fetchone()
        colnames = [desc[0] for desc in cur.description]
        if resp is None:
            result["result"]["period"] = {}
            return jsonify(result)
        features = dict(zip(colnames,resp))
        if features["n_samples_1"] > features["n_samples_2"]:
            features["periodls_2"] = features["periodls_1"]
        else:
            features["periodls_1"] = features["periodls_2"]

        result["result"]["period"] = features
        return jsonify(result)
    except:
        current_app.logger.exception("Error getting detections from ({})".format(oid))
        return Response("Something went wrong quering the database",500)

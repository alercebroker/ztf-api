from .app import cur
from flask import Blueprint,Response,current_app,request, stream_with_context

objects_blueprint = Blueprint('objects', __name__, template_folder='templates')

@objects_blueprint.route("/get_detections", methods=("POST",))
def get_detections():
    #Check query_parameters
    data = request.get_json(force=True)
    if "id" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    id = data["id"]
    query = "SELECT * FROM detections WHERE id = '{}'".format(id)
    try:
        cur.execute(query,[id])
        def generateResp():
            while True:
                resp = cur.fetchmany(20)
                if not resp:
                    break

                for row in resp:
                    row = [str(i) for i in row]
                    yield ",".join(row)+"\n"

        return Response(stream_with_context(generateResp()), content_type='application/json')
    except:
        current_app.logger.exception("Error getting detections from ({})".format(id))
        return Response("Something went wrong quering the database",500)


@objects_blueprint.route("/get_non_detections", methods=("POST",))
def get_non_detections():
    #Check query_parameters
    data = request.get_json(force=True)
    if "id" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    id = data["id"]
    query = "SELECT * FROM non_detections WHERE id = '{}'".format(id)
    try:
        cur.execute(query,[id])
        def generateResp():
            while True:
                resp = cur.fetchmany(20)
                if not resp:
                    break

                for row in resp:
                    row = [str(i) for i in row]
                    yield ",".join(row)+"\n"

        return Response(stream_with_context(generateResp()), content_type='application/json')
    except:
        current_app.logger.exception("Error getting detections from ({})".format(id))
        return Response("Something went wrong quering the database",500)

@objects_blueprint.route("/get_stats", methods=("POST",))
def get_stats():
    #Check query_parameters
    data = request.get_json(force=True)
    if "id" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    id = data["id"]
    query = "SELECT * FROM objects WHERE id = '{}'".format(id)
    try:
        cur.execute(query,[id])
        def generateResp():
            while True:
                resp = cur.fetchmany(20)
                if not resp:
                    break

                for row in resp:
                    row = [str(i) for i in row]
                    yield ",".join(row)+"\n"

        return Response(stream_with_context(generateResp()), content_type='application/json')
    except:
        current_app.logger.exception("Error getting detections from ({})".format(id))
        return Response("Something went wrong quering the database",500)


@objects_blueprint.route("/get_probabilities", methods=("POST",))
def get_probabilities():
    #Check query_parameters
    data = request.get_json(force=True)
    if "id" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    id = data["id"]
    query = "SELECT * FROM probabilities WHERE id = '{}'".format(id)
    try:
        cur.execute(query,[id])
        def generateResp():
            while True:
                resp = cur.fetchmany(20)
                if not resp:
                    break

                for row in resp:
                    row = [str(i) for i in row]
                    yield ",".join(row)+"\n"

        return Response(stream_with_context(generateResp()), content_type='application/json')
    except:
        current_app.logger.exception("Error getting detections from ({})".format(id))
        return Response("Something went wrong quering the database",500)

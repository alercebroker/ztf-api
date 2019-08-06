from .app import conn, cache
from flask import Blueprint, Response, current_app, request, jsonify, stream_with_context
import P4J
import pandas as pd
variable_blueprint = Blueprint('variable', __name__, template_folder='templates')

@variable_blueprint.route('/get_periodogram', methods=("POST",))
def get_periodogram():
    data = request.get_json(force=True)

    oid = data["oid"]
    query = "SELECT mjd,magpsf_corr,sigmapsf_corr,fid FROM detections WHERE oid = '{}'".format(oid)
    try:
        periodograms = []
        df = pd.read_sql(query, conn)
        for fid,data in df.groupby("fid"):
            my_per = P4J.periodogram()
            my_per.set_data(data.mjd.values,data.magpsf_corr.values,data.sigmapsf_corr.values)
            my_per.frequency_grid_evaluation()
            my_per.finetune_best_frequencies()
            freq,per = my_per.get_periodogram()
            periodogram = {"fid":fid, "frequencies": freq.tolist(), "periods": per.tolist() }
            try:
                fbest, pbest = my_per.get_best_frequencies()
                objperiod = 1. / fbest
                periodogram["best_period"] =  objperiod.tolist()
                periodograms.append(periodogram)
            except AttributeError:
                periodograms.append(periodogram)

        result = {
            "oid": oid,
            "periodograms": periodograms
        }
        return jsonify(result)


    except:
        current_app.logger.exception(
            "Error getting detections from ({})".format(oid))
        return Response("Something went wrong quering the database", 500)

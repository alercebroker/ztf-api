from .app import cache, config
from flask import Blueprint, Response, current_app, request, jsonify, stream_with_context
from io import StringIO
from astropy import units as u
from astropy.coordinates import SkyCoord

import pandas as pd
import numpy as np
import requests
import time


external_blueprint = Blueprint('external', __name__, template_folder='templates')


def dourl(searchweb, searchoptions):
    url = searchweb
    for key in searchoptions.keys():
        url = "%s&%s=%s" % (url, key, searchoptions[key])
    return url


def get_tns_df(searchweb,searchoptions):
    urlpage = dourl(searchweb,searchoptions)
    t0 = time.time()
    s = requests.Session()
    response = s.get(urlpage)
    response.close()
    df = pd.read_csv(StringIO(response.text))
    df.sort_values("Discovery Date (UT)",ascending=False)
    return df



@external_blueprint.route("/get_alerce_tns")
@cache.memoize(60*60)
def get_alerce_tns():
    searchweb = "https://wis-tns.weizmann.ac.il/search?"
    searchoptions = {
        "groupid[]":74,
        "num_page" : 500,  # number of rows per page
        "internal_name" : "ZTF",
        "classified_sne" : 0,
        "unclassified_at": 0,
        "discovered_period_value": 1,
        "discovered_period_units": "months",
        "format" : "csv",
        "display[remarks]":1}

    all_alerce = get_tns_df(searchweb,searchoptions)

    dict_candidates = []
    dict_classified = []

    for _,row in all_alerce.iterrows():
        coords = SkyCoord("%s %s" % (row['RA'], row['DEC']), unit = (u.hourangle, u.deg), frame = 'fk5')
        row["RA"] = coords.ra.value
        row["DEC"] = coords.dec.value
        values = [None if (type(r) is float and np.isnan(r)) else r for r in row.values]
        if type(row["Obj. Type"]) is not str:
            dict_classified.append(dict(zip(row.keys(),values)))
        else:
            dict_candidates.append(dict(zip(row.keys(),values)))

    result = {
        "results":{
            "candidates": dict_candidates,
            "classified": dict_classified
        }
    }

    return jsonify(result)



# atexit.register(lambda: scheduler.shutdown(wait=False))

from .app import cache, config, psql_pool
from flask import Blueprint, Response, current_app, request, jsonify, stream_with_context
from io import StringIO
from astropy import units as u
from astropy.coordinates import SkyCoord

import pandas as pd
import numpy as np
import requests


external_blueprint = Blueprint('external', __name__, template_folder='templates')


def dourl(searchweb, searchoptions):
    url = searchweb
    for key in searchoptions.keys():
        url = "%s&%s=%s" % (url, key, searchoptions[key])
    return url

def get_tns_df(searchweb,searchoptions):
    urlpage = dourl(searchweb,searchoptions)
    s = requests.Session()
    response = s.get(urlpage)
    response.close()
    df = pd.read_csv(StringIO(response.text))
    urls = [f"http://alerce.online/object/{oid}" for oid in df['Disc. Internal Name']]
    df['url'] = urls
    df.sort_values("Discovery Date (UT)",ascending=False)
    return df

@external_blueprint.route("/get_alerce_tns")
@cache.memoize(60*60*24)
def get_alerce_tns():
    searchweb = "https://wis-tns.weizmann.ac.il/search?"
    searchoptions = {
        "groupid[]":74,
        "num_page" : "1000",  # number of rows per page
        "internal_name" : "ZTF",
        "classified_sne" : 1,
        "unclassified_at": 0,
        "format" : "csv",
        "display[remarks]":1}

    classified_searchoptions = searchoptions.copy()
    candidates_searchoptions = searchoptions.copy()

    candidates_searchoptions["classified_sne"] = 0
    candidates_searchoptions["unclassified_at"] = 1

    candidates = get_tns_df(searchweb,candidates_searchoptions)
    classified = get_tns_df(searchweb,classified_searchoptions)

    dict_candidates = []
    dict_classified = []

    for _,row in candidates.iterrows():
        coords = SkyCoord("%s %s" % (row['RA'], row['DEC']), unit = (u.hourangle, u.deg), frame = 'fk5')
        row["RA"] = coords.ra.value
        row["DEC"] = coords.dec.value
        values = [None if (type(r) is float and np.isnan(r)) else r for r in row.values]
        dict_candidates.append(dict(zip(row.keys(),values)))

    for _,row in classified.iterrows():
        coords = SkyCoord("%s %s" % (row['RA'], row['DEC']), unit = (u.hourangle, u.deg), frame = 'fk5')
        row["RA"] = coords.ra.value
        row["DEC"] = coords.dec.value
        values = [None if (type(r) is float and np.isnan(r)) else r for r in row.values]
        dict_classified.append(dict(zip(row.keys(),values)))

    result = {
        "results":{
            "candidates": dict_candidates,
            "classified": dict_classified
        }
    }

    return jsonify(result)

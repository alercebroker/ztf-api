from psycopg2 import sql
from .app import config
from flask import Response,stream_with_context,request,Blueprint,current_app,g,jsonify

from astropy import units as u
import numpy as np
import math
import re
import logging
logger = logging.getLogger(__name__)

query_blueprint = Blueprint('query', __name__, template_folder='templates')

class_map = {
    "agn":18,
    "vs":20,
    "asteroid":21,
    "bogus":22,
    "sn":20,
    "sn ia":10,
    "sn ibc":11,
    "sn ii":12,
    "sn iin":13,
    "slsn":14,
    "eb/sd/d":15,
    "eb/c":16,
    "periodic/other":17,
    "cv":9,
    "nova":9,
    "cv/nova":9,
    "blazar":8,
    "agn i":7,
    "dsct":2,
    "ceph":0,
    "lpv":4,
    "rrl":5
}

def parse_oid(oid):
    x = re.compile("^ZTF\d\d[a-zA-Z0-9]*")
    res = x.match(oid)
    return res.group()


def parse_filters(data):
    #Base SQL statement
    sql_str = "SELECT * FROM objects"
    count_sql_str = sql_str.replace("*","COUNT(*)")

    #Array of filters
    sql_filters = []
    sql_params = []
    if "filters" in data["query_parameters"]:
        filters = data["query_parameters"]["filters"]

        for i,filter in enumerate(filters):
            #OID Filter
            if "oid" == filter:
                sql_filters.append("oid=%s")
                sql_params.append(parse_oid(filters["oid"]))

            #NOBS Filter
            if "nobs" == filter:
                if "min" in filters["nobs"]:
                    sql_filters.append(" nobs >= %s")
                    sql_params.append(filters["nobs"]["min"])
                if "max" in filters["nobs"]:
                    sql_filters.append(" nobs <= %s")
                    sql_params.append(filters["nobs"]["max"])

            # CLASS FILTER
            if filter.startswith("class"):
                if "classified" == filters[filter]:
                    sql_filters.append("{} is not null".format(filter))
                    # sql_params.append(filter)
                elif "not classified" == filters[filter]:
                    sql_filters.append("{} is null".format(filter))
                    sql_params.append(filter)
                else:
                    c = filters[filter]
                    if type(c) is int:
                        sql_filters.append("{}=%s".format(filter))
                        sql_params.append(c)
                    elif type(c) is str:
                        c = class_map[c.lower()]
                        sql_filters.append("{}=%s".format(filter))
                        sql_params.append(c)
                    elif type(c) is list:
                        cnew = []
                        for cs in c:
                            if type(cs) is str:
                                cnew.append(class_map[cs.lower()])
                            else:
                                cnew.append(cs)
                        c = cnew
                        sql_filters.append(" {} IN ({}) ".format(filter,",".join(["%s"]*len(c))))
                        sql_params.extend(c)
            if filter.startswith("pclass"):
                sql_filters.append("{}>= %s".format(filter))
                sql_params.append(filters[filter])


    if "coordinates" in data["query_parameters"]:
        filters = data["query_parameters"]
        #Coordinates Filter
        if "ra" not in filters["coordinates"] or "dec" not in filters["coordinates"] or "sr" not in filters["coordinates"]:
            return Response('{"status": "error", "text": "Malformed Coordinates parameters"}\n', 400)

        #Transorming to degrees
        arcsec = float(filters["coordinates"]["sr"]) * u.arcsec
        deg = arcsec.to(u.deg)
        deg = deg.value

        ra = float(filters["coordinates"]["ra"])
        dec = float(filters["coordinates"]["dec"])

        #Adding "Square" coordinates filter
        sql_filters.append(" q3c_radial_query(meanra,meandec,%s,%s,%s) ")
        sql_params.extend((ra,dec,deg))

    if "dates" in data["query_parameters"]:
        filters = {"dates": {}}
        if "firstmjd" in data["query_parameters"]["dates"]:
            firstmjd = data["query_parameters"]["dates"]["firstmjd"]

            if "min" in firstmjd:
                sql_filters.append(" firstmjd >= %s " )
                sql_params.append(firstmjd["min"])
            if "max" in firstmjd:
                sql_filters.append(" firstmjd <= %s ")
                sql_params.append(firstmjd["min"])

    if "magnitude" in data["query_parameters"]:
        for band in data["query_parameters"]["magnitude"].keys():
            sql_filters.append(" mean_magpsf_"+band+" >= %s")
            sql_params.append(data["query_parameters"]["magnitude"][band]["mean"][0])
            sql_filters.append(" mean_magpsf_"+band+" <= %s")
            sql_params.append(data["query_parameters"]["magnitude"][band]["mean"][1])
            sql_filters.append(" min_magpsf_"+band+" >= %s")
            sql_params.append(data["query_parameters"]["magnitude"][band]["min"][0])
            sql_filters.append(" min_magpsf_"+band+" <= %s")
            sql_params.append(data["query_parameters"]["magnitude"][band]["min"][1])
            sql_filters.append(" max_magpsf_"+band+" >= %s")
            sql_params.append(data["query_parameters"]["magnitude"][band]["max"][0])
            sql_filters.append(" max_magpsf_"+band+" <= %s")
            sql_params.append(data["query_parameters"]["magnitude"][band]["max"][1])

    #If there are filters add to sql
    if len(sql_filters) > 0:
        fields = sql_filters[0]

        for field in sql_filters[1:]:
            fields += ' AND '
            fields += field

        sql_str = sql_str + " WHERE " + fields
        count_sql_str = count_sql_str + " WHERE " + fields
    return count_sql_str,sql_str, sql_params

@query_blueprint.route("/query",methods=("POST",))
def query():
    #Check query_parameters
    data = request.get_json(force=True)
    if "query_parameters" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    #Checking other parameters
    records_per_pages = int(data["records_per_pages"]) if "records_per_pages" in data else 20
    page = int(data["page"]) if "page" in data else 1
    try:
        row_number = int(data["total"])
    except:
        if "total" in data:
            del data["total"]
        row_number = None

    #row_number = int(data["total"]) if "total" in data else None
    num_pages = int(np.ceil(row_number/records_per_pages)) if "total" in data else None
    sort_by = data["sortBy"] if "sortBy" in data else "nobs"
    sort_by = sort_by if sort_by is not None else "nobs"
    if "sortDesc" in data:
        sort_desc = "DESC" if data["sortDesc"] else "ASC"
    else:
        sort_desc = "DESC"
    count_query,sql_query,sql_params = parse_filters(data)

    connection  = g.db


    if row_number is None:
        cur = connection.cursor(name="ALERCE Big Query Counter Cursor")
        current_app.logger.debug(count_query)
        cur.execute(count_query, sql_params)
        row_number = cur.fetchone()[0]
        num_pages = int(np.ceil(row_number/records_per_pages))
        cur.close()
    order_query = " ORDER BY {} ".format(sort_by) + \
                  "{} ".format(sort_desc) + "OFFSET %s LIMIT %s"
    sql_params.extend([(page-1)*records_per_pages,records_per_pages])
    sql_query = sql_query + order_query
    current_app.logger.debug(sql_query)
    cur = connection.cursor(name="ALERCE Big Query Cursor")
    current_app.logger.debug(sql)
    cur.execute(sql_query,sql_params)

    current_app.logger.debug("Rows Returned:{}".format(row_number))
    #Generating json response
    def generateResp():
        colnames = None
        result = {
                "total":row_number,
                "num_pages": num_pages,
                "page": page,
                "result" : {}
        }
        resp = cur.fetchall()
        if colnames is None:
            colnames = [desc[0] for desc in cur.description]
            colmap = dict(zip(list(range(len(colnames))),colnames))
            for i in range(len(colnames)):
                if colmap[i] == "oid":
                    idPosition = i
                    break
            for row in resp:
                obj = {}
                for j,col in enumerate(row):
                    if col == "id":
                        continue
                    if type(col) is float and col == float("inf"):
                        obj[colmap[j]] = None#99.0
                    elif type(col) is float and math.isnan(col):
                        obj[colmap[j]] = None
                    else:
                        obj[colmap[j]] = col
                result["result"][row[idPosition]] = obj
        cur.close()
        return result

    result = generateResp()

    return jsonify(result)


@query_blueprint.route("/query_features",methods=("POST",))
def query_features():
    #Check query_parameters
    data = request.get_json(force=True)
    if "query_parameters" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    #Checking other parameters
    records_per_pages = int(data["records_per_pages"]) if "records_per_pages" in data else 20
    page = int(data["page"]) if "page" in data else 1
    try:
        row_number = int(data["total"])
    except:
        if "total" in data:
            del data["total"]

        row_number = None
    #row_number = int(data["total"]) if "total" in data else None
    num_pages = int(np.ceil(row_number/records_per_pages)) if "total" in data else None
    sort_by = data["sortBy"] if "sortBy" in data else "nobs"
    sort_by = sort_by if sort_by is not None else "nobs"
    if "sortDesc" in data:
        sort_desc = "DESC" if data["sortDesc"] else "ASC"
    else:
        sort_desc = "DESC"
    count_query,sql_query,sql_params = parse_filters(data)

    count_query = count_query.split("WHERE")
    sql_query = sql_query.split("WHERE")

    count_query[0] += " INNER JOIN features_v2 ON objects.oid=features_v2.oid "
    sql_query[0] += " INNER JOIN features_v2 ON objects.oid=features_v2.oid "

    current_app.logger.debug(count_query)
    current_app.logger.debug(sql_query)


    count_query = " WHERE ".join(count_query)
    sql_query = " WHERE ".join(sql_query)

    connection  = g.db


    if row_number is None:
        cur = connection.cursor(name="ALERCE Big Query Counter Cursor")
        current_app.logger.debug(count_query)
        cur.execute(count_query, sql_params)
        row_number = cur.fetchone()[0]
        num_pages = int(np.ceil(row_number/records_per_pages))
        cur.close()
    order_query = " ORDER BY {} ".format(sort_by) + \
                  "{} ".format(sort_desc) + "OFFSET %s LIMIT %s"
    sql_params.extend([(page-1)*records_per_pages,records_per_pages])
    sql_query = sql_query + order_query
    current_app.logger.debug(sql_query)
    cur = connection.cursor(name="ALERCE Big Query Cursor")
    current_app.logger.debug(sql)
    cur.execute(sql_query,sql_params)

    current_app.logger.debug("Rows Returned:{}".format(row_number))
    #Generating json response
    def generateResp():
        colnames = None
        result = {
                "total":row_number,
                "num_pages": num_pages,
                "page": page,
                "result" : {}
        }
        resp = cur.fetchall()
        if colnames is None:
            colnames = [desc[0] for desc in cur.description]
            colmap = dict(zip(list(range(len(colnames))),colnames))
            for i in range(len(colnames)):
                if colmap[i] == "oid":
                    idPosition = i
                    break
            for row in resp:
                obj = {}
                for j,col in enumerate(row):
                    if col == "id":
                        continue
                    if type(col) is float and col == float("inf"):
                        obj[colmap[j]] = None#99.0
                    elif type(col) is float and math.isnan(col):
                        obj[colmap[j]] = None
                    else:
                        obj[colmap[j]] = col
                result["result"][row[idPosition]] = obj
        cur.close()
        return result

    result = generateResp()

    return jsonify(result)


@query_blueprint.route("/get_sql",methods=("POST",))
def get_sql():
    data = request.get_json(force=True)
    if "query_parameters" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    _, sql, params = parse_filters(data)
    connection  = g.db
    sql = sql.replace('oid=%s',"oid='%s'")
    sql = sql.replace('%s','{}')

    return sql.format(*params)

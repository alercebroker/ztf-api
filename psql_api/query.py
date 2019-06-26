from psycopg2 import pool
from .app import config
from flask import Response,stream_with_context,request,Blueprint,current_app,g,jsonify

from astropy import units as u
import numpy as np
import math

query_blueprint = Blueprint('query', __name__, template_folder='templates')

psql_pool = pool.SimpleConnectionPool(1, 20,user = config["DATABASE"]["User"],
                                              password = config["DATABASE"]["Pass"],
                                              host = config["DATABASE"]["Host"],
                                              port = config["DATABASE"]["Port"],
                                              database = config["DATABASE"]["Database"])


def parse_filters(data):
    #Base SQL statement
    sql = "SELECT * FROM objects"
    #Array of filters
    sql_filters = []

    if "filters" in data["query_parameters"]:
        filters = data["query_parameters"]["filters"]

        for i,filter in enumerate(filters):
            #OID Filter
            if "oid" == filter:
                sql_filters.append(" oid='{}'".format(filters["oid"] ))

            #NOBS Filter
            if "nobs" == filter:
                if "min" in filters["nobs"]:
                    sql_filters.append(" nobs >= {}".format(filters["nobs"]["min"]))
                if "max" in filters["nobs"]:
                    sql_filters.append(" nobs <= {}".format(filters["nobs"]["max"]))
            # CLASS FILTER
            if filter.startswith("class"):
                if "classified" == filters[filter]:
                    sql_filters.append(" {} is not null".format(filter))
                if "not classified" == filters[filter]:
                    sql_filters.append(" {} is null".format(filter))
                if isinstance(filters[filter], int):
                    sql_filters.append(" {} = {}".format(filter, filters[filter]))
            if filter.startswith("pclass"):
                sql_filters.append(" {} >= {}".format(filter, filters[filter]))

    if "coordinates" in data["query_parameters"]:
        filters = data["query_parameters"]
        #Coordinates Filter
        if "ra" not in filters["coordinates"] or "dec" not in filters["coordinates"] or "rs" not in filters["coordinates"]:
            return Response('{"status": "error", "text": "Malformed Coordinates parameters"}\n', 400)

        #Transorming to degrees
        arcsec = float(filters["coordinates"]["rs"]) * u.arcsec
        deg = arcsec.to(u.deg)
        deg = deg.value

        ra = float(filters["coordinates"]["ra"])
        dec = float(filters["coordinates"]["dec"])

        #Adding "Square" coordinates filter
        sql_filters.append(" meanra BETWEEN {} AND {} AND meandec BETWEEN {} AND {}".format(ra-deg,ra+deg,dec-deg,dec+deg))


    if "dates" in data["query_parameters"]:
        filters = {"dates": {}}
        if "firstmjd" in data["query_parameters"]["dates"]:
            firstmjd = data["query_parameters"]["dates"]["firstmjd"]

            if "min" in firstmjd:
                sql_filters.append( " firstmjd >= {} ".format(firstmjd["min"]) )
            if "max" in firstmjd:
                sql_filters.append( " firstmjd <= {} ".format(firstmjd["max"]) )

    #If there are filters add to sql
    if len(sql_filters) > 0:
        sql_filters_str = " AND ".join(sql_filters)
        sql += " WHERE {}".format(sql_filters_str)

    return sql

@query_blueprint.route("/query",methods=("POST",))
def query():
    #Check query_parameters
    data = request.get_json(force=True)
    if "query_parameters" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    #Checking other parameters
    records_per_pages = int(data["records_per_pages"]) if "records_per_pages" in data else 20
    page = int(data["page"]) if "page" in data else 1
    row_number = int(data["total"]) if "total" in data else None
    num_pages = int(np.ceil(row_number/records_per_pages)) if "total" in data else None
    sort_by = data["sortBy"] if "sortBy" in data else "nobs"
    if "sortDesc" in data:
        sort_desc = "DESC" if data["sortDesc"] else "ASC"
    else:
        sort_desc = "DESC"
    sql = parse_filters(data)

    connection  = psql_pool.getconn()
    if row_number is None:
        cur = connection.cursor(name="ALERCE Big Query Counter Cursor")
        current_app.logger.debug(sql.replace("*","COUNT(*)"))
        cur.execute(sql.replace("*","COUNT(*)"))
        row_number = cur.fetchone()[0]
        num_pages = int(np.ceil(row_number/records_per_pages))
        cur.close()
    sql += " ORDER BY {} {} OFFSET {} LIMIT {} ".format(sort_by,sort_desc,(page-1)*records_per_pages, records_per_pages)
    cur = connection.cursor(name="ALERCE Big Query Cursor")
    current_app.logger.debug(sql)
    cur.execute(sql)
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
    psql_pool.putconn(connection)
    return jsonify(result)

@query_blueprint.route("/get_sql",methods=("POST",))
def get_sql():
    data = request.get_json(force=True)
    if "query_parameters" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)
    return parse_filters(data)

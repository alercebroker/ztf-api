from psycopg2 import sql
from .app import config, classes, psql_pool
from flask import Response,stream_with_context,request,Blueprint,current_app,g,jsonify

from astropy import units as u
import numpy as np
import math

import logging
logger = logging.getLogger(__name__)

query_blueprint = Blueprint('query', __name__, template_folder='templates')

    

def parse_filters(data):

    #Base SQL statement
    sql_str = "SELECT * FROM objects"
    count_sql_str = sql.SQL(sql_str.replace("*","COUNT(*)"))
    sql_str = sql.SQL(sql_str)


    #Array of filters
    sql_filters = []
    sql_params = []
    if "filters" in data["query_parameters"]:
        filters = data["query_parameters"]["filters"]

        for i,filter in enumerate(filters):
            #OID Filter
            if "oid" == filter:
                sql_filters.append(sql.SQL(" oid=%s"))
                sql_params.append(filters["oid"])

            #NOBS Filter
            if "nobs" == filter:
                if "min" in filters["nobs"]:
                    sql_filters.append(sql.SQL(" nobs >= %s"))
                    sql_params.append(filters["nobs"]["min"])
                if "max" in filters["nobs"]:
                    sql_filters.append(sql.SQL(" nobs <= %s"))
                    sql_params.append(filters["nobs"]["max"])

            # CLASS FILTER
            if filter.startswith("class"):
                if "classified" == filters[filter]:
                    sql_filters.append(sql.SQL("{} is not null").format(sql.Identifier(filter)))
                    # sql_params.append(filter)
                elif "not classified" == filters[filter]:
                    sql_filters.append(sql.SQL("{} is null").format(sql.Identifier(filter)))
                    sql_params.append(filter)
                else:
                    c = filters[filter]
                    sql_filters.append(sql.SQL("{}=%s").format(sql.Identifier(filter)))
                    sql_params.append(c)
            if filter.startswith("pclass"):
                sql_filters.append(sql.SQL("{}>= %s").format(sql.Identifier(filter)))
                sql_params.append(filters[filter])


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
        sql_filters.append(sql.SQL(" meanra BETWEEN %s AND %s AND meandec BETWEEN %s AND %s "))
        sql_params.extend((ra-deg,ra+deg,dec-deg,dec+deg))

    if "dates" in data["query_parameters"]:
        filters = {"dates": {}}
        if "firstmjd" in data["query_parameters"]["dates"]:
            firstmjd = data["query_parameters"]["dates"]["firstmjd"]

            if "min" in firstmjd:
                sql_filters.append( sql.SQL(" firstmjd >= %s " ))
                sql_params.append(firstmjd["min"])
            if "max" in firstmjd:
                sql_filters.append(sql.SQL( " firstmjd <= %s "))
                sql_params.append(firstmjd["min"])

    #If there are filters add to sql
    if len(sql_filters) > 0:
        fields = sql_filters[0]

        for field in sql_filters[1:]:
            fields += sql.SQL(' AND ')
            fields += field

        sql_str = sql_str + sql.SQL(" WHERE ") + fields
        count_sql_str = count_sql_str + sql.SQL(" WHERE ") + fields
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
    row_number = int(data["total"]) if "total" in data else None
    num_pages = int(np.ceil(row_number/records_per_pages)) if "total" in data else None
    sort_by = data["sortBy"] if "sortBy" in data else "nobs"
    sort_by = sort_by if sort_by is not None else "nobs"
    if "sortDesc" in data:
        sort_desc = "DESC" if data["sortDesc"] else "ASC"
    else:
        sort_desc = "DESC"
    count_query,sql_query,sql_params = parse_filters(data)

    connection  = psql_pool.getconn()
    count_query = count_query.as_string(connection)


    if row_number is None:
        cur = connection.cursor(name="ALERCE Big Query Counter Cursor")
        current_app.logger.debug(count_query)
        cur.execute(count_query, sql_params)
        row_number = cur.fetchone()[0]
        num_pages = int(np.ceil(row_number/records_per_pages))
        cur.close()
    order_query = sql.SQL("ORDER BY {} ").format(sql.Identifier(sort_by)) + \
                  sql.SQL("{} ".format(sort_desc)) + sql.SQL("OFFSET %s LIMIT %s")
    sql_params.extend([(page-1)*records_per_pages,records_per_pages])
    sql_query = sql_query + order_query
    sql_query = sql_query.as_string(connection) 
    current_app.logger.debug(sql_query)
    cur = connection.cursor(name="ALERCE Big Query Cursor")
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
    psql_pool.putconn(connection)
    return jsonify(result)

@query_blueprint.route("/get_sql",methods=("POST",))
def get_sql():
    data = request.get_json(force=True)
    if "query_parameters" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    _, sql, params = parse_filters(data)
    connection  = psql_pool.getconn()
    sql = sql.as_string(connection)
    sql = sql.replace('oid=%s',"oid='%s'")
    sql = sql.replace('%s','{}')
    psql_pool.putconn(connection)
    return sql.format(*params)

from psycopg2 import pool
from .app import config
from flask import Response,stream_with_context,request,Blueprint,current_app,g

from astropy import units as u
import numpy as np

query_blueprint = Blueprint('query', __name__, template_folder='templates')

psql_pool = pool.SimpleConnectionPool(1, 20,user = config["DATABASE"]["User"],
                                              password = config["DATABASE"]["Pass"],
                                              host = config["DATABASE"]["Host"],
                                              port = config["DATABASE"]["Port"],
                                              database = config["DATABASE"]["Database"])

@query_blueprint.route("/query",methods=("POST",))
def query():
    #Check query_parameters
    data = request.get_json(force=True)
    if "query_parameters" not in data:
        return Response('{"status": "error", "text": "Malformed Query"}\n', 400)

    records_per_pages = int(data["records_per_pages"]) if "records_per_pages" in data else 20
    page = int(data["page"]) if "page" in data else 1
    row_number = int(data["total"]) if "total" in data else None
    num_pages = np.ceil(row_number/records_per_pages).astype(np.int) if "total" in data else None

    sql = "SELECT * FROM objects"

    if "filters" in data["query_parameters"]:
        filters = data["query_parameters"]["filters"]

        if len(filters) > 0:
            sql += " WHERE "
            for i,filter in enumerate(filters):

                #OID Filter
                if "oid" == filter:
                    sql += " oid='{}'".format(filters["oid"] )

                #NOBS Filter
                if "nobs" == filter:
                    if "min" in filters["nobs"]:
                        sql += " nobs >= {}".format(filters["nobs"]["min"])
                    if len(filters["nobs"]) == 2:
                        sql += " AND "
                    if "max" in filters["nobs"]:
                        sql += " nobs <= {}".format(filters["nobs"]["max"])

                #DATES FILTER
                if "dates" == filter:
                    for j,field in enumerate(filters["dates"]):
                        if field == "firstjd":
                            sql += " firstmjd >= {}".format(filters["dates"]["firstjd"])

                        if field == "lastjd":
                            sql += " lastmjd <= {}".format(filters["dates"]["lastjd"])


                        if len(filters["dates"]) > 1 and j != len(filters["dates"])-1:
                            sql += " AND "

                        if field == "deltajd":
                            deltajd_filter = filters["dates"]["deltajd"]
                            if "min" in deltajd_filter:
                                sql += " deltajd >= {}".format(deltajd_filter["min"])
                            if len(deltajd_filter) == 2:
                                sql += " AND "
                            if "max" in deltajd_filter:
                                sql += " deltajd <= {}".format(deltajd_filter["max"])

                if "coordinates" == filter:
                    if "ra" not in filters["coordinates"] or "dec" not in filters["coordinates"] or "rs" not in filters["coordinates"]:
                        return Response('{"status": "error", "text": "Malformed Coordinates parameters"}\n', 400)

                    arcsec = float(filters["coordinates"]["rs"]) * u.arcsec
                    deg = arcsec.to(u.deg)
                    deg = deg.value

                    ra = float(filters["coordinates"]["ra"])
                    dec = float(filters["coordinates"]["dec"])



                    sql += " meanra BETWEEN {} AND {} AND meandec BETWEEN {} AND {}".format(ra-deg,ra+deg,dec-deg,dec+deg)

                if len(filters) > 1 and i != len(filters)-1:
                    sql+= " AND "


    current_app.logger.debug(sql)
    connection  = psql_pool.getconn()
    if row_number is None:
        cur = connection.cursor(name="ALERCE Big Query Counter Cursor")
        current_app.logger.debug(sql.replace("*","COUNT(*)"))
        cur.execute(sql.replace("*","COUNT(*)"))
        row_number = cur.fetchone()[0]
        num_pages = np.ceil(row_number/records_per_pages).astype(np.int)
        cur.close()

    sql += " ORDER BY oid OFFSET {} LIMIT {} ".format(page*records_per_pages, records_per_pages)
    cur = connection.cursor(name="ALERCE Big Query Cursor")
    cur.execute(sql)
    current_app.logger.debug("Rows Returned:{}".format(row_number))
    def generateResp():
        colnames = None
        yield """
            {
                "total": %d,
                "num_pages": %d,
                "page": %d,
                "result" : {
        """ % (row_number,num_pages, page)
        while True:
            resp = cur.fetchmany(20)
            if colnames is None:
                colnames = [desc[0] for desc in cur.description]
                colmap = dict(zip(list(range(len(colnames))),colnames))
                for i in range(len(colnames)):
                    if colmap[i] == "id":
                        idPosition = i
                        break
            if not resp:
                break

            obj = {}
            for row in resp:
                yield '"{}":'.format(row[idPosition])
                for j,col in enumerate(row):
                    if col == "id":
                        continue
                    obj[colmap[j]] = col
                yield "{},".format(str(obj).replace("None","null"))
        yield "}\n"
        cur.close()

    return Response(stream_with_context(generateResp()), content_type='application/json')

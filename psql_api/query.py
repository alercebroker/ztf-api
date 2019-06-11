from psycopg2 import pool
from .app import config
from flask import Blueprint,Response,stream_with_context,current_app,request,g
from astropy import units as u

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

    sql += " ORDER BY oid "

    current_app.logger.debug(sql)
    connection  = psql_pool.getconn()
    cur = connection.cursor(name="ALERCE Big Query Cursor")
    cur.execute(sql)
    rowcount = cur.rowcount

    current_app.logger.debug("Rows Returned:{}".format(rowcount))
    def generateResp():
        while True:
            resp = cur.fetchmany(20)
            if not resp:
                break

            for row in resp:
                row = [str(i) for i in row]
                yield ",".join(row)+"\n"

    return Response(stream_with_context(generateResp()), content_type='application/json')

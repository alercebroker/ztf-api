import os
import configparser
from psycopg2 import connect, sql
from flask_cors import CORS
from flask import Flask
from flask_caching import Cache
import logging
# Reading Config File
filePath = os.path.dirname(os.path.abspath(__file__))
configPath = os.path.join(filePath, "..", "config")
config = configparser.ConfigParser()
config.read(os.path.join(configPath, "config.ini"))

# Starting Flask API
cache = Cache(config={'CACHE_TYPE': 'simple'})
app = Flask(__name__)
cache.init_app(app)
CORS(app)
is_gunicorn = "gunicorn" in os.environ.get("SERVER_SOFTWARE", "")
if is_gunicorn:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

conn = connect(host=config["DATABASE"]["host"],
               port=config["DATABASE"]["port"],
               user=config["DATABASE"]["User"],
               password=config["DATABASE"]["Pass"],
               database=config["DATABASE"]["Database"])
cur = conn.cursor()

class_query = sql.SQL("SELECT * FROM class")
cur.execute(class_query)
classes = {}
resp = cur.fetchall()
colnames = [desc[0] for desc in cur.description]
colmap = dict(zip(list(range(len(colnames))), colnames))
for row in resp:
    class_name = None
    for j, val in enumerate(row):
        if colmap[j] == "name":
            class_name = val
        if colmap[j] == "id":
            class_id = val
    classes[class_name] = class_id
cur.close()

@app.route("/")
def index():
    return "Welcome to ALERCE PostgreSQL API"


# Loading Query Blueprint
from .query import query_blueprint
app.register_blueprint(query_blueprint)
# Loading Objects Blueprint
from .objects import objects_blueprint
app.register_blueprint(objects_blueprint)

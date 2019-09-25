import os
import configparser
from psycopg2 import connect, sql, pool
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
app.config['JSON_SORT_KEYS'] = False


from .db import init_app
init_app(app)

cache.init_app(app)
CORS(app)
is_gunicorn = "gunicorn" in os.environ.get("SERVER_SOFTWARE", "")
if is_gunicorn:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

@app.route("/")
def index():
    return "Welcome to ALERCE PostgreSQL API"


# Loading Query Blueprint
from .query import query_blueprint
app.register_blueprint(query_blueprint)
# Loading Objects Blueprint
from .objects import objects_blueprint
app.register_blueprint(objects_blueprint)

# from .variable import variable_blueprint
# app.register_blueprint(variable_blueprint)

from .external import external_blueprint
app.register_blueprint(external_blueprint)

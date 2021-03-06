import os
from psycopg2 import connect, sql, pool
from flask_cors import CORS
from flask import Flask
from flask_caching import Cache
import logging

# Starting Flask API
cache = Cache(config={'CACHE_TYPE': 'simple'})
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

#Adding database connection interface to app
from .db import init_app
init_app(app)

#Starting flask cache
cache.init_app(app)
#Adding CORS for async calls
CORS(app)

#Check if gunicorn for logging
is_gunicorn = "gunicorn" in os.environ.get("SERVER_SOFTWARE", "")
if is_gunicorn:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

#Default route
@app.route("/")
def index():
    return "Welcome to ALERCE PostgreSQL API"


# Loading Blueprints
from .query import query_blueprint
app.register_blueprint(query_blueprint)
# Loading Objects Blueprint
from .objects import objects_blueprint
app.register_blueprint(objects_blueprint)

# from .variable import variable_blueprint
# app.register_blueprint(variable_blueprint)

from .external import external_blueprint
app.register_blueprint(external_blueprint)

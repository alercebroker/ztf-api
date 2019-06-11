import os
import configparser
from psycopg2 import connect

#Reading Config File
filePath = os.path.dirname(os.path.abspath(__file__))
configPath = os.path.join(filePath,"..","config")
config = configparser.ConfigParser()
config.read(os.path.join(configPath,"config.ini"))

#Starting Flask API
from flask import Flask
app = Flask(__name__)
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

@app.route("/")
def index():
    return "Welcome to ALERCE PostgreSQL API"

#Loading Query Blueprint
from .query import query_blueprint
app.register_blueprint(query_blueprint)

#Loading Objects Blueprint
from .objects import objects_blueprint
app.register_blueprint(objects_blueprint)

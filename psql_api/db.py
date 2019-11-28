import psycopg2
from flask import current_app, g
from flask.cli import with_appcontext

from .app import config
import os

#Create a db connection and save to
#app global variable (g)
def get_db():
    current_app.logger.debug("Creating connection")
    if 'db' not in g:
        g.db = psycopg2.connect(user = os.environ["ZTF_USER"],
                      password = os.environ["ZTF_PASSWORD"],
                      host = os.environ["ZTF_HOST"],
                      port = int(os.environ["ZTF_PORT"]),
                      database = os.environ["ZTF_DATABASE"])

#Close the connection
def close_db(e=None):
    db = g.pop('db', None)
    current_app.logger.debug("Closing connection")
    if db is not None:
        db.commit()
        db.close()

#Add create and close behavior on request
#arrival and close (avoid hanging connections)
def init_app(app):
    app.before_request(get_db)
    app.teardown_appcontext(close_db)

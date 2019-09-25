import psycopg2
from flask import current_app, g
from flask.cli import with_appcontext

from .app import config

def get_db():
    current_app.logger.debug("Creating connection")
    if 'db' not in g:
        g.db = psycopg2.connect(user = config["DATABASE"]["User"],
                      password = config["DATABASE"]["Pass"],
                      host = config["DATABASE"]["Host"],
                      port = config["DATABASE"]["Port"],
                      database = config["DATABASE"]["Database"])
    # return g.db


def close_db(e=None):
    db = g.pop('db', None)
    current_app.logger.debug("Closing connection")
    if db is not None:
        db.commit()
        db.close()


def init_app(app):
    app.before_request(get_db)
    app.teardown_appcontext(close_db)

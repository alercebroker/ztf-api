import psycopg2
import os
from flask import current_app, g
from flask.cli import with_appcontext

def get_db():
    current_app.logger.debug("Creating connection")
    if 'db' not in g:
        g.db = psycopg2.connect(user = os.environ["ZTF_USER"],
                      password = os.environ["ZTF_PASSWORD"],
                      host = os.environ["ZTF_HOST"],
                      port = int(os.environ["ZTF_PORT"]),
                      database = os.environ["ZTF_DATABASE"])
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

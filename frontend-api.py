from flask import Flask, jsonify, g
from lib.repository import SampleRepository
from lib.sample import JsonFactory
from lib.flask import validate_sha256
import os
import logging
import psycopg2

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger('KurasutaFrontendApi')
debugging_enabled = 'FLASK_DEBUG' in os.environ
logger.setLevel(logging.DEBUG if debugging_enabled else logging.WARNING)

app = Flask(__name__)
app.config.from_object(__name__)  # load config from this file , flaskr.py

# Load default config and override config from an environment variable
app.config.update(dict(
    DATABASE=os.environ['POSTGRES_DATABASE_LINK'],
    SECRET_KEY=os.environ['FLASK_SECRET_KEY']
))
app.config.from_envvar('FLASKR_SETTINGS', silent=True)


def connect_db():
    db = psycopg2.connect(app.config['DATABASE'])
    return db


def get_db():
    if not hasattr(g, 'db'):
        g.db = connect_db()
    return g.db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'db'):
        g.db.close()


@app.route('/sha256/<sha256>', methods=['GET'])
def get_sha256(sha256):
    validate_sha256(sha256)
    sample = SampleRepository(get_db()).by_hash_sha256(sha256)
    return jsonify(JsonFactory().from_sample(sample))


if __name__ == '__main__':
    if 'RAVEN_CLIENT_STRING' in os.environ:
        from raven.contrib.flask import Sentry

        sentry = Sentry(app, dsn=os.environ['RAVEN_CLIENT_STRING'])
    else:
        logger.warning('Environment variable RAVEN_CLIENT_STRING does not exist. No logging to Sentry is performed.')
    app.run(
        port=int(os.environ['FLASK_PORT']) if 'FLASK_PORT' in os.environ else None,
        debug=debugging_enabled
    )

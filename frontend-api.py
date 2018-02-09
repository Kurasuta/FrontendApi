from flask import Flask, jsonify, g
from lib.flask import InvalidUsage
from werkzeug.exceptions import NotFound
from lib.repository import SampleRepository
from lib.sample import JsonFactory
from lib.flask import validate_sha256
import os
import random
import logging
import psycopg2

if 'PUBLIC_SOURCES' not in os.environ:
    raise Exception('Environment variable PUBLIC_SOURCES does not exist. Cannot start API.')

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger('KurasutaFrontendApi')
debugging_enabled = 'FLASK_DEBUG' in os.environ and os.environ['FLASK_DEBUG']
logger.setLevel(logging.DEBUG if debugging_enabled else logging.WARNING)

app = Flask(__name__)
app.config.from_object(__name__)  # load config from this file , flaskr.py

# Load default config and override config from an environment variable
app.config.update(dict(
    DATABASE=os.environ['POSTGRES_DATABASE_LINK'],
    SECRET_KEY=os.environ['FLASK_SECRET_KEY'],
    PUBLIC_SOURCES=[identifier.strip() for identifier in os.environ['PUBLIC_SOURCES'].split(',') if identifier.strip()]
))
app.config.from_envvar('FLASKR_SETTINGS', silent=True)


def connect_db():
    db = psycopg2.connect(app.config['DATABASE'])
    return db


def get_db():
    if not hasattr(g, 'db'):
        g.db = connect_db()
    return g.db


def get_sample_repository():
    if not hasattr(g, 'sample_repository'):
        g.sample_repository = SampleRepository(get_db(), app.config['PUBLIC_SOURCES'])
    return g.sample_repository


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'db'):
        g.db.close()


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.route('/sample/<sha256>', methods=['GET'])
def get_sample(sha256):
    validate_sha256(sha256)
    sample = get_sample_repository().by_hash_sha256(sha256)
    if not sample:
        raise NotFound()
    return jsonify(JsonFactory().from_sample(sample))


@app.route('/newest_samples', methods=['GET'])
def newest_samples():
    return jsonify([JsonFactory().from_sample(sample) for sample in (get_sample_repository().newest(10))])


@app.route('/stats/count', methods=['GET'])
def stats_count():
    with get_db().cursor() as cursor:
        cursor.execute('''
            SELECT COUNT(id)
            FROM sample
            LEFT JOIN sample_has_source
            ON (sample.id = sample_has_source.sample_id)
            WHERE (sample_has_source.source_id IN %s)
        ''', (get_sample_repository().allowed_source_ids,))
        count = int(cursor.fetchall()[0][0])
    return jsonify({'count': count})


@app.route('/stats/build_time_stamps_by_year', methods=['GET'])
def build_time_stamps_by_year():
    with get_db().cursor() as cursor:
        cursor.execute('''
            SELECT EXTRACT(YEAR FROM build_timestamp), COUNT(*)
            FROM sample
            LEFT JOIN sample_has_source ON (sample.id = sample_has_source.sample_id)
            WHERE (sample_has_source.source_id IN %s)
            GROUP BY 1
        ''', (get_sample_repository().allowed_source_ids,))
        ret = {}
        for row in cursor.fetchall():
            ret[int(row[0])] = int(row[1])
    return jsonify(ret)


@app.route('/stats/processings_per_month', methods=['GET'])
def processings_per_month():
    with get_db().cursor() as cursor:
        cursor.execute('''
            SELECT EXTRACT(YEAR FROM t.completed_at), EXTRACT(MONTH FROM t.completed_at), COUNT(t.id) 
            FROM task t
            WHERE (t.type = 'PEMetadata'::task_type) AND (t.completed_at IS NOT NULL)
            GROUP BY 1, 2
        ''')
        ret = {}
        for row in cursor.fetchall():
            year = int(row[0])
            month = int(row[1])
            count = int(row[2])
            if year not in ret:
                ret[year] = {}
            ret[year][month] = count
    return jsonify(ret)


@app.route('/random_sample/by_year/<year>', methods=['GET'])
def random_sample_by_year(year):
    try:
        year = int(year)
    except ValueError:
        raise InvalidUsage('Given year is not an integer')
    if year < 1970:
        raise InvalidUsage('Given year should be above 1970')
    if year > 3000:
        raise InvalidUsage('Given year should be below 3000')
    with get_db().cursor() as cursor:
        cursor.execute('''
            SELECT COUNT(*)
            FROM sample
            LEFT JOIN sample_has_source ON (sample.id = sample_has_source.sample_id)
            WHERE (\'%i-01-01 00:00:00\' <= build_timestamp)
              AND (build_timestamp < \'%i-01-01 00:00:00\')
              AND (sample_has_source.source_id IN %s)
        ''' % (year, year + 1, get_sample_repository().allowed_source_ids))
        count = cursor.fetchall()[0][0]
        rand = random.randint(0, count - 1)
        cursor.execute(
            '''
            SELECT hash_sha256
            FROM sample
            LEFT JOIN sample_has_source ON (sample.id = sample_has_source.sample_id)
            WHERE (\'%i-01-01 00:00:00\' <= build_timestamp)
              AND (build_timestamp < \'%i-01-01 00:00:00\')
              AND (sample_has_source.source_id IN %s)
            LIMIT 1 OFFSET %s
            ''' % (year, year + 1, get_sample_repository().allowed_source_ids, rand)
        )
        random_sha256 = cursor.fetchall()[0][0]
        return jsonify(JsonFactory().from_sample(get_sample_repository().by_hash_sha256(random_sha256)))


@app.route('/random_sample/<count>', methods=['GET'])
def random_samples(count):
    try:
        count = int(count)
    except ValueError:
        raise InvalidUsage('Given count is not an integer')
    if count <= 0:
        raise InvalidUsage('Given count should be above 0')
    if count > 50:
        raise InvalidUsage('Given count should be below 50')
    return jsonify([JsonFactory().from_sample(sample) for sample in (get_sample_repository().random(count))])


@app.route('/section/<sha256>', methods=['GET'])
def get_samples_by_section(sha256):
    validate_sha256(sha256)
    samples = get_sample_repository().by_section_hash(sha256)
    return jsonify([JsonFactory().from_sample(sample) for sample in samples])


if __name__ == '__main__':
    if 'RAVEN_CLIENT_STRING' in os.environ:
        from raven.contrib.flask import Sentry

        sentry = Sentry(app, dsn=os.environ['RAVEN_CLIENT_STRING'])
    else:
        logger.warning('Environment variable RAVEN_CLIENT_STRING does not exist. No logging to Sentry is performed.')
    app.run(
        processes=4,
        port=int(os.environ['FLASK_PORT']) if 'FLASK_PORT' in os.environ else None,
        debug=debugging_enabled
    )

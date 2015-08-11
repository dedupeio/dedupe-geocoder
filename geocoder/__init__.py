from flask import Flask, render_template, g
from geocoder.api import api
from geocoder.app_config import TIME_ZONE
from datetime import datetime

sentry = None
try:
    from raven.contrib.flask import Sentry
    from geocoder.app_config import SENTRY_DSN
    if SENTRY_DSN:
        sentry = Sentry(dsn=SENTRY_DSN) 
except ImportError:
    pass
except KeyError:
    pass

def create_app():
    app = Flask(__name__)
    config = '{0}.app_config'.format(__name__)
    app.config.from_object(config)
    app.register_blueprint(api)
    
    if sentry:
        sentry.init_app(app)
    
    @app.before_request
    def before_request():
        from geocoder.database import engine
        
        g.engine = engine

    @app.teardown_request
    def teardown_request(exception):
        g.engine.dispose()

    return app

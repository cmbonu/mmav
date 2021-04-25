from flask import Flask
from waitress import serve
from services.fights_data_source import FightsDataSource
from endpoints.data_services import dataservice

_PROD = 0

def create_app(data_source):
    ds = FightsDataSource(data_source)
    app = Flask(__name__)
    app.config['FDS'] = ds
    app.register_blueprint(dataservice)
    return app

if __name__ == '__main__':
    app = create_app("data/ufc-fights.csv")
    if _PROD:
        serve(app,listen='*:5000')
    else:
        app.run(debug=True,host='0.0.0.0')

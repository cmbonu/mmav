from flask import Flask
from waitress import serve
from services.fights_data_source import FightsDataSource
from endpoints.data_services import dataservice

PROD = 0

def create_app():
    ds = FightsDataSource("data/ufc-fights.csv")
    app = Flask(__name__)
    app.config['FDS'] = ds
    app.register_blueprint(dataservice)
    return app

if __name__ == '__main__':
    app = create_app()
    if PROD:
        serve(app,listen='*:5000')
    else:
        app.run(debug=True,host='0.0.0.0')

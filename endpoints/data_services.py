from exceptions.data_source import FighterNotFoundException
from flask import current_app as app
from flask import Blueprint, request, jsonify

dataservice = Blueprint('fight_data_service',__name__)

@dataservice.route('/test',methods=['GET'])
def health_check():
    fds = app.config['FDS']
    fds_status = lambda x : 'OK' if x else 'NO FDS'
    return jsonify({'end_point':'OK','FDS':fds_status(fds)}),200

@dataservice.route('/api/fighters',methods=['GET'])
def fetch_all_fighters():
    fds = app.config['FDS']
    param_list = [[key,request.args.get(key)] for key in request.args]

    try:
        if len(param_list) != 0:
            fighter_list = fds.find_fighters_by_parameters(param_list)
            return jsonify(fighter_list), 200
    except FighterNotFoundException as fnfe:
        return jsonify({'message':str(fnfe)}),404

    return jsonify(fds.ROOSTER), 200

@dataservice.route('/api/fighters/<int:fighter_id>',methods=['GET'])
def get_fighter_data(fighter_id):
    fds = app.config['FDS']
    try:
       fighter_profile = fds.fetch_fighter_profile(fighter_id)
       return jsonify(fighter_profile), 200
    except FighterNotFoundException as fnfe:
        return jsonify({'message':str(fnfe)}),404
    
@dataservice.route('/api/fighters/weightclass/<weightclass>',methods=['GET'])
def find_fighters_by_weightclass(weightclass):
    fds = app.config['FDS']
    return jsonify(fds.find_fighters_by_parameter('WEIGHTCLASS',weightclass)),200

@dataservice.route('/api/fighters/query',methods=['GET'])
def find_by_parameters():
    param_list = [[key,request.args.get(key)] for key in request.args]
    fds = app.config['FDS']
    return jsonify(fds.find_fighters_by_parameters(param_list)),200
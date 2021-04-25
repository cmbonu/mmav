import pytest
from app import create_app
import json

@pytest.fixture(scope='module')
def data_service():
    flask_app = create_app("data/ufc-fights.csv")
    flask_app.testing = True
    with flask_app.test_client() as testing_client:
        with flask_app.app_context():
            yield testing_client

def test_test_endpoint(data_service):
    response = data_service.get('/test')
    content = json.loads(response.data)

    assert response.status_code == 200
    assert content['FDS'] == 'OK'
    assert content['end_point'] == 'OK'

def test_api_fighters(data_service):
    response = data_service.get('/api/fighters')
    content = json.loads(response.data)

    assert response.status_code == 200
    assert len(content) > 0
    assert 'FIGHTER' in content[0].keys()
    assert 'GENDER' in content[0].keys()
    assert 'ID' in content[0].keys()
    assert 'WEIGHTCLASS' in content[0].keys()

def test_get_fighter_with_existing_id(data_service):
    response = data_service.get('/api/fighters/1')
    content = json.loads(response.data)

    assert response.status_code == 200
    assert list(content.keys()) == ['fights', 'profile', 'summary']

def test_get_fighter_with_non_existing_id(data_service):
    response = data_service.get('/api/fighters/12121122')
    content = json.loads(response.data)
    
    assert response.status_code == 404
    assert content['message'] == "Fighter with ID: 12121122 not found"
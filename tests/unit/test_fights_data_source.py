import pytest
import services.fights_data_source as fight_src

@pytest.fixture(scope='module')
def fight_db():
    return fight_src.FightsDataSource("data/ufc-fights.csv")

def test_database_load(fight_db):
    assert fight_db.MASTER_DB.shape == (4387,137)

def test_rooster(fight_db):
    assert len(fight_db.unique_fighters()) == 1622

def test_shortest_path(fight_db):
    shortest_paths = fight_db.find_shortest_paths('Keith Berish','Jason High')
    assert len(shortest_paths) > 0
    assert len(shortest_paths[0]) > 0

def test_fetch_fighter_profile(fight_db):
    fighter_profile = fight_db.fetch_fighter_profile(2)
    profile_keys = fighter_profile.keys()

    assert 'profile' in profile_keys
    assert 'fights' in profile_keys
    assert 'summary' in profile_keys

def test_fetch_fighter_profile_with_no_loss(fight_db):
    fighter_id = fight_db.find_fighters_by_parameter('FIGHTER','David Dvorak')[0]['ID']
    fighter_profile = fight_db.fetch_fighter_profile(fighter_id)
    profile_keys = fighter_profile.keys()

    assert 'profile' in profile_keys
    assert 'fights' in profile_keys
    assert 'summary' in profile_keys
    assert list(fighter_profile['summary'][0].keys()) == ['agg_cols','agg_values','col_count','W','L','total','win_rate','loss_rate','pct_of_total']
    
def test_find_fighter_by_parameters(fight_db):
    fighter_list = fight_db.find_fighters_by_parameters([["WEIGHTCLASS","Bantamweight"],["GENDER","MALE"]])
    assert len(fighter_list) > 0
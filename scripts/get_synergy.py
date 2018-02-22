#Takes one argument: Season (2014-2015) 

import json
import logging
import sys
import re
import time
from sqlalchemy import create_engine

from scrape import synergy_stats
from storage import schema
from utils import utils

def store_data(connection, is_team, playtype, scheme, synergy_data, table):
    try:
        stat_data = synergy_data.get_synergy_data_for_stat(is_team, playtype, scheme)
        if stat_data is None:
            return None
        for dicts in stat_data:
            dicts['FG_MG'] = dicts.pop('FGMG')
            dicts['FG_M']  = dicts.pop('FGM')
            dicts['TEAM_ID'] = dicts.pop('TeamIDSID')
        if is_team == 0:
            for dicts in stat_data:
                dicts['PLAYER_ID'] = dicts.pop('PlayerIDSID')
        
        connection.execute(table.insert().values(stat_data))
    except:
        logging.error(utils.LogException())
    
    return None

def main():
    #logging.basicConfig(filename='logs/synergy.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    config=json.loads(open('config.json').read())
    season = None
    if len(sys.argv) < 1:
        print("Must input 1 arguments.")
        sys.exit(0)
    else:
        season = sys.argv[1]
    
    # make sure season is valid format
    season_pattern = re.compile('^\d{4}')
    if season_pattern.match(season) == None:
        print("Invalid Season format. Example format: 2015")
        sys.exit(0)
    year = season.split("-")[0]

    is_regular_season = config['is_regular_season']

    if is_regular_season == 0:
        season_type = "Post"
    elif is_regular_season == 1:
        season_type = "Reg"
    else:
        print("Invalid is_regular_season value. Use 0 for regular season, 1 for playoffs")

    # connect to database
    username = config['username']
    password = config['password']
    host = config['host']
    database = config['database']

    engine = create_engine('mysql+pymysql://'+username+':'+password+'@'+host+'/'+database)
    conn = engine.connect()

    synergy_data = synergy_stats.SynergyData(season, season_type)

    
    store_data(conn, 1, "Transition", "offensive", synergy_data, schema.synergy_transition_team_offense)
    store_data(conn, 1, "Transition",  "defensive", synergy_data, schema.synergy_transition_team_defense)
    store_data(conn, 0, "Transition", "offensive", synergy_data, schema.synergy_transition_player_offense)

    store_data(conn, 1, "Isolation", "offensive", synergy_data, schema.synergy_isolation_team_offense)
    store_data(conn, 1, "Isolation", "defensive", synergy_data, schema.synergy_isolation_team_defense)
    store_data(conn, 0, "Isolation", "offensive", synergy_data, schema.synergy_isolation_player_offense)
    store_data(conn, 0, "Isolation", "defensive", synergy_data, schema.synergy_isolation_player_defense)

    store_data(conn, 1, "PRBallHandler", "offensive", synergy_data, schema.synergy_pr_ball_handler_team_offense)
    store_data(conn, 1, "PRBallHandler", "defensive", synergy_data, schema.synergy_pr_ball_handler_team_defense)
    store_data(conn, 0, "PRBallHandler", "offensive", synergy_data, schema.synergy_pr_ball_handler_player_offense)
    store_data(conn, 0, "PRBallHandler", "defensive", synergy_data, schema.synergy_pr_ball_handler_player_defense)


    store_data(conn, 1, "PRRollMan", "offensive", synergy_data, schema.synergy_pr_roll_man_team_offense)
    store_data(conn, 1, "PRRollMan", "defensive", synergy_data, schema.synergy_pr_roll_man_team_defense)
    store_data(conn, 0, "PRRollMan", "offensive", synergy_data, schema.synergy_pr_roll_man_player_offense)
    store_data(conn, 0, "PRRollMan", "defensive", synergy_data, schema.synergy_pr_roll_man_player_defense)

    store_data(conn, 1, "Postup", "offensive", synergy_data, schema.synergy_post_up_team_offense)
    store_data(conn, 1, "Postup", "defensive", synergy_data, schema.synergy_post_up_team_defense)
    store_data(conn, 0, "Postup", "offensive", synergy_data, schema.synergy_post_up_player_offense)
    store_data(conn, 0, "Postup", "defensive", synergy_data, schema.synergy_post_up_player_defense)

    store_data(conn, 1, "Spotup", "offensive", synergy_data, schema.synergy_spot_up_team_offense)
    store_data(conn, 1, "Spotup", "defensive", synergy_data, schema.synergy_spot_up_team_defense)
    store_data(conn, 0, "Spotup", "offensive", synergy_data, schema.synergy_spot_up_player_offense)
    store_data(conn, 0, "Spotup", "defensive", synergy_data, schema.synergy_spot_up_player_defense)

    store_data(conn, 1, "Handoff", "offensive", synergy_data, schema.synergy_handoff_team_offense)
    store_data(conn, 1, "Handoff", "defensive", synergy_data, schema.synergy_handoff_team_defense)
    store_data(conn, 0, "Handoff", "offensive", synergy_data, schema.synergy_handoff_player_offense)
    store_data(conn, 0, "Handoff", "defensive", synergy_data, schema.synergy_handoff_player_defense)

    store_data(conn, 1, "Cut", "offensive", synergy_data, schema.synergy_cut_team_offense)
    store_data(conn, 1, "Cut", "defensive", synergy_data, schema.synergy_cut_team_defense)
    store_data(conn, 0, "Cut", "offensive", synergy_data, schema.synergy_cut_player_offense)


    store_data(conn, 1, "Offscreen", "offensive", synergy_data, schema.synergy_off_screen_team_offense)
    store_data(conn, 1, "Offscreen", "defensive", synergy_data, schema.synergy_off_screen_team_defense)
    store_data(conn, 0, "Offscreen", "offensive", synergy_data, schema.synergy_off_screen_player_offense)
    store_data(conn, 0, "Offscreen", "defensive", synergy_data, schema.synergy_off_screen_player_defense)

    store_data(conn, 1, "OffRebound", "offensive", synergy_data, schema.synergy_put_back_team_offense)
    store_data(conn, 1, "OffRebound", "defensive", synergy_data, schema.synergy_put_back_team_defense)
    store_data(conn, 0, "OffRebound", "offensive", synergy_data, schema.synergy_put_back_player_offense)


    store_data(conn, 1, "Misc", "offensive", synergy_data, schema.synergy_misc_team_offense)
    store_data(conn, 1, "Misc", "defensive", synergy_data, schema.synergy_misc_team_defense)
    store_data(conn, 0, "Misc", "offensive", synergy_data, schema.synergy_misc_player_offense)



if __name__ == '__main__':
    main()

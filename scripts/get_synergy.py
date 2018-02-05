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

def store_data(connection, is_team, playtype, scheme, table):
    try:
        stat_data = synergy_stats.synergy_data_for_stat(is_team, playtype, scheme)
        for dicts in stat_data:
            dicts['FG_MG'] = dicks.pop('FGMG')
            dicts['FG_M']  = dicks.pop('FGM')
        
        connection.execute(table.insert().values(stat_data))
    except:
        logging.error(utils.LogException())
    return None

def main():
    #logging.basicConfig(filename='logs/synergy.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    config=json.loads(open('config.json').read())
    season = None
    if len(sys.argv) < 1:
        print("Must input 2 arguments. Enter a season and regular season flag")
        sys.exit(0)
    else:
        season = sys.argv[1]
    
    # make sure season is valid format
    season_pattern = re.compile('\d{4}[-]\d{2}$')
    if season_pattern.match(season) == None:
        print("Invalid Season format. Example format: 2014-15")
        sys.exit(0)
    year = season.split("-")[0]

    is_regular_season = config['is_regular_season']

    if is_regular_season == 0:
        season_type = "_post"
    elif is_regular_season == 1:
        season_type = ""
    else:
        print("Invalid is_regular_season value. Use 0 for regular season, 1 for playoffs")

    # connect to database
    username = config['username']
    password = config['password']
    host = config['host']
    database = config['database']

    engine = create_engine('mysql+pymysql:://'+username+':'+password+'@'+host+'/'+database)
    conn = engine.connect()

    synergy_data = synergy_stats.SynergyData(season, season_type)

    
    store_data(conn, 1, "Transition", "offensive", schema.synergy_transition_team)
    store_data(conn, 1, "Transition",  "defensive", schema.synergy_transition_team)
    store_data(conn, 0, "Transition", "offensive" schema.synergy_transition_player)
    store_data(conn, 0, "Transition", "defensive" schema.synergy_transition_player)

    store_data(conn, 1, "Isolation", "offensive", schema.synergy_isolation_team)
    store_data(conn, 1, "Isolation", "defensive", schema.synergy_isolation_team)
    store_data(conn, 0, "Isolation", "offensive", schema.synergy_isolation_player)
    store_data(conn, 0, "Isolation", "defensive",schema.synergy_isolation_player)

    store_data(conn, 1, "PRBallHandler", "offensive", schema.synergy_pr_ball_handler_team)
    store_data(conn, 1, "PRBallHandler", "defensive", schema.synergy_pr_ball_handler_team)
    store_data(conn, 0, "PRBallHandler", "offensive", schema.synergy_pr_ball_handler_player)
    store_data(conn, 0, "PRBallHandler", "defensive", schema.synergy_pr_ball_handler_player)


    store_data(conn, 1, "PRRollMan", "offensive", schema.synergy_pr_roll_man_team)
    store_data(conn, 1, "PRRollMan", "defensive", schema.synergy_pr_roll_man_team)
    store_data(conn, 0, "PRRollMan", "offensive", schema.synergy_pr_roll_man_player)
    store_data(conn, 0, "PRRollMan", "defensive", schema.synergy_pr_roll_man_player)

    store_data(conn, 1, "Postup", "offensive", schema.synergy_post_up_team)
    store_data(conn, 1, "Postup", "defensive", schema.synergy_post_up_team)
    store_data(conn, 0, "Postup", "offensive", schema.synergy_post_up_player)
    store_data(conn, 0, "Postup", "defensive", schema.synergy_post_up_player)

    store_data(conn, 1, "Spotup", "offensive", schema.synergy_spot_up_team)
    store_data(conn, 1, "Spotup", "defensive", schema.synergy_spot_up_team)
    store_data(conn, 0, "Spotup", "offensive", schema.synergy_spot_up_player)
    store_data(conn, 0, "Spotup", "defensive", schema.synergy_spot_up_player)

    store_data(conn, 1, "Handoff", "offensive", schema.synergy_handoff_team)
    store_data(conn, 1, "Handoff", "defensive", schema.synergy_handoff_team)
    store_data(conn, 0, "Handoff", "offensive", schema.synergy_handoff_player)
    store_data(conn, 0, "Handoff", "defensive", schema.synergy_handoff_player)

    store_data(conn, 1, "Cut", "offensive", schema.synergy_cut_team)
    store_data(conn, 1, "Cut", "defensive", schema.synergy_cut_team)
    store_data(conn, 0, "Cut", "offensive", schema.synergy_cut_player)
    store_data(conn, 0, "Cut", "defensive", schema.synergy_cut_player)


    store_data(conn, 1, "Offscreen", "offensive", schema.synergy_off_screen_team)
    store_data(conn, 1, "Offscreen", "defensive", schema.synergy_off_screen_team)
    store_data(conn, 0, "Offscreen", "offensive", schema.synergy_off_screen_player)
    store_data(conn, 0, "Offscreen", "defensive", schema.synergy_off_screen_player)

    store_data(conn, 1, "Putback", "offensive", schema.synergy_put_back_team)
    store_data(conn, 1, "Putback", "defensive", schema.synergy_put_back_team)
    store_data(conn, 0, "Putback", "offensive", schema.synergy_put_back_player)
    store_data(conn, 0, "Putback", "defensive", schema.synergy_put_back_player)


    store_data(conn, 1, "Misc", "offensive", schema.synergy_misc_team)
    store_data(conn, 1, "Misc", "defensive", schema.synergy_misc_team)
    store_data(conn, 0, "Misc", "offensive", schema.synergy_misc_player)
    store_data(conn, 0, "Misc", "defensive", schema.synergy_misc_player)



if __name__ == '__main__':
    main()

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

def store_data(connection, data, table):
    try:
        connection.execute(table.insert().values(data))
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

    engine = create_engine('mysql://'+username+':'+password+'@'+host+'/'+database)
    conn = engine.connect()

    synergy_data = synergy_stats.SynergyData(season_type)

    store_data(conn, utils.add_keys(synergy_data.transition_team_offense(), season, is_regular_season), schema.synergy_transition_team_offense)
    store_data(conn, utils.add_keys(synergy_data.transition_team_defense(), season, is_regular_season), schema.synergy_transition_team_defense)
    store_data(conn, utils.add_keys(synergy_data.transition_offense(), season, is_regular_season), schema.synergy_transition_offense)

    store_data(conn, utils.add_keys(synergy_data.isolation_team_offense(), season, is_regular_season), schema.synergy_isolation_team_offense)
    store_data(conn, utils.add_keys(synergy_data.isolation_team_defense(), season, is_regular_season), schema.synergy_isolation_team_defense)
    store_data(conn, utils.add_keys(synergy_data.isolation_offense(), season, is_regular_season), schema.synergy_isolation_offense)
    store_data(conn, utils.add_keys(synergy_data.isolation_defense(), season, is_regular_season), schema.synergy_isolation_defense)

    store_data(conn, utils.add_keys(synergy_data.pr_ball_handler_team_offense(), season, is_regular_season), schema.synergy_pr_ball_handler_team_offense)
    store_data(conn, utils.add_keys(synergy_data.pr_ball_handler_team_defense(), season, is_regular_season), schema.synergy_pr_ball_handler_team_defense)
    store_data(conn, utils.add_keys(synergy_data.pr_ball_handler_offense(), season, is_regular_season), schema.synergy_pr_ball_handler_offense)
    store_data(conn, utils.add_keys(synergy_data.pr_ball_handler_defense(), season, is_regular_season), schema.synergy_pr_ball_handler_defense)

    store_data(conn, utils.add_keys(synergy_data.pr_roll_man_team_offense(), season, is_regular_season), schema.synergy_pr_roll_man_team_offense)
    store_data(conn, utils.add_keys(synergy_data.pr_roll_man_team_defense(), season, is_regular_season), schema.synergy_pr_roll_man_team_defense)
    store_data(conn, utils.add_keys(synergy_data.pr_roll_man_offense(), season, is_regular_season), schema.synergy_pr_roll_man_offense)
    store_data(conn, utils.add_keys(synergy_data.pr_roll_man_defense(), season, is_regular_season), schema.synergy_pr_roll_man_defense)

    store_data(conn, utils.add_keys(synergy_data.post_up_team_offense(), season, is_regular_season), schema.synergy_post_up_team_offense)
    store_data(conn, utils.add_keys(synergy_data.post_up_team_defense(), season, is_regular_season), schema.synergy_post_up_team_defense)
    store_data(conn, utils.add_keys(synergy_data.post_up_offense(), season, is_regular_season), schema.synergy_post_up_offense)
    store_data(conn, utils.add_keys(synergy_data.post_up_defense(), season, is_regular_season), schema.synergy_post_up_defense)

    store_data(conn, utils.add_keys(synergy_data.spot_up_team_offense(), season, is_regular_season), schema.synergy_spot_up_team_offense)
    store_data(conn, utils.add_keys(synergy_data.spot_up_team_defense(), season, is_regular_season), schema.synergy_spot_up_team_defense)
    store_data(conn, utils.add_keys(synergy_data.spot_up_offense(), season, is_regular_season), schema.synergy_spot_up_offense)
    store_data(conn, utils.add_keys(synergy_data.spot_up_defense(), season, is_regular_season), schema.synergy_spot_up_defense)

    store_data(conn, utils.add_keys(synergy_data.handoff_team_offense(), season, is_regular_season), schema.synergy_handoff_team_offense)
    store_data(conn, utils.add_keys(synergy_data.handoff_team_defense(), season, is_regular_season), schema.synergy_handoff_team_defense)
    store_data(conn, utils.add_keys(synergy_data.handoff_offense(), season, is_regular_season), schema.synergy_handoff_offense)
    store_data(conn, utils.add_keys(synergy_data.handoff_defense(), season, is_regular_season), schema.synergy_handoff_defense)

    store_data(conn, utils.add_keys(synergy_data.cut_team_offense(), season, is_regular_season), schema.synergy_cut_team_offense)
    store_data(conn, utils.add_keys(synergy_data.cut_team_defense(), season, is_regular_season), schema.synergy_cut_team_defense)
    store_data(conn, utils.add_keys(synergy_data.cut_offense(), season, is_regular_season), schema.synergy_cut_offense)

    store_data(conn, utils.add_keys(synergy_data.off_screen_team_offense(), season, is_regular_season), schema.synergy_off_screen_team_offense)
    store_data(conn, utils.add_keys(synergy_data.off_screen_team_defense(), season, is_regular_season), schema.synergy_off_screen_team_defense)
    store_data(conn, utils.add_keys(synergy_data.off_screen_offense(), season, is_regular_season), schema.synergy_off_screen_offense)
    store_data(conn, utils.add_keys(synergy_data.off_screen_defense(), season, is_regular_season), schema.synergy_off_screen_defense)

    store_data(conn, utils.add_keys(synergy_data.put_back_team_offense(), season, is_regular_season), schema.synergy_put_back_team_offense)
    store_data(conn, utils.add_keys(synergy_data.put_back_team_defense(), season, is_regular_season), schema.synergy_put_back_team_defense)
    store_data(conn, utils.add_keys(synergy_data.put_back_offense(), season, is_regular_season), schema.synergy_put_back_offense)

    store_data(conn, utils.add_keys(synergy_data.misc_team_offense(), season, is_regular_season), schema.synergy_misc_team_offense)
    store_data(conn, utils.add_keys(synergy_data.misc_team_defense(), season, is_regular_season), schema.synergy_misc_team_defense)
    store_data(conn, utils.add_keys(synergy_data.misc_offense(), season, is_regular_season), schema.synergy_misc_offense)


if __name__ == '__main__':
    main()

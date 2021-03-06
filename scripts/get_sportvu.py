#Takes two arguments: Season (2014-15) 

import json
import logging
import sys
import re
import time
from sqlalchemy import create_engine

from utils import utils
from storage import schema
from scrape import sportvu_stats

def store_stat(sportsVu, season, player_or_team, measure_type, is_regular_season, table, connection):
    try:
        stat_data = sportsVu.get_sportvu_data_for_stat(player_or_team, measure_type)
        if stat_data is None:
            return None
        connection.execute(table.insert().values(utils.add_keys(stat_data, season, is_regular_season)))
    except:
        logging.error(utils.LogException())
    return None

def main():
    #logging.basicConfig(filename='logs/sportvu.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    config=json.loads(open('config.json').read())
    season = None
    if len(sys.argv) < 2:
        print("Must input 1 argument. Enter a season. ")
        sys.exit(0)
    else:
        season = sys.argv[1]
    #season = config["season"]
    #is_regular_season = config["is_regular_season"]
    # make sure season is valid format
    season_pattern = re.compile('\d{4}[-]\d{2}$')
    if season_pattern.match(season) == None:
        print("Invalid Season format. Example format: 2014-15")
        sys.exit(0)
    year = season.split("-")[0]
    
    is_regular_season = config['is_regular_season']
    if is_regular_season == 0:
        season_type = "Playoffs"
    elif is_regular_season == 1:
        season_type = "Regular Season"
    else:
        print("Invalid is_regular_season value. Use 0 for regular season, 1 for playoffs")
        sys.exit(0)

    username = config['username']
    password = config['password']
    host = config['host']
    database = config['database']

    engine = create_engine('mysql+pymysql://'+username+':'+password+'@'+host+'/'+database)
    conn = engine.connect()

    sportsVu = sportvu_stats.SportsVuData(season, season_type)

    store_stat(sportsVu, season, "Player", "CatchShoot", is_regular_season, schema.sportvu_catch_shoot, conn)
    store_stat(sportsVu, season, "Team", "CatchShoot", is_regular_season, schema.sportvu_catch_shoot_team, conn)
    store_stat(sportsVu, season, "Player", "Defense", is_regular_season, schema.sportvu_defense, conn)
    store_stat(sportsVu, season, "Team", "Defense", is_regular_season, schema.sportvu_defense_team, conn)
    store_stat(sportsVu, season, "Player", "Drives", is_regular_season, schema.sportvu_drives, conn)
    store_stat(sportsVu, season, "Team", "Drives", is_regular_season, schema.sportvu_drives_team, conn)
    store_stat(sportsVu, season, "Player", "Passing", is_regular_season, schema.sportvu_passing, conn)
    store_stat(sportsVu, season, "Team", "Passing", is_regular_season, schema.sportvu_passing_team, conn)
    store_stat(sportsVu, season, "Player", "PullUpShot", is_regular_season, schema.sportvu_pull_up_shoot, conn)
    store_stat(sportsVu, season, "Team", "PullUpShot", is_regular_season, schema.sportvu_pull_up_shoot_team, conn)
    store_stat(sportsVu, season, "Player", "Rebounding", is_regular_season, schema.sportvu_rebounding, conn)
    store_stat(sportsVu, season, "Team", "Rebounding", is_regular_season, schema.sportvu_rebounding_team, conn)
    store_stat(sportsVu, season, "Player", "Efficiency", is_regular_season, schema.sportvu_shooting, conn)
    store_stat(sportsVu, season, "Team", "Efficiency", is_regular_season, schema.sportvu_shooting_team, conn)
    store_stat(sportsVu, season, "Player", "SpeedDistance", is_regular_season, schema.sportvu_speed, conn)
    store_stat(sportsVu, season, "Team", "SpeedDistance", is_regular_season, schema.sportvu_speed_team, conn)
    store_stat(sportsVu, season, "Player", "ElbowTouch", is_regular_season, schema.sportvu_elbow_touches, conn)
    store_stat(sportsVu, season, "Team", "ElbowTouch", is_regular_season, schema.sportvu_elbow_touches_team, conn)
    store_stat(sportsVu, season, "Player", "PaintTouch", is_regular_season, schema.sportvu_paint_touches, conn)
    store_stat(sportsVu, season, "Team", "PaintTouch", is_regular_season, schema.sportvu_paint_touches_team, conn)
    store_stat(sportsVu, season, "Player", "PostTouch", is_regular_season, schema.sportvu_post_touches, conn)
    store_stat(sportsVu, season, "Team", "PostTouch", is_regular_season, schema.sportvu_post_touches_team, conn)
    store_stat(sportsVu, season, "Player", "Possessions", is_regular_season, schema.sportvu_possessions, conn)
    store_stat(sportsVu, season, "Team", "Possessions", is_regular_season, schema.sportvu_possessions_team, conn)

if __name__ == '__main__':
    main()

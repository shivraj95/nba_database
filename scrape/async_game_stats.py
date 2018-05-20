# Takes three arguments: season, start date, and end date. Date format YYYY-MM-DD. If no dates entered, then gets yesterday
import json
import sys
import datetime
from dateutil.rrule import rrule, DAILY
import logging
import re
import linecache
from sqlalchemy import sa
from scrape import game_stats
from storage import schema
from scrape import async_helper
from utils import utils
import asyncio
import sqlalchemy 
#from concurrent.futures import ProcessPoolExecutor
#from aiopg.sa import create_pool
from aiohttp import ClientSession

async def get_games(start_date, end_date):
    #start aiohttp connection
    #make sure to set semaphores
    #use asyncio gather to return results for each date
    loop = asyncio.get_event_loop()
    # create instance of Semaphore
    sem = asyncio.Semaphore(10)
    tasks = []
    with ClientSession() as session:
        for dt in rrule(DAILY, dtstart=start_date, until=end_date):
            # pass Semaphore and session to every GET request
        task = asyncio.ensure_future(async_helper.get_game_ids_for_date(sem, session, dt))
        tasks.append(task)
    game_ids = loop.run_until_complete(asyncio.gather(*tasks))

    return game_ids 

def store_data(connection, table, data):
    try:
        connection.execute(table.insert().values(data))
    except:
        logging.error(utils.LogException())
    return None

def main():
    logging.basicConfig(filename='logs/games.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    config=json.loads(open('config.json').read())
    season = None 
    if len(sys.argv) < 4:
        start_date = datetime.date.today() - datetime.timedelta(1)
        end_date = datetime.date.today() - datetime.timedelta(1)
    elif len(sys.argv) > 4:
        print("Too many arguments. Enter a start and end date with format YYYY-MM-DD")
        sys.exit(0)
    else:
        season = sys.argv[1]
        start = sys.argv[2]
        end = sys.argv[3]
        # validate dates
        try:
            datetime.datetime.strptime(start, '%Y-%m-%d')
        except:
            print('invalid format for start date')
            sys.exit(0)

        try:
            datetime.datetime.strptime(end, '%Y-%m-%d')
        except:
            print('invalid format for end date')
            sys.exit(0)

        start_split = start.split("-")
        end_split = end.split("-")

        start_date = datetime.date(int(start_split[0]), int(start_split[1]), int(start_split[2]))
        end_date = datetime.date(int(end_split[0]), int(end_split[1]), int(end_split[2]))

    # make sure season is valid format
    season_pattern = re.compile('\d{4}[-]\d{2}$')
    if season_pattern.match(season) == None:
        print("Invalid Season format. Example format: 2014-15")
        sys.exit(0)

    is_regular_season = config["is_regular_season"]
    if is_regular_season == 0:
        season_type = "Playoffs"
        game_prefix = "004"
    elif is_regular_season == 1:
        season_type = "Regular Season"
        game_prefix = "002"
    else:
        print("Invalid is_regular_season value. Use 0 for regular season, 1 for playoffs")
        sys.exit(0)

    username = config['username']
    password = config['password']
    host = config['host']
    database = config['database']

    loop = asyncio.get_event_loop()

    game_ids = loop.run_until_complete(get_games(start_date, end_date))
    
    print('Length of game_ids ', len(game_ids))
    """
    loop.run_until_complete(
        asyncio.gather(
            *(get_game_stats(*games) for games in game_ids)
        )
    )
    with (yield from pool) as conn:
        cur = yield from conn.cursor()
        yield from cur.execute("SELECT 10")
        # print(cur.description)
        (r,) = yield from cur.fetchone()
       assert r == 10
    pool.close()
    yield from pool.wait_closed()
    engine = yield from create_engine('mysql+pymysql://'+username+':'+password+'@'+host+'/'+database)
    with (yield from engine) as conn:


    #apply asynchronous methods
    for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        games = helper.get_game_ids_for_date(dt.strftime("%Y-%m-%d"))
        for game_id in games:
            if game_id[:3] == game_prefix:
                game_data = game_stats.GameData(game_id, season, season_type)
                store_data(conn, schema.pbp, game_data.pbp())
                store_data(conn, schema.player_tracking_boxscores, game_data.player_tracking_boxscore())
                store_data(conn, schema.player_tracking_boxscores_team, game_data.player_tracking_boxscore_team())
                store_data(conn, schema.shots, game_data.shots())
                store_data(conn, schema.traditional_boxscores, game_data.traditional_boxscore())
                store_data(conn, schema.traditional_boxscores_team, game_data.traditional_boxscore_team())
                store_data(conn, schema.advanced_boxscores, game_data.advanced_boxscore())
                store_data(conn, schema.advanced_boxscores_team, game_data.advanced_boxscore_team())
                store_data(conn, schema.scoring_boxscores, game_data.scoring_boxscore())
                store_data(conn, schema.scoring_boxscores_team, game_data.scoring_boxscore_team())
                store_data(conn, schema.misc_boxscores, game_data.misc_boxscore())
                store_data(conn, schema.misc_boxscores_team, game_data.misc_boxscore_team())
                store_data(conn, schema.usage_boxscores, game_data.usage_boxscore())
                store_data(conn, schema.four_factors_boxscores, game_data.four_factors_boxscore())
                store_data(conn, schema.four_factors_boxscores_team, game_data.four_factors_boxscore_team())
                store_data(conn, schema.other_stats, game_data.other_stats())
                store_data(conn, schema.officials, game_data.officials())
                store_data(conn, schema.inactives, game_data.inactives())
        """
if __name__ == '__main__':
    main()

# Takes three arguments: season, start date, and end date. Date format YYYY-MM-DD. If no dates entered, then gets yesterday
import json
import sys
import datetime
from dateutil.rrule import rrule, DAILY
import logging
import re
import linecache
import sqlalchemy
from storage import schema
from utils import utils
import asyncio
import sqlalchemy 
import aiomysql
import aiohttp

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36"
REFERER = "http://stats.nba.com/scores/"


#From Gamestats
async def pbp(pbp_base_url, query, game_id, session, pool):
    parameters = {
                        "GameId": game_id,
                        "StartPeriod": 0,
                        "EndPeriod": 10,
                        "RangeType": 2,
                        "StartRange": 0,
                        "EndRange": 55800
        }
    data = await get_data_from_url_with_parameters(pbp_base_url, parameters, 0, session)
    return await store_data(pool, query, data)

async def player_tracking_boxscore(player_tracking_boxscore_base_url, query, game_id, session, pool):
    params = {"GameId": game_id}
    data = await get_data_from_url_with_parameters(player_tracking_boxscore_base_url, params, 0, session)
    return await store_data(pool, query, data)


async def player_tracking_boxscore_team(player_tracking_boxscore_base_url, query, game_id, session, pool):
    params = {"GameId": game_id}
    data = await get_data_from_url_with_parameters(player_tracking_boxscore_base_url, params, 1, session)
    return await store_data(pool, query, data)

async def traditional_boxscore(traditional_boxscore_base_url, query, game_id, session, pool):
    parameters = {
                        "GameId": game_id,
                        "StartPeriod": 0,
                        "EndPeriod": 10,
                        "RangeType": 2,
                        "StartRange": 0,
                        "EndRange": 55800
        }
    data = await get_data_from_url_with_parameters(traditional_boxscore_base_url, parameters, 0, session)
    return await store_data(pool, query, data)

async def traditional_boxscore_team(traditional_boxscore_base_url, query, game_id, session, pool):
    parameters = {
                        "GameId": game_id,
                        "StartPeriod": 0,
                        "EndPeriod": 10,
                        "RangeType": 2,
                        "StartRange": 0,
                        "EndRange": 55800
        }

    data = await get_data_from_url_with_parameters(traditional_boxscore_base_url, parameters, 1, session)
    return await store_data(pool, query, data)

async def advanced_boxscore(advanced_boxscore_base_url, query, game_id, session, pool):
    parameters = {
                        "GameId": game_id,
                        "StartPeriod": 0,
                        "EndPeriod": 10,
                        "RangeType": 2,
                        "StartRange": 0,
                        "EndRange": 55800
        }
    data = await get_data_from_url_with_parameters(advanced_boxscore_base_url, parameters, 0, session)
    return await store_data(pool, query, data)

async def advanced_boxscore_team(advanced_boxscore_base_url, query, game_id, session, pool):
    parameters = {
                        "GameId": game_id,
                        "StartPeriod": 0,
                        "EndPeriod": 10,
                        "RangeType": 2,
                        "StartRange": 0,
                        "EndRange": 55800
        }
    data = await get_data_from_url_with_parameters(advanced_boxscore_base_url, parameters, 1, session)
    return await store_data(pool, query, data)

async def scoring_boxscore(scoring_boxscore_base_url, query, game_id, session, pool):
    parameters = {
                        "GameId": game_id,
                        "StartPeriod": 0,
                        "EndPeriod": 10,
                        "RangeType": 2,
                        "StartRange": 0,
                        "EndRange": 55800
        }
    data = await get_data_from_url_with_parameters(scoring_boxscore_base_url, parameters, 0, session)
    return await store_data(pool, query, data)

async def scoring_boxscore_team(scoring_boxscore_base_url, query, game_id, session, pool):
    parameters = {
                        "GameId": game_id,
                        "StartPeriod": 0,
                        "EndPeriod": 10,
                        "RangeType": 2,
                        "StartRange": 0,
                        "EndRange": 55800
        }
    data = await get_data_from_url_with_parameters(scoring_boxscore_base_url, parameters, 1, session)
    return await store_data(pool, query, data)

async def misc_boxscore(misc_boxscore_base_url, query, game_id, session, pool):
    parameters = {
                        "GameId": game_id,
                        "StartPeriod": 0,
                        "EndPeriod": 10,
                        "RangeType": 2,
                        "StartRange": 0,
                        "EndRange": 55800
        }
    data = await get_data_from_url_with_parameters(misc_boxscore_base_url, parameters, 0, session)
    return await store_data(pool, query, data)

async def misc_boxscore_team(misc_boxscore_base_url, query, game_id, session, pool):
    parameters = {
                        "GameId": game_id,
                        "StartPeriod": 0,
                        "EndPeriod": 10,
                        "RangeType": 2,
                        "StartRange": 0,
                        "EndRange": 55800
        }
    data = await get_data_from_url_with_parameters(misc_boxscore_base_url, parameters, 1, session)
    return await store_data(pool, query, data)

async def usage_boxscore(usage_boxscore_base_url, query, game_id, session, pool):
    parameters = {
                        "GameId": game_id,
                        "StartPeriod": 0,
                        "EndPeriod": 10,
                        "RangeType": 2,
                        "StartRange": 0,
                        "EndRange": 55800
        }
    data = await get_data_from_url_with_parameters(usage_boxscore_base_url, parameters, 0, session)
    return await store_data(pool, query, data)

async def four_factors_boxscore(four_factors_boxscore_base_url, query, game_id, session, pool):
    parameters = {
                        "GameId": game_id,
                        "StartPeriod": 0,
                        "EndPeriod": 10,
                        "RangeType": 2,
                        "StartRange": 0,
                        "EndRange": 55800
        }
    data = await get_data_from_url_with_parameters(four_factors_boxscore_base_url, parameters, 0, session)
    return await store_data(pool, query, data)

async def four_factors_boxscore_team(four_factors_boxscore_base_url, query, game_id, session, pool):
    parameters = {
                        "GameId": game_id,
                        "StartPeriod": 0,
                        "EndPeriod": 10,
                        "RangeType": 2,
                        "StartRange": 0,
                        "EndRange": 55800
        }
    data = await get_data_from_url_with_parameters(four_factors_boxscore_base_url, parameters, 1, session)
    return await store_data(pool, query, data)

async def shots(shots_base_url, query, season, season_type, game_id, session, pool):
    params = {
                        "GameId": game_id,
                        "Season": season,
                        "SeasonType": season_type,
                        "SeasonSegment":'',
                        "TeamID": 0,
                        "PlayerID": 0,
                        "PlayerPosition":'', 
                        "Outcome": '',
                        "Location": '',
                        "Month": 0,
                        "DateFrom": '',
                        "DateTo": '',
                        "OpponentTeamID": 0,
                        "VsConference": '',
                        "VsDivision": '',
                        "Position": '',
                        "RookieYear": '',
                        "GameSegment": '',
                        "Period": 0,
                        "LastNGames":  0,
                        "ContextMeasure": 'FGA'
        }
    data = await get_data_from_url_with_parameters(shots_base_url, params, 0, session)
    return await store_data(pool, query, data)


async def officials(summary_base_url, query, game_id, session, pool):
    params = {"GameId": game_id}
    data = await get_data_from_url_with_parameters_add_game_id(summary_base_url, params, game_id, 2, session)
    return await store_data(pool, query, data)


async def other_stats(summary_base_url, query, game_id, session, pool):
    params= {"GameId": game_id}
    data = await get_data_from_url_with_parameters_add_game_id(summary_base_url, params, game_id, 1, session)
    return await store_data(pool, query, data)

async def inactives(summary_base_url, query, game_id, session, pool):
    params = {"GameId": game_id}
    data = await get_data_from_url_with_parameters_add_game_id(summary_base_url, params, game_id, 3, session)
    return await store_data(pool, query, data)
#-------------------------------------------------------------

#From helper 
async def get_data_from_url_with_parameters(base_url, parameters, index, session):
    request_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.8',
        'Connection': 'keep-alive',
        'Host': 'stats.nba.com',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': USER_AGENT, 
        'Referer': REFERER
    }
    try: 
        async with session.get(base_url, params=parameters, headers=request_headers, timeout=None) as resp:
            print(resp.status)
            data = await resp.json()
            headers = data['resultSets'][index]['headers']
            rows = data['resultSets'][index]['rowSet']
            #print('returning processed data')
            return [dict(zip(headers, row)) for row in rows]
    except Exception as e:
        print('Timeout error for game_id: ' + parameters['GameId'] + base_url + ' ' + str(type(e)) + str(e))
    

async def get_data_from_url_with_parameters_add_game_id(base_url, parameters, game_id, index, session):
    request_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.8',
        'Connection': 'keep-alive',
        'Host': 'stats.nba.com',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': USER_AGENT, 
        'Referer': REFERER
    }
    try: 
        async with session.get(base_url, params=parameters, headers=request_headers, timeout=None) as resp:   
            print(resp.status)
            data = await resp.json()
            headers = data['resultSets'][index]['headers']
            headers = ["GAME_ID"] + headers
            rows = data['resultSets'][index]['rowSet']
            
            return [dict(zip(headers, [game_id] + row)) for row in rows]
    except Exception as e:
        print('Timeout error for game_id: ' + game_id + base_url + ' ' + str(type(e)) + str(e))


async def get_game_ids_for_date(session, date):
    # date format YYYY-MM-DD
    game_ids = []
    split = date.split("-")
    parameters = {
                    "DayOffset": 0,
                    "LeagueID": "00",
                    "gameDate": split[1]+"/"+split[2]+"/"+split[0]
    }
    result = await get_data_from_url_with_parameters("http://stats.nba.com/stats/scoreboardV2", parameters, 1, session)
    for games in result:    
        game_ids.append(games['GAME_ID'])
        
    return list(set(game_ids))


async def get_games(start_date, end_date):
    #start aiohttp connection
    #make sure to set semaphores
    #use asyncio gather to return results for each date
    #loop = asyncio.get_event_loop()
    # create instance of Semaphore
    tasks = []
    conn = aiohttp.TCPConnector(limit=10,limit_per_host=1)
    async with aiohttp.ClientSession(connector=conn) as session:
        for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        # pass Semaphore and session to every GET request
            task = asyncio.ensure_future(get_game_ids_for_date(session, dt.strftime("%Y-%m-%d")))
            tasks.append(task)
        return await asyncio.gather(*tasks)
        
    #return game_ids 



async def process_game_data(host, user_name, password, database, loop, season, season_type, game_ids, query_dict):
    pool = await aiomysql.create_pool(host=host, port=3306, user=user_name, password=password, db=database, loop=loop, maxsize=20)
    game_tasks = []
    conn = aiohttp.TCPConnector(limit=20, limit_per_host=1)
    async with aiohttp.ClientSession(connector=conn) as session:
        for game in game_ids:
            game_tasks.append(asyncio.ensure_future(pbp("http://stats.nba.com/stats/playbyplayv2", query_dict['pbp'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(player_tracking_boxscore("http://stats.nba.com/stats/boxscoreplayertrackv2", query_dict['player_tracking_boxscores'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(player_tracking_boxscore_team("http://stats.nba.com/stats/boxscoreplayertrackv2", query_dict['player_tracking_boxscores_team'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(shots("http://stats.nba.com/stats/shotchartdetail", query_dict['shots'], season, season_type, game, session, pool)))
            game_tasks.append(asyncio.ensure_future(traditional_boxscore("http://stats.nba.com/stats/boxscoretraditionalv2", query_dict['traditional_boxscores'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(traditional_boxscore_team("http://stats.nba.com/stats/boxscoretraditionalv2", query_dict['traditional_boxscores_team'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(advanced_boxscore("http://stats.nba.com/stats/boxscoreadvancedv2", query_dict['advanced_boxscores'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(advanced_boxscore_team("http://stats.nba.com/stats/boxscoreadvancedv2", query_dict['advanced_boxscores_team'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(scoring_boxscore("http://stats.nba.com/stats/boxscorescoringv2", query_dict['scoring_boxscores'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(scoring_boxscore_team("http://stats.nba.com/stats/boxscorescoringv2", query_dict['scoring_boxscores_team'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(misc_boxscore("http://stats.nba.com/stats/boxscoremiscv2",  query_dict['misc_boxscores'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(misc_boxscore_team("http://stats.nba.com/stats/boxscoremiscv2", query_dict['misc_boxscores_team'] ,game, session, pool)))
            game_tasks.append(asyncio.ensure_future(usage_boxscore("http://stats.nba.com/stats/boxscoreusagev2", query_dict['usage_boxscores'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(four_factors_boxscore("http://stats.nba.com/stats/boxscorefourfactorsv2", query_dict['four_factors_boxscores'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(four_factors_boxscore_team("http://stats.nba.com/stats/boxscorefourfactorsv2", query_dict['four_factors_boxscores_team'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(other_stats( "http://stats.nba.com/stats/boxscoresummaryv2", query_dict['other_stats'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(officials("http://stats.nba.com/stats/boxscoresummaryv2", query_dict['officials'], game, session, pool)))
            game_tasks.append(asyncio.ensure_future(inactives("http://stats.nba.com/stats/boxscoresummaryv2", query_dict['inactives'], game, session, pool)))

        return await asyncio.gather(*game_tasks) 



def format_query(query):
    temp = query.replace(', :', ')s,%(')
    temp = temp.replace('(:', '(%(')
    temp = re.sub('\)$', ')s)', temp)
    return temp.replace('"', '`')


async def store_data(pool, query, data):
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(query, data)
                await conn.commit()
    except:
        logging.error(utils.LogException())
        print('\n')
        print(query)
        
    return 

def main():
    #logging.basicConfig(filename='logs/games.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
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


    #create queries for bulk insert into different tables
    query_dict = {}
    query_dict['pbp'] = format_query(str(schema.pbp.insert()))
    query_dict['player_tracking_boxscores'] = format_query(str(schema.player_tracking_boxscores.insert()))
    query_dict['player_tracking_boxscores_team'] = format_query(str(schema.player_tracking_boxscores_team.insert()))
    query_dict['shots'] = format_query(str(schema.shots.insert()))
    query_dict['traditional_boxscores'] = format_query(str(schema.traditional_boxscores.insert()))
    query_dict['traditional_boxscores_team'] = format_query(str(schema.traditional_boxscores_team.insert())) 
    query_dict['advanced_boxscores'] = format_query(str(schema.advanced_boxscores.insert()))
    query_dict['advanced_boxscores_team'] = format_query(str(schema.advanced_boxscores_team.insert()))
    query_dict['scoring_boxscores'] =  format_query(str(schema.scoring_boxscores.insert()))
    query_dict['scoring_boxscores_team'] = format_query(str(schema.scoring_boxscores_team.insert()))
    query_dict['misc_boxscores'] = format_query(str(schema.misc_boxscores.insert()))
    query_dict['misc_boxscores_team'] = format_query(str(schema.misc_boxscores_team.insert()))
    query_dict['usage_boxscores'] = format_query(str(schema.usage_boxscores.insert()))
    query_dict['four_factors_boxscores'] = format_query(str(schema.four_factors_boxscores.insert()))
    query_dict['four_factors_boxscores_team'] = format_query(str(schema.four_factors_boxscores_team.insert()))
    query_dict['other_stats'] = format_query(str(schema.other_stats.insert()))
    query_dict['officials'] = format_query(str(schema.officials.insert()))
    query_dict['inactives'] = format_query(str(schema.inactives.insert()))
  


    #begin asynchronous event loop    
    loop = asyncio.get_event_loop()

    #for games in game_ids_set:
    #    print(games)
    #store game data associated with each game id into mysql 
    #conn = aiohttp.TCPConnector(limit_per_host=5)
    #session = aiohttp.ClientSession(connector=conn)
    try: 
        #gather game_ids in date range
        game_ids = loop.run_until_complete(get_games(start_date, end_date))
        game_ids_set = [games for days in game_ids for games in days]     
    finally: 
        
        loop.close()
    
    print('Number of games: ', len(game_ids_set))
    loop = asyncio.new_event_loop()
    try:
        print('Begin storing data.')
        loop.run_until_complete(process_game_data(host, username, password, database, loop, season, season_type, game_ids_set, query_dict))
    finally:
        print('Done fetching and bulk inserting data. Closing client session.')
        loop.close()
    #for keys in query_dict:
    #    print(query_dict[keys]) 


if __name__ == '__main__':
    main()

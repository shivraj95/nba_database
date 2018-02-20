import json

from . import helper

class GameData:
    def __init__(self, game_id, season, season_type):
        self.game_id = game_id
        self.season = season
        self.season_type = season_type
        self.pbp_base_url = "http://stats.nba.com/stats/playbyplayv2"
        self.player_tracking_boxscore_base_url = "http://stats.nba.com/stats/boxscoreplayertrackv2"
        self.traditional_boxscore_base_url = "http://stats.nba.com/stats/boxscoretraditionalv2"
        self.advanced_boxscore_base_url = "http://stats.nba.com/stats/boxscoreadvancedv2"
        self.scoring_boxscore_base_url = "http://stats.nba.com/stats/boxscorescoringv2"
        self.misc_boxscore_base_url = "http://stats.nba.com/stats/boxscoremiscv2"
        self.usage_boxscore_base_url = "http://stats.nba.com/stats/boxscoreusagev2"
        self.four_factors_boxscore_base_url = "http://stats.nba.com/stats/boxscorefourfactorsv2"
        self.summary_base_url = "http://stats.nba.com/stats/boxscoresummaryv2"
        self.shots_base_url = "http://stats.nba.com/stats/shotchartdetail"
        self.params = {
            'LeagueID':'00', # format: d{2}
            'SeasonID':'00215',
            'Season':0, # format: d{4}-d{2}
            'SeasonYear': season,  # d{4}-d{2}
            'SeasonType': season_type, # format: (Regular Season)|(Pre Season)|(Playoffs)
            'GameID': game_id, 
            'GameEventID':0, # format: 
            'StartPeriod':1, # format: 1-10
            'EndPeriod':10, # format: 1-10
            'StartRange':0,
            'EndRange':0,
            'RangeType':0,
            'AheadBehind':'', # format: (Ahead or Behind)|(Behind or Tied)|(Ahead or Tied)
            'ClutchTime':'', # format: (Last 5 Minutes)|(Last 4 Minutes)|(Last 3 Minutes)|(Last 2 Minutes)|(Last 1 Minute)|(Last 30 Seconds)|(Last 10 Seconds)
            'ContextFilter':'', # format: 
            'ContextMeasure':'FGA', # format: (FGM)|(FGA)|(FG_PCT)|(FG3M)|(FG3A)|(FG3_PCT)|(FTM)|(FTA)|(OREB)|(DREB)|(AST)|(FGM_AST)|(FG3_AST)|(STL)|(BLK)|(BLKA)|(TOV)|(POSS_END_FT)|(PTS_PAINT)|(PTS_FB)|(PTS_OFF_TOV)|(PTS_2ND_CHANCE)|(REB)|(TM_FGM)|(TM_FGA)|(TM_FG3M)|(TM_FG3A)|(TM_FTM)|(TM_FTA)|(TM_OREB)|(TM_DREB)|(TM_REB)|(TM_TEAM_REB)|(TM_AST)|(TM_STL)|(TM_BLK)|(TM_BLKA)|(TM_TOV)|(TM_TEAM_TOV)|(TM_PF)|(TM_PFD)|(TM_PTS)|(TM_PTS_PAINT)|(TM_PTS_FB)|(TM_PTS_OFF_TOV)|(TM_PTS_2ND_CHANCE)|(TM_FGM_AST)|(TM_FG3_AST)|(TM_POSS_END_FT)|(OPP_FTM)|(OPP_FTA)|(OPP_OREB)|(OPP_DREB)|(OPP_REB)|(OPP_TEAM_REB)|(OPP_AST)|(OPP_STL)|(OPP_BLK)|(OPP_BLKA)|(OPP_TOV)|(OPP_TEAM_TOV)|(OPP_PF)|(OPP_PFD)|(OPP_PTS)|(OPP_PTS_PAINT)|(OPP_PTS_FB)|(OPP_PTS_OFF_TOV)|(OPP_PTS_2ND_CHANCE)|(OPP_FGM_AST)|(OPP_FG3_AST)|(OPP_POSS_END_FT)|(PTS))
            'DateFrom':'',  # format: YYYY-MM-DD
            'DateTo':'',  # format: YYYY-MM-DD
            'DistanceRange':'', # format: (5ft Range)|(8ft Range)|(By Zone)
            'GameDate':'', # format: YYYY-MM-DD
            # 'GameScope':'', # format (1): (Season)|(Last 10)|(Yesterday)|(Finals)
            'GameScope':'', # format (2): (Season)|(Last 10)|(Yesterday)|(Finals)
            'GameSegment':'', # format: (First Half)|(Overtime)|(Second Half)
            #  'GraphStartSeason':'2015-11', # format: d{4}-d{2}
            #  'GraphEndSeason':'2016-01', # format: d{4}-d{2}
            #  'GraphStat':'FGM', # format: 
            'GROUP_ID':0,
            'GroupQuantity':1, # format: 1-5
            'IsOnlyCurrentSeason':0, # format: 0-1
            'LastNGames':0,
            'Location':'', # format: (Home)|(Road)
            'MeasureType':'', # format: (Base)|(Advanced)|(Misc)|(Four Factors)|(Scoring)|(Opponent)|(Usage)
            'Month':0,
            'OpponentTeamID':0,
            'Outcome':'', # format: W/L
            'PaceAdjust':'N', # format: Y/N
            'Period':0,
            'PerMode':'Totals', # format: (Totals)|(PerGame)|(MinutesPer)|(Per48)|(Per40)|(Per36)|(PerMinute)|(PerPossession)|(PerPlay)|(Per100Possessions)|(Per100Plays)
            'PlayerID':0,
            'PlayerExperience':'', # format: (Rookie)|(Sophomore)|(Veteran)
            'PlayerOrTeam':'', # format: (Player)|(Team)
            'PlayerPosition':'', # format: (F)|(C)|(G)|(C-F)|(F-C)|(F-G)|(G-F)
            'PlayerScope':'All Players', # format: (All Players)|(Rookies)
            'PlayerTeamID':0,
            'PlusMinus':'Y', # format: Y/N
            'PointDiff':'', # format: 
            'Position':'',
            'Rank':'N', # format: format: Y/N
            'RookieYear':'',
            'Scope':'', # format: (RS)|(S)|(Rookies)
            'Season':'',
            'SeasonSegment':'', # format: (Post All-Star)|(Pre All-Star)
            'StarterBench':'', # format: (Starters)|(Bench)
            'StatCategory':'', # (Points)|(Rebounds)|(Assists)|(Defense)|(Clutch)|(Playmaking)|(Efficiency)|(Fast Break)|(Scoring Breakdown)    
            'TeamID':0,
            'viewShots':'true',
            'VsConference':'', #  'VsConference':'East', # format: (East)|(West)
            'VsDivision':'' # format: (Atlantic)|(Central)|(Northwest)|(Pacific)|(Southeast)|(Southwest)|(East)|(West) 
              #'zone-mode':'basic',
        }

    def pbp(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.pbp_base_url, self.params, 0)

    def player_tracking_boxscore(self):
        
        return helper.get_data_from_url_with_parameters(self.player_tracking_boxscore_base_url, self.params, 0)

    def player_tracking_boxscore_team(self):
        return helper.get_data_from_url_with_parameters(self.player_tracking_boxscore_base_url, self.params, 1)

    def traditional_boxscore(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.traditional_boxscore_base_url, self.params, 0)

    def traditional_boxscore_team(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.traditional_boxscore_base_url, self.params, 1)

    def advanced_boxscore(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.advanced_boxscore_base_url, self.params, 0)

    def advanced_boxscore_team(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.advanced_boxscore_base_url, self.params, 1)

    def scoring_boxscore(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.scoring_boxscore_base_url, self.params, 0)

    def scoring_boxscore_team(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.scoring_boxscore_base_url, self.params, 1)

    def misc_boxscore(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.misc_boxscore_base_url, self.params, 0)

    def misc_boxscore_team(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.misc_boxscore_base_url, self.params, 1)

    def usage_boxscore(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.usage_boxscore_base_url, self.params, 0)

    def four_factors_boxscore(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.four_factors_boxscore_base_url, self.params, 0)

    def four_factors_boxscore_team(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.four_factors_boxscore_base_url, self.params, 1)

    def shots(self):
        return helper.get_data_from_url_with_parameters(self.shots_base_url, self.params, 0)


    def officials(self):
        parameters = {"GameId": self.game_id}
        return helper.get_data_from_url_with_parameters_add_game_id(self.summary_base_url, self.params, self.game_id, 2)

    def other_stats(self):
        parameters = {"GameId": self.game_id}
        return helper.get_data_from_url_with_parameters_add_game_id(self.summary_base_url, self.params, self.game_id, 1)

    def inactives(self):
        parameters = {"GameId": self.game_id}
        return helper.get_data_from_url_with_parameters_add_game_id(self.summary_base_url, self.params, self.game_id, 3)

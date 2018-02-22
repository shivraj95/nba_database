import json

from . import helper

class GameData:
    def __init__(self, game_id, season, season_type, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
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
        self.parameters = {
                        "GameId": self.game_id,
                        "StartPeriod": start_period,
                        "EndPeriod": end_period,
                        "RangeType": range_type,
                        "StartRange": start_range,
                        "EndRange": end_range
        }
        
    def pbp(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.pbp_base_url, self.parameters, 0)

    def player_tracking_boxscore(self):
        params = {"GameId": self.game_id}
        return helper.get_data_from_url_with_parameters(self.player_tracking_boxscore_base_url, params, 0)

    def player_tracking_boxscore_team(self):
        params = {"GameId": self.game_id}
        return helper.get_data_from_url_with_parameters(self.player_tracking_boxscore_base_url, params, 1)

    def traditional_boxscore(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.traditional_boxscore_base_url, self.parameters, 0)

    def traditional_boxscore_team(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.traditional_boxscore_base_url, self.parameters, 1)

    def advanced_boxscore(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.advanced_boxscore_base_url, self.parameters, 0)

    def advanced_boxscore_team(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.advanced_boxscore_base_url, self.parameters, 1)

    def scoring_boxscore(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.scoring_boxscore_base_url, self.parameters, 0)

    def scoring_boxscore_team(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.scoring_boxscore_base_url, self.parameters, 1)

    def misc_boxscore(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.misc_boxscore_base_url, self.parameters, 0)

    def misc_boxscore_team(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.misc_boxscore_base_url, self.parameters, 1)

    def usage_boxscore(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.usage_boxscore_base_url, self.parameters, 0)

    def four_factors_boxscore(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.four_factors_boxscore_base_url, self.parameters, 0)

    def four_factors_boxscore_team(self, start_period=0, end_period=10, range_type=2, start_range=0, end_range=55800):
        
        return helper.get_data_from_url_with_parameters(self.four_factors_boxscore_base_url, self.parameters, 1)

    def shots(self):
        params = {
                            "GameID": self.game_id,
                            "Season": self.season,
                            "SeasonType": self.season_type,
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
        return helper.get_data_from_url_with_parameters(self.shots_base_url, params, 0)


    def officials(self):
        params = {"GameId": self.game_id}
        return helper.get_data_from_url_with_parameters_add_game_id(self.summary_base_url, params, self.game_id, 2)

    def other_stats(self):
        params= {"GameId": self.game_id}
        return helper.get_data_from_url_with_parameters_add_game_id(self.summary_base_url, params, self.game_id, 1)

    def inactives(self):
        params = {"GameId": self.game_id}
        return helper.get_data_from_url_with_parameters_add_game_id(self.summary_base_url, params, self.game_id, 3)

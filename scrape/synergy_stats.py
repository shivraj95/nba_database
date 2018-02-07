import json

from . import helper

class SynergyData:
    def __init__(self, season, season_type):
        self.params = {
                "category": None,
                "limit": "500",
                "names": None,
                "q": "2529493",
                "season": season, 
                "seasonType": season_type
        }       
        self.team_url = "https://stats-prod.nba.com/wp-json/statscms/v1/synergy/team"
        self.player_url = "https://stats-prod.nba.com/wp-json/statscms/v1/synergy/player"

    def get_synergy_data_for_stat(self, is_team, playtype, scheme):
        self.params['category'] = playtype
        self.params['names'] = scheme
        if is_team:
            return helper.get_data_from_url_with_parameters_synergy(self.team_url, self.params)
        else:
            return helper.get_data_from_url_with_parameters_synergy(self.player_url, self.params)

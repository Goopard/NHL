import requests

from functools import reduce

from operator import add

from tqdm import tqdm

import numpy as np
import pandas as pd


def get_team_stats(team_id):
    """Downloads the raw stats for a team.

    :param team_id: Id of a team.
    :return: dict.
    """
    request = 'https://statsapi.web.nhl.com/api/v1/teams/{}/stats'.format(team_id)
    return requests.get(request)


def get_teams():
    """Downloads the list of teams.

    :return: list.
    """
    teams = (requests.get('https://statsapi.web.nhl.com/api/v1/teams').json())['teams']
    return teams


def get_game(game_id):
    """Downloads the raw stats for a single game based on the game's id.

    :param game_id: Game id.
    :type game_id: int.
    :return: dict.
    """
    request = 'https://statsapi.web.nhl.com/api/v1/game/{}/feed/live'.format(game_id)
    return requests.get(request).json()['liveData']


def get_game_stats(game_id):
    """Downloads the stats for a single game based on the game's id as a pandas DataFrame.

    :param game_id: Game id.
    :type game_id: int.
    :return: pandas.DataFrame.
    """
    game = get_game(game_id)
    game_away_stats = game['boxscore']['teams']['away']['teamStats']['teamSkaterStats']
    game_home_stats = game['boxscore']['teams']['home']['teamStats']['teamSkaterStats']
    away_id = game['boxscore']['teams']['away']['team']['id']
    home_id = game['boxscore']['teams']['home']['team']['id']

    periods = [[period['home']['goals'], period['home']['shotsOnGoal'],
                period['away']['goals'], period['away']['shotsOnGoal']]
               for period in game['linescore']['periods']]
    periods = periods + [[np.nan] * 4] * (3 - len(periods))

    game_home_stats.update(
        {
            'id': away_id,
            'game_id': game_id,
            'away': False,
            '1_period_our_goals': periods[0][0],
            '1_period_their_goals': periods[0][2],
            '2_period_our_goals': periods[1][0],
            '2_period_their_goals': periods[1][2],
            '3_period_our_goals': periods[2][0],
            '3_period_their_goals': periods[2][2],
            '1_period_our_shots': periods[0][1],
            '1_period_their_shots': periods[0][3],
            '2_period_our_shots': periods[1][1],
            '2_period_their_shots': periods[1][3],
            '3_period_our_shots': periods[2][1],
            '3_period_their_shots': periods[2][3]
        })
    game_away_stats.update(
        {
            'id': away_id,
            'game_id': game_id,
            'away': True,
            '1_period_their_goals': periods[0][0],
            '1_period_our_goals': periods[0][2],
            '2_period_their_goals': periods[1][0],
            '2_period_our_goals': periods[1][2],
            '3_period_their_goals': periods[2][0],
            '3_period_our_goals': periods[2][2],
            '1_period_their_shots': periods[0][1],
            '1_period_our_shots': periods[0][3],
            '2_period_their_shots': periods[1][1],
            '2_period_our_shots': periods[1][3],
            '3_period_their_shots': periods[2][1],
            '3_period_our_shots': periods[2][3]
        })
    game_stats_df = pd.DataFrame([game_away_stats, game_home_stats])
    game_stats_df['OT'] = game['linescore']['currentPeriod'] > 3
    return game_stats_df


def get_schedule(start_date, end_date):
    """Downloads the schedule of games in the given period.

    :param start_date: Starting date.
    :type start_date: str.
    :param end_date: Ending date.
    :type end_date: str.
    :return: dict.
    """
    request = 'https://statsapi.web.nhl.com/api/v1/schedule?startDate={}&endDate={}'.format(start_date, end_date)
    return requests.get(request).json()


def get_games_df(start_date, end_date):
    """Downloads the stats for all games in the given period as a pandas DataFrame.

    :param start_date: Starting date.
    :type start_date: str.
    :param end_date: Ending date.
    :type end_date: str.
    :return: pandas.DataFrame.
    """
    schedule = get_schedule(start_date, end_date)['dates']
    game_ids = reduce(add, [[game['gamePk'] for game in date['games']] for date in schedule], [])

    print('Total number of games to download: ', len(game_ids))
    games_stats = []
    for game_id in tqdm(game_ids):
        try:
            games_stats.append(get_game_stats(game_id))
        except IndexError:
            print(game_id)
            raise ValueError

    return pd.concat(games_stats)

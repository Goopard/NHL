import requests

from functools import reduce

from operator import add

import pandas as pd


def get_team_stats(team_id):
    request = 'https://statsapi.web.nhl.com/api/v1/teams/{}/stats'.format(team_id)
    return requests.get(request)


def get_teams():
    teams = (requests.get('https://statsapi.web.nhl.com/api/v1/teams').json())['teams']
    return teams


def get_game(game_id):
    request = 'https://statsapi.web.nhl.com/api/v1/game/{}/feed/live'.format(game_id)
    return requests.get(request).json()['liveData']


def get_game_stats(game_id):
    game = get_game(game_id)
    game_away_stats = game['boxscore']['teams']['away']['teamStats']['teamSkaterStats']
    game_home_stats = game['boxscore']['teams']['home']['teamStats']['teamSkaterStats']
    away_id = game['boxscore']['teams']['away']['team']['id']
    home_id = game['boxscore']['teams']['home']['team']['id']

    periods = [[period['home']['goals'], period['home']['shotsOnGoal'],
                period['away']['goals'], period['away']['shotsOnGoal']]
               for period in game['linescore']['periods']]

    game_away_stats.update(
        {
            'id': away_id,
            'game_id': game_id,
            'away': True,
            '1_period_our_goals': periods[0][1],
            '1_period_their_goals': periods[0][0],
            '2_period_our_goals': periods[1][1],
            '2_period_their_goals': periods[1][0],
            '3_period_our_goals': periods[2][1],
            '3_period_their_goals': periods[2][0]
        })
    game_home_stats.update(
        {
            'id': home_id,
            'game_id': game_id,
            'away': False,
            '1_period_our_goals': periods[0][0],
            '1_period_their_goals': periods[0][1],
            '2_period_our_goals': periods[1][0],
            '2_period_their_goals': periods[1][1],
            '3_period_our_goals': periods[2][0],
            '3_period_their_goals': periods[2][1]
        })
    game_stats_df = pd.DataFrame([game_away_stats, game_home_stats])
    game_stats_df['OT'] = game['linescore']['currentPeriod'] > 3
    return game_stats_df


def get_schedule(start_date, end_date):
    request = 'https://statsapi.web.nhl.com/api/v1/schedule?startDate={}&endDate={}'.format(start_date, end_date)
    return requests.get(request).json()


def get_games_df(start_date, end_date):
    schedule = get_schedule(start_date, end_date)['dates']
    game_ids = reduce(add, [[game['gamePk'] for game in date['games']] for date in schedule], [])
    games_stats = [get_game_stats(game_id) for game_id in game_ids]
    return pd.concat(games_stats)


if __name__ == '__main__':
    #get_games_df(start_date='2018-10-01', end_date='2018-12-06').to_csv('..\\test_data\\games.csv', index=False)
    print('abacana'.ljust(10))
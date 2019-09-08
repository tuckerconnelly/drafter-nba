"""
Module for getting data
"""

import statistics
import random
import functools
import logging
import time
import multiprocessing
import json
import sys
import os
import pprint
import datetime

from sklearn.preprocessing import LabelBinarizer, MultiLabelBinarizer
import numpy as np
from joblib import Memory
import progressbar
import dateparser

import services


ABBREVIATIONS = {
    'NO': 'NOP',
    'DAL': 'DAL',
    'CHA': 'CHO',
    'BKN': 'BRK',
    'TOR': 'TOR',
    'MIA': 'MIA',
    'WAS': 'WAS',
    'DET': 'DET',
    'PHO': 'PHO',
    'ORL': 'ORL',
    'MIN': 'MIN',
    'CHI': 'CHI',
    'CLE': 'CLE',
    'MEM': 'MEM',
    'DEN': 'DEN',
    'SA': 'SAS',
    'SAC': 'SAC',
    'LAC': 'LAC',
    'IND': 'IND',
    'ATL': 'ATL',
    'BOS': 'BOS',
    'HOU': 'HOU',
    'POR': 'POR',
    'GS': 'GSW',
    'MIL': 'MIL',
    'NY': 'NYK',
    'LAL': 'LAL',
    'PHI': 'PHI',
    'UTA': 'UTA',
    'OKC': 'OKC'
}


class MeanMinMaxEncoder:
    def __init__(self, mean, min, max, output_min=0.1, output_max=0.9):
        assert mean is not None
        assert min is not None
        assert max is not None

        self.mean = mean
        self.min = min
        self.max = max
        self.output_range = output_max - output_min
        self.output_min = output_min

    def transform(self, raw_value):
        return (float((raw_value - self.mean) / (self.max - self.min)) / 2 * self.output_range) + 0.5 + self.output_min

    # TODO Test this
    def inverse_transform(self, encoded_value):
        without_range = (encoded_value - 0.5 - self.output_min) / self.output_range * 2
        return float((without_range * (self.max - self.min)) + self.mean)


class SineEncoder:
    def __init__(self, min, max, output_max=0.90, output_min=0.1):
        assert min is not None
        assert max is not None

        self.min = min
        self.max = max
        self.output_amplitude = output_max - output_min
        self.output_min = output_min

    def transform(self, raw_value):
        return np.sin(2 * np.pi * (raw_value / (self.max - self.min))) * self.output_amplitude + 1 + self.output_min


# Cache Data #


MIN_SECONDS_PLAYED_IN_GAME = 12 * 60
MIN_GAMES_PLAYED_PER_SEASON = 15


def calculate_fantasy_score(stats):
    if stats['seconds_played'] == None:
        return None

    if stats['seconds_played'] == 0:
        return 0

    double_double_stat_count = len(list(filter(lambda s: isinstance(s, int) and s >= 10, [
        stats['points'],
        stats['total_rebounds'],
        stats['assists'],
        stats['steals'],
        stats['blocks'],
    ])))

    return (
        (stats['points'] or 0)
        + (stats['three_point_field_goals'] or 0) * 0.5
        + (stats['total_rebounds'] or 0) * 1.25
        + (stats['assists'] or 0) * 1.5
        + (stats['steals'] or 0) * 2
        + (stats['blocks'] or 0) * 2
        + (stats['turnovers'] or 0) * -0.5
        + (1.5 if double_double_stat_count >= 2 else 0)
        + (3 if double_double_stat_count >= 3 else 0)
    )


def calculate_fppm(stats):
    if stats['seconds_played'] == None:
        return None

    if stats['seconds_played'] == 0:
        return 0

    return calculate_fantasy_score(stats) / (stats['seconds_played'] / 60)


def get_stats_last_games_from_pg(
    player_basketball_reference_id,
    season,
    current_game_date,
    player_team,
    opp_team,
    player_position,
    away_team=None,
    home_team=None
):
    team_sql = ''
    if away_team and home_team:
        team_sql = f'''
            and g.away_team_basketball_reference_id = '{away_team}'
            and g.home_team_basketball_reference_id = '{home_team}'
        '''

    rows = services.sql.execute(f"""
        select
          gp.points,
          gp.three_point_field_goals,
          gp.total_rebounds,
          gp.assists,
          gp.blocks,
          gp.steals,
          gp.turnovers,
          gp.seconds_played,
          gp.plus_minus,
          gp.field_goals_attempted,
          gp.free_throws_attempted,

          g.time_of_game
        from games g
        left join games_players as gp
          on gp.game_basketball_reference_id = g.basketball_reference_id
          and gp.player_basketball_reference_id = '{player_basketball_reference_id}'
          and gp.seconds_played > {MIN_SECONDS_PLAYED_IN_GAME}
        inner join teams_players as tp
          on tp.player_basketball_reference_id = '{player_basketball_reference_id}'
          and tp.season = g.season
        where g.season = {season}
        and (
          g.home_team_basketball_reference_id = tp.team_basketball_reference_id
          or g.away_team_basketball_reference_id = tp.team_basketball_reference_id
        )
        and g.time_of_game < datetime('{current_game_date}')
        {team_sql}
        order by datetime(g.time_of_game) desc
    """).fetchall()

    opp_teams_rows = services.sql.execute(
        f"""
            select
                sum(gpc.dk_fantasy_points) as opp_dk_fantasy_points_allowed_vs_position
            from games_players_computed as gpc
            inner join games as g
           	  on g.basketball_reference_id = gpc.game_basketball_reference_id
            inner join teams_players as tp
              on tp.player_basketball_reference_id = gpc.player_basketball_reference_id
              and tp.season = g.season
              and tp.team_basketball_reference_id != '{opp_team}'
              and tp.position = '{player_position}'
            where (
              g.away_team_basketball_reference_id = '{opp_team}'
              or g.home_team_basketball_reference_id = '{opp_team}'
            )
            and g.season = {season}
            and g.time_of_game < datetime('{current_game_date}')
            {team_sql}
            group by g.id
            order by datetime(g.time_of_game) desc
        """
    ).fetchall()

    return {
        'points_last_games_last_games': list(map(lambda r: r['points'], rows)),
        'three_point_field_goals_last_games': list(map(lambda r: r['three_point_field_goals'], rows)),
        'total_rebounds_last_games': list(map(lambda r: r['total_rebounds'], rows)),
        'assists_last_games': list(map(lambda r: r['assists'], rows)),
        'blocks_last_games': list(map(lambda r: r['blocks'], rows)),
        'steals_last_games': list(map(lambda r: r['steals'], rows)),
        'turnovers_last_games': list(map(lambda r: r['turnovers'], rows)),
        'seconds_played_last_games': list(map(lambda r: r['seconds_played'], rows)),
        'dk_fantasy_points_last_games': list(map(lambda r: calculate_fantasy_score(r), rows)),
        'times_of_games': list(map(lambda r: r['time_of_game'], rows)),
        'plus_minus_last_games': list(map(lambda r: r['plus_minus'], rows)),
        'dk_fantasy_points_per_minute_last_games': list(map(lambda r: calculate_fppm(r), rows)),
        'opp_dk_fantasy_points_allowed_vs_position_last_games': list(map(lambda r: r['opp_dk_fantasy_points_allowed_vs_position'], opp_teams_rows))
    }


def cache_single_games_player(
    games_player,
    player_team_basketball_reference_id,
    opp_team_basketball_reference_id,
    season,
    time_of_game,
    position
):
    assert games_player['player_basketball_reference_id']

    stats_last_games = get_stats_last_games_from_pg(
        player_basketball_reference_id=games_player['player_basketball_reference_id'],
        season=season,
        current_game_date=time_of_game,
        player_team=player_team_basketball_reference_id,
        opp_team=opp_team_basketball_reference_id,
        player_position=position
    )
    opp_dk_fantasy_points_allowed_vs_position_last_game_only = None
    if len(stats_last_games['opp_dk_fantasy_points_allowed_vs_position_last_games']) > 0:
        opp_dk_fantasy_points_allowed_vs_position_last_game_only = stats_last_games['opp_dk_fantasy_points_allowed_vs_position_last_games'][0] 

    stats_last_games_against_opp_away = get_stats_last_games_from_pg(
        player_basketball_reference_id=games_player['player_basketball_reference_id'],
        season=season,
        current_game_date=time_of_game,
        player_team=player_team_basketball_reference_id,
        opp_team=opp_team_basketball_reference_id,
        player_position=position,
        away_team=player_team_basketball_reference_id,
        home_team=opp_team_basketball_reference_id
    )

    stats_last_games_against_opp_home = get_stats_last_games_from_pg(
        player_basketball_reference_id=games_player['player_basketball_reference_id'],
        season=season,
        current_game_date=time_of_game,
        player_team=player_team_basketball_reference_id,
        opp_team=opp_team_basketball_reference_id,
        player_position=position,
        away_team=opp_team_basketball_reference_id,
        home_team=player_team_basketball_reference_id
    )

    services.sql.execute(
        f'''
            delete from games_players_computed
            where game_basketball_reference_id = '{games_player['game_basketball_reference_id']}'
            and player_basketball_reference_id = '{games_player['player_basketball_reference_id']}'
        ''')

    services.sql.execute(
        f'''
            insert into games_players_computed (
                game_basketball_reference_id,
                player_basketball_reference_id,

                times_of_last_games,
                times_of_last_games_against_opp_away,
                times_of_last_games_against_opp_home,

                dk_fantasy_points,
                dk_fantasy_points_last_games,
                dk_fantasy_points_last_games_against_opp_away,
                dk_fantasy_points_last_games_against_opp_home,

                seconds_played_last_games,
                seconds_played_last_games_against_opp_away,
                seconds_played_last_games_against_opp_home,

                plus_minus_last_games,
                plus_minus_last_games_against_opp_away,
                plus_minus_last_games_against_opp_home,

                dk_fantasy_points_per_minute,
                dk_fantasy_points_per_minute_last_games,
                dk_fantasy_points_per_minute_last_games_against_opp_away,
                dk_fantasy_points_per_minute_last_games_against_opp_home,

                opp_dk_fantasy_points_allowed_vs_position_last_game_only,
                opp_dk_fantasy_points_allowed_vs_position_last_games,
                opp_dk_fantasy_points_allowed_vs_position_last_games_away,
                opp_dk_fantasy_points_allowed_vs_position_last_games_home
            )
            values (
                '{games_player['game_basketball_reference_id']}',
                '{games_player['player_basketball_reference_id']}',

                '{json.dumps(stats_last_games['times_of_games'])}',
                '{json.dumps(stats_last_games_against_opp_away['times_of_games'])}',
                '{json.dumps(stats_last_games_against_opp_home['times_of_games'])}',

                {json.dumps(calculate_fantasy_score(games_player))},
                '{json.dumps(stats_last_games['dk_fantasy_points_last_games'])}',
                '{json.dumps(stats_last_games_against_opp_away['dk_fantasy_points_last_games'])}',
                '{json.dumps(stats_last_games_against_opp_home['dk_fantasy_points_last_games'])}',

                '{json.dumps(stats_last_games['seconds_played_last_games'])}',
                '{json.dumps(stats_last_games_against_opp_away['seconds_played_last_games'])}',
                '{json.dumps(stats_last_games_against_opp_home['seconds_played_last_games'])}',

                '{json.dumps(stats_last_games['plus_minus_last_games'])}',
                '{json.dumps(stats_last_games_against_opp_away['plus_minus_last_games'])}',
                '{json.dumps(stats_last_games_against_opp_home['plus_minus_last_games'])}',

                {json.dumps(calculate_fppm(games_player))},
                '{json.dumps(stats_last_games['dk_fantasy_points_per_minute_last_games'])}',
                '{json.dumps(stats_last_games_against_opp_away['dk_fantasy_points_per_minute_last_games'])}',
                '{json.dumps(stats_last_games_against_opp_home['dk_fantasy_points_per_minute_last_games'])}',

                {json.dumps(opp_dk_fantasy_points_allowed_vs_position_last_game_only)},
                '{json.dumps(stats_last_games['opp_dk_fantasy_points_allowed_vs_position_last_games'])}',
                '{json.dumps(stats_last_games_against_opp_away['opp_dk_fantasy_points_allowed_vs_position_last_games'])}',
                '{json.dumps(stats_last_games_against_opp_home['opp_dk_fantasy_points_allowed_vs_position_last_games'])}'
            )
        '''
    )

    services.sql.commit()

    # if os.environ.get('DEBUG') == '1':
    #     pprint.pprint(gpc)


def get_valid_season_players():
    player_season_averages = services.sql.execute(
        '''
            select gp.player_basketball_reference_id, tp.season, avg(gp.seconds_played) as avg_seconds_played, count(gp.player_basketball_reference_id) as games_played
            from games_players as gp
            inner join games as g
            	on g.basketball_reference_id = gp.game_basketball_reference_id
            inner join teams_players as tp
            	on tp.player_basketball_reference_id = gp.player_basketball_reference_id
            	and (
            		tp.team_basketball_reference_id = g.home_team_basketball_reference_id
            		or tp.team_basketball_reference_id = g.away_team_basketball_reference_id
            	)
            	and tp.season = g.season
            group by gp.player_basketball_reference_id, tp.season
        '''
    ).fetchall()

    valid_season_players = {}
    for player_season in player_season_averages:
        if player_season['avg_seconds_played'] < MIN_SECONDS_PLAYED_IN_GAME:
            continue
        if player_season['games_played'] < MIN_GAMES_PLAYED_PER_SEASON:
            continue

        valid_season_players[f"{player_season['season']}_{player_season['player_basketball_reference_id']}"] = player_season

    return valid_season_players


def cache_data_for_season(season):
    print(f'Caching games_players for season {season}')

    # Only compute new games_players in real mode
    gp_debug_sql = '''
        and not exists (
        	select id
        	from games_players_computed as gpc
        	where gpc.game_basketball_reference_id = gp.game_basketball_reference_id
        	and gpc.player_basketball_reference_id = gp.player_basketball_reference_id
        )
        order by datetime(g.time_of_game) asc
    '''

    # Only get a recent James Harden game in debug mode
    if os.environ.get('DEBUG') == '1':
        gp_debug_sql = '''
          and gp.player_basketball_reference_id = 'hardeja01'
          order by datetime(g.time_of_game) asc
          limit 1
        '''

    games_players = services.sql.execute(
        f'''
          select
            gp.game_basketball_reference_id,
            gp.player_basketball_reference_id,
            gp.seconds_played,
            gp.points,
            gp.three_point_field_goals,
            gp.total_rebounds,
            gp.assists,
            gp.steals,
            gp.blocks,
            gp.turnovers,
            gp.free_throws,
            gp.field_goals_attempted,
            gp.free_throws_attempted,

            g.season,
            g.time_of_game,
            tp.team_basketball_reference_id as player_team_basketball_reference_id,
            tp.position as position,
            (case when g.home_team_basketball_reference_id = tp.team_basketball_reference_id then g.away_team_basketball_reference_id else g.home_team_basketball_reference_id end) as opp_team_basketball_reference_id
          from games_players as gp
          inner join games as g
            on g.basketball_reference_id = gp.game_basketball_reference_id
          inner join teams_players as tp
            on tp.player_basketball_reference_id = gp.player_basketball_reference_id
            and tp.season = g.season
          where g.season = {season}
          {gp_debug_sql}
        '''
    ).fetchall()

    print(f"Total games_players: {len(games_players)}")

    valid_season_players = get_valid_season_players()
    games_players = list(filter(
        lambda gp: valid_season_players.get(f"{gp['season']}_{gp['player_basketball_reference_id']}", False),
        games_players
    ))
    # Add averages to games_players
    games_players = list(map(
        lambda gp: {**gp, **valid_season_players.get(f"{gp['season']}_{gp['player_basketball_reference_id']}", {})},
        games_players
    ))

    print(f"Valid games_players: {len(games_players)}")

    widgets = [
        ' [', progressbar.Timer(), '] ',
        progressbar.Bar(),
        ' (', progressbar.ETA(), ') '
    ]
    for i in progressbar.progressbar(range(len(games_players)), widgets=widgets):
        cache_single_games_player(
            games_players[i],
            games_players[i]['player_team_basketball_reference_id'],
            games_players[i]['opp_team_basketball_reference_id'],
            games_players[i]['season'],
            games_players[i]['time_of_game'],
            games_players[i]['position']
        )
    
    services.sql.commit()


def cache_data():
    seasons = services.sql.execute(
        'select distinct season from games order by season asc'
    ).fetchall()
    seasons = list(map(lambda r: r['season'], seasons))

    p = multiprocessing.Pool(int(multiprocessing.cpu_count() / 2) - 1)
    p.map(cache_data_for_season, seasons)


# Load Data ##

def add_age(gp):
    time_of_game = datetime.datetime.strptime(gp['time_of_game'], "%Y-%m-%d %H:%M:%S")
    date_of_birth = datetime.datetime.strptime(gp['date_of_birth'], "%Y-%m-%d")
    age = int((time_of_game - date_of_birth).days / 365)
    return {**gp, 'age_at_time_of_game': age}


def get_data(
    limit=None,
    offset=None,
    player_basketball_reference_id=None,
    game_basketball_reference_id=None,
    season=None
):
    limit_sql = ''
    if limit is not None:
        limit_sql = f'limit {limit}'
    
    offset_sql = ''
    if offset is not None:
        offset_sql = f'offset {offset}'

    player_sql = ''
    if player_basketball_reference_id is not None:
        player_sql = f"and gp.player_basketball_reference_id = '{player_basketball_reference_id}'"

    game_sql = ''
    if game_basketball_reference_id is not None:
        game_sql = f"and gp.game_basketball_reference_id = '{game_basketball_reference_id}'"
    
    season_sql = ''
    if season is not None:
        season_sql = f"and g.season = {season}"

    games_players = services.sql.execute(
        f"""
             	select
                  gp.game_basketball_reference_id,
                  p.name as player_name,
                  t.name as team_name,
                  g.season as season,
                  g.time_of_game as time_of_game,

                  p.date_of_birth,
                  tp.height_inches,
                  tp.weight_lbs,
                  tp.experience,
                  (g.home_team_basketball_reference_id = t.basketball_reference_id) as playing_at_home,
                  gp.starter,
                  cast(strftime('%Y', g.time_of_game) as int) as year_of_game,
                  cast(strftime('%m', g.time_of_game) as int) as month_of_game,
                  cast(strftime('%d', g.time_of_game) as int) as day_of_game,

                  gpc.dk_fantasy_points_last_games,
                  gpc.dk_fantasy_points_last_games_against_opp_away,
                  gpc.dk_fantasy_points_last_games_against_opp_home,

                  gp.seconds_played,
                  gpc.seconds_played_last_games,
                  gpc.seconds_played_last_games_against_opp_away,
                  gpc.seconds_played_last_games_against_opp_home,

                  gpc.dk_fantasy_points_per_minute,
                  gpc.dk_fantasy_points_per_minute_last_games,
                  gpc.dk_fantasy_points_per_minute_last_games_against_opp_away,
                  gpc.dk_fantasy_points_per_minute_last_games_against_opp_home,

                  gpc.opp_dk_fantasy_points_allowed_vs_position_last_game_only,
                  gpc.opp_dk_fantasy_points_allowed_vs_position_last_games,
                  gpc.opp_dk_fantasy_points_allowed_vs_position_last_games_away,
                  gpc.opp_dk_fantasy_points_allowed_vs_position_last_games_home,

                  gpc.times_of_last_games,
                  gpc.times_of_last_games_against_opp_away,
                  gpc.times_of_last_games_against_opp_home,

                  tp.team_basketball_reference_id as player_team_basketball_reference_id,
                  (case when g.home_team_basketball_reference_id = t.basketball_reference_id then g.away_team_basketball_reference_id else g.home_team_basketball_reference_id end) as opposing_team_basketball_reference_id,
                  tp.position,
                  
                  gp.player_basketball_reference_id,


                  gpc.dk_fantasy_points
                from games_players as gp
                inner join players as p
                  on p.basketball_reference_id = gp.player_basketball_reference_id
                inner join games as g
                  on g.basketball_reference_id = gp.game_basketball_reference_id
                inner join teams_players as tp
                  on tp.player_basketball_reference_id = gp.player_basketball_reference_id
                  and (
                  	tp.team_basketball_reference_id = g.away_team_basketball_reference_id
                  	or tp.team_basketball_reference_id = g.home_team_basketball_reference_id
                  )
                  and tp.season = g.season
                inner join teams as t
                  on t.basketball_reference_id = tp.team_basketball_reference_id
                left join games_players_computed as gpc
                  on gpc.game_basketball_reference_id = gp.game_basketball_reference_id
                  and gpc.player_basketball_reference_id = gp.player_basketball_reference_id
                where gp.seconds_played > {MIN_SECONDS_PLAYED_IN_GAME}
                {player_sql}
                {game_sql}
                {season_sql}
                group by gp.id
                {limit_sql}
                {offset_sql}
        """
    ).fetchall()

    print(f"Total games_players: {len(games_players)}")

    valid_season_players = get_valid_season_players()
    games_players = list(filter(
        lambda gp: valid_season_players.get(f"{gp['season']}_{gp['player_basketball_reference_id']}", False),
        games_players
    ))
    # Add averages to games_players
    games_players = list(map(
        lambda gp: {**gp, **valid_season_players.get(f"{gp['season']}_{gp['player_basketball_reference_id']}", {})},
        games_players
    ))

    print(f"Valid games_players: {len(games_players)}")

    print('Calculating ages')

    try:
        p = multiprocessing.Pool(int(multiprocessing.cpu_count() / 2) - 1)
        games_players = p.map(add_age, games_players)
    except AssertionError:
        games_players = list(map(add_age, games_players))

    print('Done calculating ages')

    return games_players


### Cache features ###


def cache_features():
    # NOTE Calling to build cache for other processes
    print('Warming cache')
    make_mappers()
    print('Done warming cache')

    # Get data and compute features

    games_players = []
    if os.environ.get('DEBUG') == '1':
        games_players = get_data(season=2019)
    else:
        games_players = get_data()

    p = multiprocessing.Pool(int(multiprocessing.cpu_count() / 2))
    computed_features = p.map(compute_features_single_row, games_players)

    # Insert features

    services.sql.isolation_level = None
    services.sql.execute('pragma journal_mode=WAL')

    services.sql.execute('begin')
    services.sql.execute(f'delete from computed_features')
    for cf in computed_features:
        services.sql.execute(
            f'''
                insert into computed_features (
                    game_basketball_reference_id,
                    player_basketball_reference_id,
                    season,
                    x,
                    y,
                    sw
                )
                values (
                    '{cf['game_basketball_reference_id']}',
                    '{cf['player_basketball_reference_id']}',
                    {cf['season']},
                    '{cf['x']}',
                    '{cf['y']}',
                    '{cf['sw']}'
                )
            '''
        )
    services.sql.execute('end')
    services.sql.commit()

def compute_features_single_row(datum):
    mappers = make_mappers()

    return {
        'game_basketball_reference_id': datum['game_basketball_reference_id'],
        'player_basketball_reference_id': datum['player_basketball_reference_id'],
        'season': datum['season'],
        'x': mappers.datum_to_x(datum).tolist(),
        'y': mappers.datum_to_y(datum),
        'sw': mappers.datum_to_sw(datum)
    }


def get_mapped_data(
    limit=None,
    offset=None,
    player_basketball_reference_id=None,
    game_basketball_reference_id=None,
    season=None
):
    limit_sql = ''
    if limit is not None:
        limit_sql = f'limit {limit}'
    
    offset_sql = ''
    if offset is not None:
        offset_sql = f'offset {offset}'

    player_sql = ''
    if player_basketball_reference_id is not None:
        player_sql = f"and cf.player_basketball_reference_id = '{player_basketball_reference_id}'"

    game_sql = ''
    if game_basketball_reference_id is not None:
        game_sql = f"and cf.game_basketball_reference_id = '{game_basketball_reference_id}'"

    season_sql = ''
    if season is not None:
        season_sql = f'and cf.season = {season}'

    data = services.sql.execute(
        f'''
            select *
            from computed_features as cf
            where 1
            {player_sql}
            {game_sql}
            {season_sql}
            {limit_sql}
            {offset_sql}
        '''
    ).fetchall()

    assert len(data) != 0

    random.Random(0).shuffle(data)

    return {
        'x': np.stack([json.loads(d['x']) for d in data]),
        'y': np.stack([json.loads(d['y']) for d in data]),
        'sw': np.stack([json.loads(d['sw']) for d in data])
    }


def get_stats():
    print('Getting stats')

    games_players = get_data()

    print('..Mapping')

    times_of_games = [gp['time_of_game'] for gp in games_players]
    age_at_time_of_games = [gp['age_at_time_of_game'] for gp in games_players]
    experiences = [gp['experience'] for gp in games_players]
    height_inches = [gp['height_inches'] for gp in games_players]
    weight_lbs = [gp['weight_lbs'] for gp in games_players]
    year_of_games = [int(gp['year_of_game']) for gp in games_players]
    month_of_games = [int(gp['month_of_game']) for gp in games_players]
    day_of_games = [int(gp['day_of_game']) for gp in games_players]
    seconds_playeds = [gp['seconds_played'] for gp in games_players]
    dk_fantasy_points_per_minutes = [gp['dk_fantasy_points_per_minute'] for gp in games_players]
    opp_dk_fantasy_points_allowed_vs_position_last_games = [gp['opp_dk_fantasy_points_allowed_vs_position_last_game_only'] for gp in games_players if gp['opp_dk_fantasy_points_allowed_vs_position_last_game_only'] is not None]
    dk_fantasy_points = [gp['dk_fantasy_points'] for gp in games_players if gp['dk_fantasy_points'] is not None]

    assert len(opp_dk_fantasy_points_allowed_vs_position_last_games) > 500000
    assert len(dk_fantasy_points) > 500000

    print('..Done mapping')
    print('..Calculating')

    stats = {
        'time_of_first_game': min(times_of_games),
        'time_of_most_recent_game': max(times_of_games),

        'age_at_time_of_game_avg': statistics.mean(age_at_time_of_games),
        'age_at_time_of_game_min': min(age_at_time_of_games),
        'age_at_time_of_game_max': max(age_at_time_of_games),

        'experience_avg': statistics.mean(experiences),
        'experience_min': min(experiences),
        'experience_max': max(experiences),

        'height_inches_avg': statistics.mean(height_inches),
        'height_inches_min': min(height_inches),
        'height_inches_max': max(height_inches),

        'weight_lbs_avg': statistics.mean(weight_lbs),
        'weight_lbs_min': min(weight_lbs),
        'weight_lbs_max': max(weight_lbs),

        'year_of_game_avg': statistics.mean(year_of_games),
        'year_of_game_min': min(year_of_games),
        'year_of_game_max': max(year_of_games),

        'month_of_game_avg': statistics.mean(month_of_games),
        'month_of_game_min': min(month_of_games),
        'month_of_game_max': max(month_of_games),

        'day_of_game_avg': statistics.mean(day_of_games),
        'day_of_game_min': min(day_of_games),
        'day_of_game_max': max(day_of_games),

        'seconds_played_avg': statistics.mean(seconds_playeds),
        'seconds_played_min': min(seconds_playeds),
        'seconds_played_max': max(seconds_playeds),

        'dk_fantasy_points_per_minute_avg': statistics.mean(dk_fantasy_points_per_minutes),
        'dk_fantasy_points_per_minute_min': min(dk_fantasy_points_per_minutes),
        'dk_fantasy_points_per_minute_max': max(dk_fantasy_points_per_minutes),

        'dk_fantasy_points_allowed_vs_position_avg': statistics.mean(opp_dk_fantasy_points_allowed_vs_position_last_games),
        'dk_fantasy_points_allowed_vs_position_min': min(opp_dk_fantasy_points_allowed_vs_position_last_games),
        'dk_fantasy_points_allowed_vs_position_max': max(opp_dk_fantasy_points_allowed_vs_position_last_games),

        'dk_fantasy_points_avg': statistics.mean(dk_fantasy_points),
        'dk_fantasy_points_min': min(dk_fantasy_points),
        'dk_fantasy_points_max': max(dk_fantasy_points)
    }

    print('..Done calculating')

    pprint.pprint(stats)

    print('Done getting stats')

    return stats


def get_players():
    rows = services.sql.execute(
        '''
          select basketball_reference_id
          from players as p
          inner join teams_players as tp
            on tp.player_basketball_reference_id = p.basketball_reference_id
          where tp.season = 2019;
        '''
    ).fetchall()

    valid_season_players = get_valid_season_players()
    rows = filter(
        lambda r: valid_season_players.get(f"2019_{r['basketball_reference_id']}"),
        rows
    )
    return set(map(lambda r: r['basketball_reference_id'], rows))


SUFFIXES = ['Jr.', 'II', 'III', 'IV', 'V']


def format_player_name(name):
    parts = name.strip().split(' ')
    first = parts[0][0]
    last = parts[-1]

    if (last in SUFFIXES):
        last = parts[-2]

    return '{first}. {last}'.format(first=first, last=last)


@functools.lru_cache()
def get_players_by_team_and_formatted_name():
    players = services.sql.execute(
        """
            select p.*, tp.team_basketball_reference_id
            from players as p
            inner join teams_players as tp on tp.player_basketball_reference_id = p.basketball_reference_id
            where season = 2019
        """
    ).fetchall()

    valid_season_players = get_valid_season_players()
    players = list(filter(
        lambda r: valid_season_players.get(f"2019_{r['basketball_reference_id']}"),
        players
    ))

    def tp_to_kv(tp):
        key = f'{tp.team_basketball_reference_id} {format_player_name(tp.name)}'
        return (key, tp.as_dict())

    players_by_team_and_formatted_name = {
        k: v for k, v in map(tp_to_kv, players)
    }

    assert len(players_by_team_and_formatted_name.values()) == len(players)

    return players_by_team_and_formatted_name


def get_teams():
    return list(map(
        lambda r: r['basketball_reference_id'],
        services.sql.execute(
            'select distinct basketball_reference_id from teams').fetchall()
    ))


def get_positions():
    return list(map(
        lambda r: r['position'],
        services.sql.execute('select distinct position from teams_players where position is not null').fetchall()
    ))


def parse_json_field(the_json):
    if the_json is None:
        return None

    if isinstance(the_json, list) or isinstance(the_json, dict):
        return the_json

    return json.loads(the_json)
    

def last_games_transform(num, encode_fn, last_games):
    parsed_last_games = parse_json_field(last_games)
    if parsed_last_games is None or len(parsed_last_games) == 0:
        return [0.1 for i in range(num)]

    real_last_games = [stat for stat in parsed_last_games if stat is not None]
    mean = 0 if len(real_last_games) == 0 else statistics.mean(real_last_games)

    encoded_last_games = [None] * num
    for i in range(num):
        if i > len(real_last_games) - 1:
            encoded_last_games[i] = encode_fn(mean)
            continue

        encoded_last_games[i] = encode_fn(real_last_games[i])

    return np.array(encoded_last_games, dtype=np.float32)


def avg_last_five_over_avg_all(last_games):
    parsed_last_games = parse_json_field(last_games)
    non_null_last_games = [d for d in (parsed_last_games or []) if d is not None]

    if non_null_last_games is None or len(non_null_last_games) == 0:
        return 0

    mean_all = statistics.mean(non_null_last_games)
    mean_l5 = statistics.mean(non_null_last_games[0:5])
    return mean_l5 / mean_all


def parse_game_date(time_of_game):
    return datetime.datetime.strptime(time_of_game, "%Y-%m-%d %H:%M:%S")


def days_since_last_game(dts, game_date, enc_f):
    parsed_dts = parse_json_field(dts)
    if parsed_dts is None or len(parsed_dts) == 0:
        # TODO Replace w/ actual avg of days since
        return enc_f(30)
    
    parsed_game_date = parse_game_date(game_date)
    differences = [(parsed_game_date - parse_game_date(dt)).days for dt in parsed_dts if dt is not None]

    if len(differences) == 0:
        # TODO Replace w/ actual avg of days since
        return enc_f(30)

    return enc_f(differences[0])


def sd_last_five_games(ds):
    parsed_ds = parse_json_field(ds)
    parsed_ds = [d for d in (parsed_ds or []) if d is not None]
    if len(parsed_ds) < 2:
        return 0

    return statistics.stdev([d for d in parsed_ds if d is not None][0:5])


def z_score_last_games(num, all_last_games, last_games):
    parsed_all_last_games = parse_json_field(all_last_games)
    parsed_all_last_games = [lg for lg in (parsed_all_last_games or []) if lg is not None]
    if parsed_all_last_games is None or len(parsed_all_last_games) < 2:
        return [0 for i in range(num)]

    parsed_last_games = parse_json_field(last_games)
    parsed_last_games = [lg for lg in (parsed_last_games or []) if lg is not None]
    if parsed_last_games is None or len(parsed_last_games) < 2:
        return [0 for i in range(num)]

    all_mean = statistics.mean(parsed_all_last_games)
    all_stdev = statistics.stdev(parsed_all_last_games, all_mean)

    def encode_fn(val):
        z_score = (val - all_mean) / (all_stdev or 1)
        return 0.1 + ((2 + z_score) / 4)

    mean = 0 if len(parsed_last_games) == 0 else statistics.mean(parsed_last_games)

    encoded_last_games = [None] * num
    for i in range(num):
        if i > len(parsed_last_games) - 1:
            encoded_last_games[i] = encode_fn(mean)
            continue
 
        encoded_last_games[i] = encode_fn(parsed_last_games[i])

    return np.array(encoded_last_games, dtype=np.float32)


class Mappers:
    def __init__(self):
        self.stats = get_stats()

        self.age_enc = MeanMinMaxEncoder(
            self.stats['age_at_time_of_game_avg'],
            self.stats['age_at_time_of_game_min'],
            self.stats['age_at_time_of_game_max']
        )

        self.experience_enc = MeanMinMaxEncoder(
            self.stats['experience_avg'],
            self.stats['experience_min'],
            self.stats['experience_max']
        )

        self.height_inches_enc = MeanMinMaxEncoder(
            self.stats['height_inches_avg'],
            self.stats['height_inches_min'],
            self.stats['height_inches_max']
        )

        self.weight_lbs_enc = MeanMinMaxEncoder(
            self.stats['weight_lbs_avg'],
            self.stats['weight_lbs_min'],
            self.stats['weight_lbs_max']
        )

        self.year_of_game_enc = MeanMinMaxEncoder(
            self.stats['year_of_game_avg'],
            self.stats['year_of_game_min'],
            self.stats['year_of_game_max']
        )

        self.month_of_game_enc = MeanMinMaxEncoder(
            self.stats['month_of_game_avg'],
            self.stats['month_of_game_min'],
            self.stats['month_of_game_max']
        )

        self.day_of_game_enc = SineEncoder(
            1,
            31
        )

        self.seconds_played_enc = MeanMinMaxEncoder(
            self.stats['seconds_played_avg'],
            self.stats['seconds_played_min'],
            self.stats['seconds_played_max']
        )

        self.dk_fantasy_points_per_minute_enc = MeanMinMaxEncoder(
            self.stats['dk_fantasy_points_per_minute_avg'],
            self.stats['dk_fantasy_points_per_minute_min'],
            self.stats['dk_fantasy_points_per_minute_max']
        )

        self.dk_fantasy_points_enc = MeanMinMaxEncoder(
            self.stats['dk_fantasy_points_avg'],
            self.stats['dk_fantasy_points_min'],
            self.stats['dk_fantasy_points_max']
        )

        self.dk_fantasy_points_allowed_vs_position_enc = MeanMinMaxEncoder(
            self.stats['dk_fantasy_points_allowed_vs_position_avg'],
            self.stats['dk_fantasy_points_allowed_vs_position_min'],
            self.stats['dk_fantasy_points_allowed_vs_position_max']
        )

        # June 16, 2019 - October 16, 2018 = 243 days
        self.days_since_last_game_against_opp_enc = MeanMinMaxEncoder(
            0,
            1,
            243
        )

        self.teams_enc = LabelBinarizer()
        self.teams_enc.fit(get_teams())
        print(self.teams_enc.classes_)

        self.players = get_players()
        self.players_enc = MultiLabelBinarizer()
        self.players_enc.fit([self.players])

        self.positions_enc = LabelBinarizer()
        self.positions_enc.fit(get_positions())
        print(self.positions_enc.classes_)


    def datum_to_x(self, d):
        return np.concatenate((
            np.array([
                self.age_enc.transform(d['age_at_time_of_game']),
                self.height_inches_enc.transform(d['height_inches']),
                self.weight_lbs_enc.transform(d['weight_lbs']),
                self.experience_enc.transform(d['experience']),
                0.9 if d['playing_at_home'] else 0.1,
                0.9 if d['starter'] else 0.1,
                self.year_of_game_enc.transform(d['year_of_game']),
                self.month_of_game_enc.transform(d['month_of_game']),
                self.day_of_game_enc.transform(d['day_of_game'])
            ], dtype=np.float32),
            
            last_games_transform(
                5, self.dk_fantasy_points_enc.transform, d['dk_fantasy_points_last_games']),
            last_games_transform(
                2, self.dk_fantasy_points_enc.transform, d['dk_fantasy_points_last_games_against_opp_away']),
            last_games_transform(
                2, self.dk_fantasy_points_enc.transform, d['dk_fantasy_points_last_games_against_opp_home']),

            last_games_transform(
                5, self.seconds_played_enc.transform, d['seconds_played_last_games']),
            last_games_transform(
                2, self.seconds_played_enc.transform, d['seconds_played_last_games_against_opp_away']),
            last_games_transform(
                2, self.seconds_played_enc.transform, d['seconds_played_last_games_against_opp_home']),

            last_games_transform(
                5, self.dk_fantasy_points_per_minute_enc.transform, d['dk_fantasy_points_per_minute_last_games']),
            last_games_transform(
                2, self.dk_fantasy_points_per_minute_enc.transform, d['dk_fantasy_points_per_minute_last_games_against_opp_away']),
            last_games_transform(
                2, self.dk_fantasy_points_per_minute_enc.transform, d['dk_fantasy_points_per_minute_last_games_against_opp_home']),

            last_games_transform(
                5, self.seconds_played_enc.transform, d['opp_dk_fantasy_points_allowed_vs_position_last_games']),
            last_games_transform(
                2, self.seconds_played_enc.transform, d['opp_dk_fantasy_points_allowed_vs_position_last_games_away']),
            last_games_transform(
                2, self.seconds_played_enc.transform, d['opp_dk_fantasy_points_allowed_vs_position_last_games_home']),

            # NOTE pos_label and neg_label in sklearn not working
            [0.1 if d == 0 else 0.9 for d in self.teams_enc.transform([d['player_team_basketball_reference_id']]).flatten()],
            [0.1 if d == 0 else 0.9 for d in self.teams_enc.transform([d['opposing_team_basketball_reference_id']]).flatten()],

            [0.1 if d == 0 else 0.9 for d in self.positions_enc.transform([d['position']]).flatten()],

            # Odd features

            np.array([
                avg_last_five_over_avg_all(d['seconds_played_last_games']) or self.seconds_played_enc.transform(self.stats['seconds_played_avg']),
                days_since_last_game(d['times_of_last_games_against_opp_away'], d['time_of_game'], self.days_since_last_game_against_opp_enc.transform),
                days_since_last_game(d['times_of_last_games_against_opp_home'], d['time_of_game'], self.days_since_last_game_against_opp_enc.transform),
                self.dk_fantasy_points_enc.transform(sd_last_five_games(d['dk_fantasy_points_last_games']))
            ]),

            z_score_last_games(5, d['dk_fantasy_points_last_games'], d['dk_fantasy_points_last_games']),
            z_score_last_games(5, d['opp_dk_fantasy_points_allowed_vs_position_last_games'], d['opp_dk_fantasy_points_allowed_vs_position_last_games']),

            # self.players_enc.transform([set(player_team_starters)]).flatten(),
            # self.players_enc.transform([set(player_team_starters)]).flatten(),
            # [0.1 if d == 0 else 0.9 for d in self.players_enc.transform([[d['player_basketball_reference_id']]]).flatten()]
        )).astype(np.float32)

    def datum_to_y(self, d):
        return [d['dk_fantasy_points'] or 0]

    def datum_to_sw(self, d):
        FIRST_SEASON = 1984
        LATEST_SEASON = 2019
        SCALE = 10
        ADD_IF_WITHIN_TWO_WEEKS = 10

        time_of_game = parse_game_date(d['time_of_game'])
        
        season = time_of_game.year
        if time_of_game.month > 7:
            season += 1

        through_all = (LATEST_SEASON - season) / (LATEST_SEASON - FIRST_SEASON)
        through_all *= SCALE
        
        # TODO Get actual end of season
        start_of_season = datetime.datetime(season - 1, 10, 15)
        end_of_season = datetime.datetime(season, 7, 1)

        through_season = (time_of_game - start_of_season).days / (end_of_season - start_of_season).days

        val = through_all + through_season

        if (datetime.datetime.now() - time_of_game).days <= 14:
            val += ADD_IF_WITHIN_TWO_WEEKS

        return val

    def y_to_datum(self, y):
        return {'dk_fantasy_points': round(y[0], 2)}


@functools.lru_cache()
def make_mappers():
    return Mappers()


# Got from https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_mapped_data_for_player(player_basketball_reference_id):
    return get_mapped_data(player_basketball_reference_id=player_basketball_reference_id)


def cache_player_data():
    print('Caching player data')
    players = get_players()
    p = multiprocessing.Pool(int(multiprocessing.cpu_count() / 2) - 1)
    p.map(get_mapped_data_for_player, players)


memory = Memory(location='./tmp', verbose=1)
get_data = memory.cache(get_data)
get_stats = memory.cache(get_stats)
if os.environ.get('DEBUG') != '1':
    get_mapped_data = memory.cache(get_mapped_data)
get_players = memory.cache(get_players)
cache_player_data = memory.cache(cache_player_data)


if __name__ == '__main__':
    arg = sys.argv[1]
    if arg == 'cache-data':
        cache_data()
    elif arg == 'cache-features':
        cache_features()
    elif arg == 'get-mapped-data-debug':
        mapped_data = get_mapped_data(limit=10, player_basketball_reference_id='hardeja01')
        pprint.pprint(mapped_data['og'][9])
        pprint.pprint(mapped_data['x'][9])
    else:
        print(f'Argument not recognized: {arg}')

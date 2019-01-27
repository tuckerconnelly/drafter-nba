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
        return float((raw_value - self.mean) / (self.max - self.min)) * self.output_range + 1 + self.output_min

    def inverse_transform(self, encoded_value):
        without_range = (encoded_value - 1 - self.output_min) / self.output_range
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

    p = multiprocessing.Pool(4)
    p.map(cache_data_for_season, seasons)


# Load Data ##


def get_data_from_pg(
    limit=None,
    offset=0,
    player_basketball_reference_id=None,
    game_basketball_reference_id=None
):
    limit_sql = 'null'
    if (limit is not None):
        limit_sql = limit

    player_sql = ''
    if player_basketball_reference_id is not None:
        player_sql = f"and gp.player_basketball_reference_id = '{player_basketball_reference_id}'"

    game_sql = ''
    if game_basketball_reference_id is not None:
        game_sql = f"and gp.game_basketball_reference_id = '{game_basketball_reference_id}'"

    games_players = services.sqr.query(
        f"""
                select
                  gp.game_basketball_reference_id,
                  p.name as player_name,
                  t.name as team_name,
                  g.season as season,
                  g.time_of_game as time_of_game,

                  gp.player_basketball_reference_id, -- enum
                  p.birth_country, -- enum
                  floor(date_part('days', g.time_of_game - p.date_of_birth) / 365) as age_at_time_of_game,
                  date_part('year', g.time_of_game) as year_of_game,
                  date_part('month', g.time_of_game) as month_of_game,
                  date_part('day', g.time_of_game) as day_of_game,
                  date_part('hour', g.time_of_game) as hour_of_game,
                  (case (g.home_team_basketball_reference_id = t.basketball_reference_id) when true then g.away_team_basketball_reference_id else g.home_team_basketball_reference_id end) as opposing_team_basketball_reference_id, --enum
                  tp.team_basketball_reference_id as player_team_basketball_reference_id, -- enum
                  tp.experience,
                  tp.position, -- enum
                  (g.home_team_basketball_reference_id = t.basketball_reference_id) as playing_at_home,
                  gp.seconds_played,
                  gp.starter,
                  gpc.dk_fantasy_points,
                  gpc.seconds_played_last_games,
                  gpc.dk_fantasy_points_last_games,
                  gc.away_starters,
                  gc.home_starters,
                  gc.away_losses,
                  gc.away_wins,
                  gc.home_wins,
                  gc.home_losses,
                  gc.away_dk_fantasy_points_allowed_last_games,
                  gc.home_dk_fantasy_points_allowed_last_games,

                  gpc.dk_fantasy_points
                from games_players as gp
                inner join players as p
                  on p.basketball_reference_id = gp.player_basketball_reference_id
                inner join games as g
                  on g.basketball_reference_id = gp.game_basketball_reference_id
                inner join teams_players as tp
                  on tp.player_basketball_reference_id = gp.player_basketball_reference_id
                  and tp.currently_on_this_team = true
                inner join teams as t
                  on t.basketball_reference_id = tp.team_basketball_reference_id
                left join games_players_computed as gpc
                  on gpc.game_basketball_reference_id = gp.game_basketball_reference_id
                  and gpc.player_basketball_reference_id = gp.player_basketball_reference_id
                left join games_computed as gc
                  on gc.game_basketball_reference_id = g.basketball_reference_id
                where 1
                and gp.seconds_played > :min_seconds_played_in_game
                {player_sql}
                {game_sql}
                limit {limit_sql}
                offset {offset}
        """,
        min_seconds_played_in_game=MIN_SECONDS_PLAYED_IN_GAME
    ).all(as_dict=True)

    print(f"Total games_players: {len(games_players)}")

    valid_season_players = get_valid_season_players(c)
    games_players = filter(
        lambda gp: valid_season_players[f"{gp['season']}_{gp['player_basketball_reference_id']}"], games_players)

    print(f"Valid games_players: {len(games_players)}")

    return games_players


def get_mapped_data(
    limit=None,
    offset=0,
    player_basketball_reference_id=None,
    game_basketball_reference_id=None
):
    mappers = make_mappers()

    data = get_data_from_pg(
        limit=limit,
        offset=offset,
        player_basketball_reference_id=player_basketball_reference_id,
        game_basketball_reference_id=game_basketball_reference_id
    )

    if len(data) == 0:
        return {'x': [], 'y': [], 'sw': []}

    if len(data) < 100:
        return {
            'x': np.stack([mappers.datum_to_x(d) for d in data]),
            'y': np.stack([mappers.datum_to_y(d) for d in data]).flatten(),
            'sw': np.stack([mappers.datum_to_sw(d) for d in data])
        }

    random.Random(0).shuffle(data)

    p = multiprocessing.Pool(multiprocessing.cpu_count() - 1)

    return_val = {
        'x': np.stack(p.map(mappers.datum_to_x, data)),
        'y': np.stack(p.map(mappers.datum_to_y, data)).flatten(),
        'sw': np.stack(p.map(mappers.datum_to_sw, data))
    }

    p.close()
    p.join()

    return return_val


def get_game_stats():
    return services.sqr.query(
        """
            select
              avg(gc.away_dk_fantasy_points_allowed) as away_dk_fantasy_points_allowed_avg,
              min(gc.away_dk_fantasy_points_allowed) as away_dk_fantasy_points_allowed_min,
              max(gc.away_dk_fantasy_points_allowed) as away_dk_fantasy_points_allowed_max,

              avg(gc.home_dk_fantasy_points_allowed) as home_dk_fantasy_points_allowed_avg,
              min(gc.home_dk_fantasy_points_allowed) as home_dk_fantasy_points_allowed_min,
              max(gc.home_dk_fantasy_points_allowed) as home_dk_fantasy_points_allowed_max
            from games_computed as gc
        """
    ).first().as_dict()


def get_stats():
    return services.sqr.query(
        """
            select
              min(g.time_of_game) as time_of_first_game,
              max(g.time_of_game) as time_of_most_recent_game,

              avg(floor(date_part('days', g.time_of_game - p.date_of_birth) / 365)) as age_at_time_of_game_avg,
              min(floor(date_part('days', g.time_of_game - p.date_of_birth) / 365)) as age_at_time_of_game_min,
              max(floor(date_part('days', g.time_of_game - p.date_of_birth) / 365)) as age_at_time_of_game_max,

              avg(tp.experience) as experience_avg,
              min(tp.experience) as experience_min,
              max(tp.experience) as experience_max,

              avg(tp.height_inches) as height_inches_avg,
              min(tp.height_inches) as height_inches_min,
              max(tp.height_inches) as height_inches_max,

              avg(tp.weight_lbs) as weight_lbs_avg,
              min(tp.weight_lbs) as weight_lbs_avg,
              max(tp.weight_lbs) as weight_lbs_max,

              avg(date_part('year', g.time_of_game)) as year_of_game_avg,
              min(date_part('year', g.time_of_game)) as year_of_game_min,
              max(date_part('year', g.time_of_game)) as year_of_game_max,

              avg(date_part('month', g.time_of_game)) as month_of_game_avg,
              min(date_part('month', g.time_of_game)) as month_of_game_min,
              max(date_part('month', g.time_of_game)) as month_of_game_max,

              avg(date_part('day', g.time_of_game)) as day_of_game_avg,
              min(date_part('day', g.time_of_game)) as day_of_game_min,
              max(date_part('day', g.time_of_game)) as day_of_game_max,

              avg(gp.seconds_played) as seconds_played_avg,
              min(gp.seconds_played) as seconds_played_min,
              max(gp.seconds_played) as seconds_played_max,


              avg(gpc.dk_fantasy_points_per_minute) as dk_fantasy_points_per_minute_avg,
              min(gpc.dk_fantasy_points_per_minute) as dk_fantasy_points_per_minute_min,
              max(gpc.dk_fantasy_points_per_minute) as dk_fantasy_points_per_minute_max,

              avg((gpc.opp_dk_fantasy_points_allowed_vs_position_last_games->>0)::real) as fantasy_points_allowed_vs_position_avg,
              min((gpc.opp_dk_fantasy_points_allowed_vs_position_last_games->>0)::real) as fantasy_points_allowed_vs_position_min,
              max((gpc.opp_dk_fantasy_points_allowed_vs_position_last_games->>0)::real) as fantasy_points_allowed_vs_position_max,

              avg(gpc.dk_fantasy_points) as dk_fantasy_points_avg,
              min(gpc.dk_fantasy_points) as dk_fantasy_points_min,
              max(gpc.dk_fantasy_points) as dk_fantasy_points_max

            from games_players as gp
            inner join players as p
              on p.basketball_reference_id = gp.player_basketball_reference_id
            inner join games as g
              on g.basketball_reference_id = gp.game_basketball_reference_id
            inner join teams_players as tp
              on tp.player_basketball_reference_id = gp.player_basketball_reference_id
              and tp.season = g.season
            inner join teams as t
              on t.basketball_reference_id = tp.team_basketball_reference_id
            inner join games_players_computed as gpc
              on gpc.game_basketball_reference_id = gp.game_basketball_reference_id
              and gpc.player_basketball_reference_id = gp.player_basketball_reference_id
            where true
            and gp.seconds_played > :min_seconds_played_in_game
        """,
        min_seconds_played_in_game=MIN_SECONDS_PLAYED_IN_GAME
    ).first().as_dict()


def get_players():
    valid_season_players = get_valid_season_players(c)

    rows = services.sqr.query(
        '''
          select basketball_reference_id
          from players as p
          inner join teams_players as tp
            on tp.player_basketball_reference_id = p.basketball_reference_id
          where tp.season = 2019;
        '''
    ).all(as_dict=True)
    rows = filter(
        lambda r: valid_season_players[f"2019_{r['basketball_reference_id']}"],
        rows
    )
    return set(map(lambda r: r.basketball_reference_id, rows))


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
    players = services.sqr.query(
        """
            select p.*, tp.team_basketball_reference_id
            from players as p
            inner join teams_players as tp on tp.player_basketball_reference_id = p.basketball_reference_id
            where currently_on_this_team = true
        """
    ).all()

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
        lambda r: r.basketball_reference_id,
        services.sqr.query(
            'select distinct basketball_reference_id from teams').all()
    ))


def get_positions():
    return list(map(
        lambda r: r.position,
        services.sqr.query('select distinct position from teams_players where position is not null').all()
    ))


def last_games_transform(num, encode_fn, last_games_stats):
    if last_games_stats is None:
        return [0 for i in range(num)]

    real_last_games = [stat for stat in last_games_stats if stat is not None]
    mean = 0 if len(real_last_games) == 0 else statistics.mean(real_last_games)

    encoded_last_games = []
    for i in range(num):
        if i > len(real_last_games) - 1:
            encoded_last_games[i] = encode_fn(mean)
            continue

        encoded_last_games[i] = encode_fn(real_last_games[i])

    return np.array(encoded_last_games, dtype=np.float32)


def avg_last_five_over_avg_all(lgs):
    if len(lgs) == 0:
        return None

    non_null_last_games = [d for d in lgs if d not None]
    mean_all = statistics.mean(non_null_last_games)
    mean_l5 = statistics.mean(non_null_last_games[0:5])
    return mean_l5 / mean_alle


def days_since_last_game(dts, game_date, enc_f):
    if len(dt) == 0:
        return enc_f(30)

    deltas = datetimes_to_deltas(dts)
    enc_f([(game_date - dt).days for dt in dts if dt not None][0])


def sd_last_five_games(ds):
    if len(ds) == 0:
        return 0
    statistics.stdev([d for d in ds if d is not None][0:5])


def z_score_last_games(num, all_last_games, last_games):
    if last_games is None:
        return [0 for i in range(num )]

    all_mean = statistics.mean(all_last_games)
    all_stdev = statistics.stdev(all_last_games, all_mean)

    def encode_fn(val):
        z_score = (val - all_mean) / all_stdev
        return 0.1 + ((2 + z_score) / 4)

    real_last_games = [stat for stat in last_games_stats if stat is not None]
    mean = 0 if len(real_last_games) == 0 else statistics.mean(real_last_games)

    encoded_last_games = []
    for i in range(num):
        if i > len(real_last_games) - 1:
            encoded_last_games[i] = encode_fn(mean)
            continue
 
        encoded_last_games[i] = encode_fn(real_last_games[i])

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
            self.stats['fantasy_points_per_minute_avg'],
            self.stats['fantasy_points_per_minute_min'],
            self.stats['fantasy_points_per_minute_max']
        )

        self.dk_fantasy_points_enc = MeanMinMaxEncoder(
            self.stats['dk_fantasy_points_avg'],
            self.stats['dk_fantasy_points_min'],
            self.stats['dk_fantasy_points_max']
        )

        self.fantasy_points_allowed_vs_position_enc = MeanMinMaxEncoder(
            self.stats['fantasy_points_allowed_vs_position_avg'],
            self.stats['fantasy_points_allowed_vs_position_min'],
            self.stats['fantasy_points_allowed_vs_position_max']
        )

        # June 16, 2019 - October 16, 2018 = 243 days
        self.days_since_last_game_against_opp_enc = MeanMinMaxEncoder(
            0,
            1,
            243
        )

        self.teams_enc = LabelBinarizer(neg_label=0.1, pos_label=0.9)
        self.teams_enc.fit(get_teams())

        self.players = get_players()
        self.players_enc = MultiLabelBinarizer(neg_label=0.1, pos_label=0.9)
        self.players_enc.fit([self.players])

        self.positions_enc = LabelBinarizer(neg_label=0.1, pos_label=0.9)
        self.positions_enc.fit(get_positions())


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

            self.teams_enc.transform(
                [d['player_team_basketball_reference_id']]).flatten(),
            self.teams_enc.transform(
                [d['opposing_team_basketball_reference_id']]).flatten(),

            self.positions_enc.transform([d['position']]).flatten(),

            # Odd features

            np.array([
                avg_last_five_over_avg_all(d['seconds_played_last_games']) or self.seconds_played_enc(self.stats['seconds_played_avg']),
                days_since_last_game(d['times_of_last_games_against_opp_away'], d['time_of_game'], self.days_since_last_game_against_opp_enc.transform),
                days_since_last_game(d['times_of_last_games_against_opp_home'], d['time_of_game'], self.days_since_last_game_against_opp_enc.transform),
                self.dk_fantasy_points_enc.transform(sd_last_five_games(d['dk_fantasy_points_last_games']))
            ]),

            z_score_last_games(5, d['dk_fantasy_points_last_games'], d['dk_fantasy_points_last_games']),
            z_score_last_games(5, d['opp_dk_fantasy_points_allowed_vs_position_last_games'], d['opp_dk_fantasy_points_allowed_vs_position_last_games'])

            # self.players_enc.transform([set(player_team_starters)]).flatten(),
            # self.players_enc.transform([set(player_team_starters)]).flatten(),
            # self.players_enc.transform([[d['player_basketball_reference_id']]]).flatten()
        ))

    def datum_to_y(self, d):
        return [d['dk_fantasy_points']]

    def datum_to_sw(self, d):
        FIRST_SEASON = 1984
        LATEST_SEASON = 2019
        SCALE = 10
        ADD_IF_WITHIN_TWO_WEEKS = 10
        
        season = d['time_of_game'].year
        if d['time_of_game'].month > 7:
            season += 1

        season = (LATEST_SEASON - season) / (LATEST_SEASON - FIRST_SEASON)
        season *= SCALE
        
        # TODO Get actual end of season
        start_of_season = datetime.datetime(season - 1, 10, 15)
        end_of_season = datetime.datetime(season, 7, 1)

        through_season = (d['time_of_game'] - start_of_season).days / (end_of_season - start_of_season).days

        val = season + through_season

        if (datetime.datetime.now() - d['time_of_game']).days <= 14:
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
    p = multiprocessing.Pool(int(multiprocessing.cpu_count() / 2))
    p.map(get_mapped_data_for_player, players)


memory = Memory(location='./tmp', verbose=1)
get_mapped_data = memory.cache(get_mapped_data)
get_players = memory.cache(get_players)
cache_player_data = memory.cache(cache_player_data)


if __name__ == '__main__':
    arg = sys.argv[1]
    if arg == 'cache-data':
        cache_data()
    else:
        print(f'Argument not recognized: {arg}')

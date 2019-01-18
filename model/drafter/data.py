"""
Module for getting data
"""

import statistics
import random
import functools
import logging
import time
import multiprocessing

from sklearn.preprocessing import LabelBinarizer, MultiLabelBinarizer
import numpy as np
from joblib import Memory

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
    def __init__(self, mean, min, max):
        assert mean is not None
        assert min is not None
        assert max is not None

        self.mean = mean
        self.min = min
        self.max = max

    def transform(self, raw_value):
        return float((raw_value - self.mean) / (self.max - self.min))

    def inverse_transform(self, encoded_value):
        return float((encoded_value * (self.max - self.min)) + self.mean)


# Cache Data #


def calculate_fantasy_score(stats):
    if not stats['seconds_played']:
        return 0

    return (
        stats['points']
        + stats['three_point_field_goals'] * 0.5
        + stats['total_rebounds'] * 1.25
        + stats['assists'] * 1.5
        + stats['steals'] * 2
        + stats['blocks'] * 2
        + stats['turnovers'] * -0.5
        + (1.5 if len([True for k in stats if stats[k] >= 10]) > 2 else 0)
        + (3 if len([True for k in stats if stats[k] >= 10]) > 3 else 0)
    )


def get_stats_last_games_from_pg(
    player_basketball_reference_id,
    season,
    current_game_date
):
    rows = services.pgr.query("""
        select
          gp.points,
          gp.three_point_field_goals,
          gp.total_rebounds,
          gp.assists,
          gp.blocks,
          gp.steals,
          gp.turnovers,
          gp.seconds_played
        from games g
        left join games_players gp
          on gp.game_basketball_reference_id = g.basketball_reference_id
          and gp.player_basketball_reference_id = :player_basketball_reference_id
        inner join teams_players tp
          on tp.player_basketball_reference_id = :player_basketball_reference_id
          and tp.season = g.season
        where g.season = :season
          and (
            g.home_team_basketball_reference_id = tp.team_basketball_reference_id
            or g.away_team_basketball_reference_id = tp.team_basketball_reference_id
          )
        and g.time_of_game < :current_game_date
        order by g.time_of_game desc
    """,
                              player_basketball_reference_id=player_basketball_reference_id,
                              season=season,
                              current_game_date=current_game_date)

    return {
        'points_last_games_last_games': list(map(lambda r: r.points, rows)),
        'three_point_field_goals_last_games': list(map(lambda r: r.three_point_field_goals, rows)),
        'total_rebounds_last_games': list(map(lambda r: r.total_rebounds, rows)),
        'assists_last_games': list(map(lambda r: r.assists, rows)),
        'blocks_last_games': list(map(lambda r: r.blocks, rows)),
        'steals_last_games': list(map(lambda r: r.steals, rows)),
        'turnovers_last_games': list(map(lambda r: r.turnovers, rows)),
        'seconds_played_last_games': list(map(lambda r: r.seconds_played, rows)),
        'dk_fantasy_points_last_games': list(map(lambda r: calculate_fantasy_score(r.as_dict()), rows))
    }


def team_get_stats_last_games_from_pg(
    team_basketball_reference_id,
    season,
    current_game_date
):
    wins = len(services.pgr.query(
        """
            select id
            from games g
            where g.season = :season
            and (
              (g.away_team_basketball_reference_id = :team_basketball_reference_id and g.away_score > g.home_score)
              or (g.home_team_basketball_reference_id = :team_basketball_reference_id and g.home_score > g.away_score)
            )
            and g.time_of_game < :current_game_date
        """,
        team_basketball_reference_id=team_basketball_reference_id,
        season=season,
        current_game_date=current_game_date
    ).all())

    losses = len(services.pgr.query(
        """
            select id
            from games g
            where g.season = :season
            and (
              (g.away_team_basketball_reference_id = :team_basketball_reference_id and g.away_score < g.home_score)
              or (g.home_team_basketball_reference_id = :team_basketball_reference_id and g.home_score < g.away_score)
            )
            and g.time_of_game < :current_game_date
        """,
        team_basketball_reference_id=team_basketball_reference_id,
        season=season,
        current_game_date=current_game_date
    ).all())

    dk_fantasy_points_allowed_last_games = services.pgr.query(
        """
            select sum(gpc.dk_fantasy_points) as dk_fantasy_points_allowed
            from games_players_computed gpc
            inner join games g
              on g.basketball_reference_id = gpc.game_basketball_reference_id
              and (
                g.away_team_basketball_reference_id = :team_basketball_reference_id
                or g.home_team_basketball_reference_id = :team_basketball_reference_id
              )
              and g.season = :season
              and g.time_of_game < :current_game_date
            inner join teams_players tp
              on tp.player_basketball_reference_id = gpc.player_basketball_reference_id
              and tp.team_basketball_reference_id != :team_basketball_reference_id
              and tp.season = :season
            group by g.id
            order by g.time_of_game desc
        """,
        team_basketball_reference_id=team_basketball_reference_id,
        season=season,
        current_game_date=current_game_date
    )

    return {
        'wins': wins,
        'losses': losses,
        'dk_fantasy_points_allowed_last_games': list(map(lambda r: r.dk_fantasy_points_allowed, dk_fantasy_points_allowed_last_games))
    }


def cache_single_games_players(games_player):
    assert games_player['player_basketball_reference_id']
    assert games_players['season']
    assert games_player['time_of_game']

    services.pgw.query(
        '''
            delete from games_players_computed
            where game_basketball_reference_id = :game_basketball_reference_id
            and player_basketball_reference_id = :player_basketball_reference_id
        ''',
        game_basketball_reference_id=games_players['game_basketball_reference_id'],
        player_basketball_reference_id=games_players['player_basketball_reference_id']
    )

    stats_last_games = get_stats_last_games_from_pg(
        player_basketball_reference_id=games_player['player_basketball_reference_id'],
        season=games_player['season'],
        current_game_date=games_player['time_of_game']
    )

    services.pgw.query(
        '''
            insert into games_players_computed (
                game_basketball_reference_id,
                player_basketball_reference_id,
                dk_fantasy_points,
                dk_fantasy_points_last_games,
                seconds_played_last_games
            )
            values (
                :game_basketball_reference_id,
                :player_basketball_reference_id,
                :dk_fantasy_points,
                :dk_fantasy_points_last_games,
                :seconds_played_last_games
            )
        ''',
        game_basketball_reference_id=games_players['game_basketball_reference_id'],
        player_basketball_reference_id=games_players['player_basketball_reference_id'],
        dk_fantasy_points=calculate_fantasy_score(games_player),
        dk_fantasy_points_last_games=stats_last_games['dk_fantasy_points_last_games'],
        seconds_played_last_games=stats_last_games['seconds_played_last_games']
    )


def cache_single_game():
    assert game['basketball_reference_id']
    assert game['season']
    assert game['time_of_game']
    assert game['away_team_basketball_reference_id']
    assert game['home_team_basketball_reference_id']

    away_starters = services.pgr.query(
        '''
            select gp.player_basketball_reference_id
            from games_players gp
            inner join teams_players tp
              on tp.player_basketball_reference_id = gp.player_basketball_reference_id
              and tp.season = :season
            where gp.game_basketball_reference_id = :basketball_reference_id
            and gp.starter = true
            and tp.team_basketball_reference_id = :away_team_basketball_reference_id
        ''',
        season=game['season'],
        basketball_reference_id=game['basketball_reference_id'],
        away_team_basketball_reference_id=game['away_team_basketball_reference_id']
    ).all(as_dict=True)
    away_starters = map(lambda gp: gp['player_basketball_reference_id'], away_starters)

    assert len(away_starters) == 5

    home_starters = services.pgr.query(
        '''
            select gp.player_basketball_reference_id
            from games_players gp
            inner join teams_players tp
              on tp.player_basketball_reference_id = gp.player_basketball_reference_id
              and tp.season = :season
            where gp.game_basketball_reference_id = :basketball_reference_id
            and gp.starter = true
            and tp.team_basketball_reference_id = :home_team_basketball_reference_id
        ''',
        season=game['season'],
        basketball_reference_id=game['basketball_reference_id'],
        home_team_basketball_reference_id=game['home_team_basketball_reference_id']
    ).all(as_dict=True)
    home_starters = map(lambda gp: gp['player_basketball_reference_id'], home_starters)

    assert len(home_starters) == 5

    away_dk_fantasy_points_allowed = services.pgr.query(
        '''
          select sum(gpc.dk_fantasy_points) as dk_fantasy_points_allowed
          from games_players_computed gpc
          inner join games g
            on g.basketball_reference_id = gpc.game_basketball_reference_id
          inner join teams_players tp
            on tp.player_basketball_reference_id = gpc.player_basketball_reference_id
            and tp.team_basketball_reference_id != g.away_team_basketball_reference_id
            and tp.season = g.season
          where gpc.game_basketball_reference_id = :game_basketball_reference_id
          group by g.id
        ''',
        game_basketball_reference_id=game['basketball_reference_id']
    ).all(as_dict=True)
    away_dk_fantasy_points_allowed = map(lambda gpc: gpc['dk_fantasy_points_allowed'], away_dk_fantasy_points_allowed)

    home_dk_fantasy_points_allowed = services.pgr.query(
        '''
          select sum(gpc.dk_fantasy_points) as dk_fantasy_points_allowed
          from games_players_computed gpc
          inner join games g
            on g.basketball_reference_id = gpc.game_basketball_reference_id
          inner join teams_players tp
            on tp.player_basketball_reference_id = gpc.player_basketball_reference_id
            and tp.team_basketball_reference_id != g.away_team_basketball_reference_id
            and tp.season = g.season
          where gpc.game_basketball_reference_id = :game_basketball_reference_id
          group by g.id
        ''',
        game_basketball_reference_id=game['basketball_reference_id']
    ).all(as_dict=True)
    home_dk_fantasy_points_allowed = map(lambda gpc: gpc['dk_fantasy_points_allowed'], home_dk_fantasy_points_allowed)

    away_team_stats_last_games = team_get_stats_last_games_from_pg(
        team_basketball_reference_id=game['away_team_basketball_reference_id'],
        season=game['season'],
        current_game_date=game['time_of_game']
    )

    home_team_stats_last_games = team_get_stats_last_games_from_pg(
        team_basketball_reference_id=game['home_team_basketball_reference_id'],
        season=game['season'],
        current_game_date=game['time_of_game']
    )

    services.pgw.query(
        '''
            delete from game_computed
            where game_basketball_reference_id = :game_basketball_reference_id
        ''',
        game_basketball_reference_id=game['basketball_reference_id']
    )

    services.pgw.query(
        '''
            insert into games_computed (
                game_basketball_reference_id,
                away_starters,
                home_starters,
                away_wins,
                away_losses,
                home_wins,
                home_losses,
                away_dk_fantasy_points_allowed,
                home_dk_fantasy_points_allowed,
                away_dk_fantasy_points_allowed_last_games,
                home_dk_fantasy_points_allowed_last_games
            )
            values (
                :game_basketball_reference_id,
                :away_starters,
                :home_starters,
                :away_wins,
                :away_losses,
                :home_wins,
                :home_losses,
                :away_dk_fantasy_points_allowed,
                :home_dk_fantasy_points_allowed,
                :away_dk_fantasy_points_allowed_last_games,
                :home_dk_fantasy_points_allowed_last_games
            )
        ''',
        game_basketball_reference_id=game['basketball_reference_id'],
        away_starters=away_starters,
        home_starters=home_starters,
        away_wins=away_team_stats_last_games['wins'],
        away_losses=away_team_stats_last_games['losses'],
        home_wins=home_team_stats_last_games['wins'],
        home_losses=home_team_stats_last_games['losses'],
        away_dk_fantasy_points_allowed=away_dk_fantasy_points_allowed,
        home_dk_fantasy_points_allowed=home_dk_fantasy_points_allowed,
        away_dk_fantasy_points_allowed_last_games=away_team_stats_last_games['dk_fantasy_points_allowed_last_games'],
        home_dk_fantasy_points_allowed_last_games=home_team_stats_last_games['dk_fantasy_points_allowed_last_games']
    )


def cache_data():
    print('Caching games_players')

    games_players = services.pgr.query(
        '''
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

            g.season,
            g.time_of_game
          from games_players gp
          inner join games g
            on g.basketball_reference_id = gp.game_basketball_reference_id
          where not exists (
          	select
          	from games_players_computed gpc
          	where gpc.game_basketball_reference_id = gp.game_basketball_reference_id
          	and gpc.player_basketball_reference_id = gp.player_basketball_reference_id
          )
        '''
    ).all(as_dict=True)

    widgets = [
        ' [', progressbar.Timer(), '] ',
        progressbar.Bar(),
        ' (', progressbar.ETA(), ') '
    ]
    for i in progressbar.progressbar(range(len(games_players)), widgets=widgets):
        cache_single_games_players(games_players[i])

    print('Caching games')

    games = services.pgr.query(
        '''
          select
            season,
            basketball_reference_id,
            away_team_basketball_reference_id,
            home_team_basketball_reference_id,
            time_of_game
          from games
        '''
    ).all(as_dict=True)

    widgets = [
        ' [', progressbar.Timer(), '] ',
        progressbar.Bar(),
        ' (', progressbar.ETA(), ') '
    ]
    for i in progressbar.progressbar(range(len(games_players)), widgets=widgets):
        cache_single_game(games[i])


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

    return services.pgr.query(
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
                from games_players gp
                inner join players p
                  on p.basketball_reference_id = gp.player_basketball_reference_id
                inner join games g
                  on g.basketball_reference_id = gp.game_basketball_reference_id
                inner join teams_players tp
                  on tp.player_basketball_reference_id = gp.player_basketball_reference_id
                  and tp.currently_on_this_team = true
                inner join teams t
                  on t.basketball_reference_id = tp.team_basketball_reference_id
                left join games_players_computed gpc
                  on gpc.game_basketball_reference_id = gp.game_basketball_reference_id
                  and gpc.player_basketball_reference_id = gp.player_basketball_reference_id
                left join games_computed gc
                  on gc.game_basketball_reference_id = g.basketball_reference_id
                where true
                and gp.seconds_played > 0
                {player_sql}
                {game_sql}
                limit {limit_sql}
                offset {offset}
        """
    ).all(as_dict=True)


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
    return services.pgr.query(
        """
            select
              avg(gc.away_dk_fantasy_points_allowed) as away_dk_fantasy_points_allowed_avg,
              min(gc.away_dk_fantasy_points_allowed) as away_dk_fantasy_points_allowed_min,
              max(gc.away_dk_fantasy_points_allowed) as away_dk_fantasy_points_allowed_max,

              avg(gc.home_dk_fantasy_points_allowed) as home_dk_fantasy_points_allowed_avg,
              min(gc.home_dk_fantasy_points_allowed) as home_dk_fantasy_points_allowed_min,
              max(gc.home_dk_fantasy_points_allowed) as home_dk_fantasy_points_allowed_max
            from games_computed gc
        """
    ).first().as_dict()


def get_stats():
    return services.pgr.query(
        """
            select
              min(g.time_of_game) as time_of_first_game,
              max(g.time_of_game) as time_of_most_recent_game,

              avg(floor(date_part('days', g.time_of_game - p.date_of_birth) / 365)) as age_at_time_of_game_avg,
              min(floor(date_part('days', g.time_of_game - p.date_of_birth) / 365)) as age_at_time_of_game_min,
              max(floor(date_part('days', g.time_of_game - p.date_of_birth) / 365)) as age_at_time_of_game_max,

              avg(date_part('year', g.time_of_game)) as year_of_game_avg,
              min(date_part('year', g.time_of_game)) as year_of_game_min,
              max(date_part('year', g.time_of_game)) as year_of_game_max,

              avg(date_part('month', g.time_of_game)) as month_of_game_avg,
              min(date_part('month', g.time_of_game)) as month_of_game_min,
              max(date_part('month', g.time_of_game)) as month_of_game_max,

              avg(date_part('day', g.time_of_game)) as day_of_game_avg,
              min(date_part('day', g.time_of_game)) as day_of_game_min,
              max(date_part('day', g.time_of_game)) as day_of_game_max,

              avg(date_part('hour', g.time_of_game)) as hour_of_game_avg,
              min(date_part('hour', g.time_of_game)) as hour_of_game_min,
              max(date_part('hour', g.time_of_game)) as hour_of_game_max,

              avg(tp.experience) as experience_avg,
              min(tp.experience) as experience_min,
              max(tp.experience) as experience_max,

              avg(gp.seconds_played) as seconds_played_avg,
              min(gp.seconds_played) as seconds_played_min,
              max(gp.seconds_played) as seconds_played_max,


              avg(gpc.dk_fantasy_points) as dk_fantasy_points_avg,
              min(gpc.dk_fantasy_points) as dk_fantasy_points_min,
              max(gpc.dk_fantasy_points) as dk_fantasy_points_max

            from games_players gp
            inner join players p
              on p.basketball_reference_id = gp.player_basketball_reference_id
            inner join games g
              on g.basketball_reference_id = gp.game_basketball_reference_id
            inner join teams_players tp
              on tp.player_basketball_reference_id = gp.player_basketball_reference_id
              and tp.season = g.season
            inner join teams t
              on t.basketball_reference_id = tp.team_basketball_reference_id
            inner join games_players_computed gpc
              on gpc.game_basketball_reference_id = gp.game_basketball_reference_id
              and gpc.player_basketball_reference_id = gp.player_basketball_reference_id
            where true
            and gp.seconds_played > 0
        """
    ).first().as_dict()


def get_players():
    rows = services.pgr.query(
        """
          select basketball_reference_id
          from players p
          inner join teams_players tp on tp.player_basketball_reference_id = p.basketball_reference_id
          where currently_on_this_team = true;
        """
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
    players = services.pgr.query(
        """
            select p.*, tp.team_basketball_reference_id
            from players p
            inner join teams_players tp on tp.player_basketball_reference_id = p.basketball_reference_id
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
        services.pgr.query(
            'select distinct basketball_reference_id from teams').all()
    ))


def get_positions():
    return list(map(
        lambda r: r.position,
        services.pgr.query('select distinct position from teams_players').all()
    ))


def standardized_average_of_normal_last_games(num, last_games):
    non_null_last_games = [game for game in last_games if game is not None]

    if len(non_null_last_games) < 2:
        return 0

    mean = statistics.mean(non_null_last_games)
    stdev = statistics.stdev(non_null_last_games, mean)
    lower_fence = (mean - 1.5 * stdev) or 0

    if (stdev == 0):
        return mean

    last_normal_games = [
        game for game in non_null_last_games if game >= lower_fence]
    last_normal_games = last_normal_games[0:num]

    if (len(last_normal_games) == 0):
        return 0
    if (len(last_normal_games) == 1):
        return last_normal_games[0]

    mean_of_normal_last_games = statistics.mean(last_normal_games)
    standard_mean_of_normal_last_games = (
        mean_of_normal_last_games - mean) / stdev
    return standard_mean_of_normal_last_games or 0


def last_games_transform(num, encode_fn, last_games_stats):
    if last_games_stats is None:
        return [0 for i in range(num)]

    real_last_games = [stat for stat in last_games_stats if stat is not None]
    mean = 0 if len(real_last_games) < 2 else statistics.mean(real_last_games)
    stdev = 0 if len(real_last_games) < 2 else statistics.stdev(
        real_last_games, mean)
    lower_fence = mean - 1.5 * stdev

    last_normal_games = [
        game for game in real_last_games if game >= lower_fence]
    last_normal_games_average = 0 if len(
        last_normal_games) < 2 else statistics.mean(last_normal_games[0:num])
    encoded_last_normal_games = [None for i in range(num)]

    for i in range(num):
        if i > len(last_normal_games) - 1:
            encoded_last_normal_games[i] = encode_fn(last_normal_games_average)
            continue

        encoded_last_normal_games[i] = encode_fn(last_normal_games[i])

    return np.array(encoded_last_normal_games, dtype=np.float32)


def win_loss_ratio(wins, losses):
    if (wins + losses == 0):
        return 0
    return wins / (wins + losses)


class Mappers:
    def __init__(self):
        self.stats = get_stats()

        self.age_enc = MeanMinMaxEncoder(
            self.stats['age_at_time_of_game_avg'],
            self.stats['age_at_time_of_game_min'],
            self.stats['age_at_time_of_game_max']
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

        self.day_of_game_enc = MeanMinMaxEncoder(
            self.stats['day_of_game_avg'],
            self.stats['day_of_game_min'],
            self.stats['day_of_game_max']
        )

        self.hour_of_game_enc = MeanMinMaxEncoder(
            self.stats['hour_of_game_avg'],
            self.stats['hour_of_game_min'],
            self.stats['hour_of_game_max']
        )

        self.experience_enc = MeanMinMaxEncoder(
            self.stats['experience_avg'],
            self.stats['experience_min'],
            self.stats['experience_max']
        )

        self.experience_enc = MeanMinMaxEncoder(
            self.stats['experience_avg'],
            self.stats['experience_min'],
            self.stats['experience_max']
        )

        self.dk_fantasy_points_enc = MeanMinMaxEncoder(
            self.stats['dk_fantasy_points_avg'],
            self.stats['dk_fantasy_points_min'],
            self.stats['dk_fantasy_points_max']
        )

        self.teams_enc = LabelBinarizer()
        self.teams_enc.fit(get_teams())

        self.players = get_players()
        self.players_enc = MultiLabelBinarizer()
        self.players_enc.fit([self.players])

        self.positions_enc = LabelBinarizer()
        self.positions_enc.fit(get_positions())

    def datum_to_x(self, d):
        if d['playing_at_home']:
            player_team_wins = d['home_wins']
            opposing_team_wins = d['away_wins']

            player_team_losses = d['home_losses']
            opposing_team_losses = d['away_losses']

            player_team_starters = d['home_starters']
            opposing_team_starters = d['away_starters']

            player_team_fantasy_points_allowed_last_games = d[
                'home_dk_fantasy_points_allowed_last_games']
            opposing_team_fantasy_points_allowed_last_games = d[
                'away_dk_fantasy_points_allowed_last_games']
        else:
            player_team_wins = d['away_wins']
            opposing_team_wins = d['home_wins']

            player_team_losses = d['away_losses']
            opposing_team_losses = d['home_losses']

            player_team_starters = d['away_starters']
            opposing_team_starters = d['home_starters']

            player_team_fantasy_points_allowed_last_games = d[
                'away_dk_fantasy_points_allowed_last_games']
            opposing_team_fantasy_points_allowed_last_games = d[
                'home_dk_fantasy_points_allowed_last_games']

        player_team_starters = [
            p for p in player_team_starters if p in self.players]
        opposing_team_starters = [
            p for p in opposing_team_starters if p in self.players]

        return np.concatenate((
            np.array([
                self.age_enc.transform(d['age_at_time_of_game']),
                self.year_of_game_enc.transform(d['year_of_game']),
                self.month_of_game_enc.transform(d['month_of_game']),
                self.day_of_game_enc.transform(d['day_of_game']),
                self.hour_of_game_enc.transform(d['hour_of_game']),
                self.experience_enc.transform(d['experience']),
                1 if d['playing_at_home'] else 0,
                1 if d['starter'] else 0,
                win_loss_ratio(player_team_wins, player_team_losses),
                win_loss_ratio(opposing_team_wins, opposing_team_losses),
                standardized_average_of_normal_last_games(
                    5, d['dk_fantasy_points_last_games']),
                standardized_average_of_normal_last_games(
                    5, player_team_fantasy_points_allowed_last_games),
                standardized_average_of_normal_last_games(
                    5, opposing_team_fantasy_points_allowed_last_games)
            ], dtype=np.float32),
            last_games_transform(
                5, self.dk_fantasy_points_enc.transform, d['dk_fantasy_points_last_games']),
            self.teams_enc.transform(
                [d['player_team_basketball_reference_id']]).flatten(),
            self.teams_enc.transform(
                [d['opposing_team_basketball_reference_id']]).flatten(),
            self.positions_enc.transform([d['position']]).flatten(),
            self.players_enc.transform([set(player_team_starters)]).flatten(),
            self.players_enc.transform([set(player_team_starters)]).flatten(),
            last_games_transform(5, self.dk_fantasy_points_enc.transform,
                                 player_team_fantasy_points_allowed_last_games),
            last_games_transform(5, self.dk_fantasy_points_enc.transform,
                                 opposing_team_fantasy_points_allowed_last_games),
            self.players_enc.transform(
                [[d['player_basketball_reference_id']]]).flatten()
        ))

    def datum_to_y(self, d):
        return [d['dk_fantasy_points']]

    def datum_to_sw(self, d):
        value_through_range = d['time_of_game'].timestamp() - self.stats['time_of_first_game'].timestamp()
        the_range = self.stats['time_of_most_recent_game'].timestamp() - self.stats['time_of_first_game'].timestamp()
        x = value_through_range / the_range
        # https://www.desmos.com/calculator/irzszceutx
        return 1 + ((10 - 1) / (1 + 10 ** ((0.95 - x) * 15))) + x ** 2

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

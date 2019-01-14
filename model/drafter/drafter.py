import re
import datetime
import itertools

import scipy
import pandas as pd
import dateparser
import progressbar
from joblib import Memory
from terminaltables import AsciiTable

import services
import data
import model
import scraping


def embellish_salary_data(salary_df):
    parsed_players = []
    widgets = [
        ' [', progressbar.Timer(), '] ',
        progressbar.Bar(),
        ' (', progressbar.ETA(), ') '
    ]

    for i in progressbar.progressbar(range(len(salary_df)), widgets=widgets):
        row = salary_df.iloc[i]
        now = datetime.datetime.now()

        stats_last_games = data.get_stats_last_games_from_pg(
            player_basketball_reference_id=row['basketball_reference_id'],
            season=2019,
            current_game_date=now
        )

        away_stats_last_games = data.team_get_stats_last_games_from_pg(
            team_basketball_reference_id=row['Away Team'],
            season=2019,
            current_game_date=now
        )

        home_stats_last_games = data.team_get_stats_last_games_from_pg(
            team_basketball_reference_id=row['Home Team'],
            season=2019,
            current_game_date=now
        )

        pbtfn = data.get_players_by_team_and_formatted_name()
        lineups = scraping.get_lineups(datetime.datetime.now().date())
        team_lineup = lineups[row['Player Team']]
        away_starters = [pbtfn['{team} {name}'.format(
            team=d['team'], name=d['name'])]['basketball_reference_id'] for d in lineups[row['Away Team']]['starters']]
        home_starters = [pbtfn['{team} {name}'.format(
            team=d['team'], name=d['name'])]['basketball_reference_id'] for d in lineups[row['Home Team']]['starters']]

        parsed_player = {
            'name': row['Name'],
            'salary_dollars': row['Salary'],
            'avg_points_per_game': row['AvgPointsPerGame'],
            'roster_positions': row['Roster Position'],
            'player_basketball_reference_id': row['basketball_reference_id'],
            'player_team_basketball_reference_id': row['Player Team'],
            'opposing_team_basketball_reference_id':
            row['Home Team'] if row['Player Team'] == row['Away Team'] else row['Away Team'],
            'position': row['Position'][0],
            'age_at_time_of_game': int((row['Time of Game'].date() - row['date_of_birth']).days / 365),
            'year_of_game': row['Time of Game'].year,
            'month_of_game': row['Time of Game'].month,
            'day_of_game': row['Time of Game'].day,
            'hour_of_game': row['Time of Game'].hour,
            'experience': row['experience'],
            'playing_at_home': row['Player Team'] == row['Home Team'],

            **stats_last_games,

            'away_wins': away_stats_last_games['wins'],
            'away_losses': away_stats_last_games['losses'],
            'home_wins': home_stats_last_games['wins'],
            'home_losses': home_stats_last_games['losses'],
            'away_dk_fantasy_points_allowed_last_games':
            away_stats_last_games['dk_fantasy_points_allowed_last_games'],
            'home_dk_fantasy_points_allowed_last_games':
            home_stats_last_games['dk_fantasy_points_allowed_last_games'],

            'starter': len([d['name'] for d in team_lineup['starters']]) > 0,
            'injured': len([d['name'] for d in team_lineup['injured']]) > 0,
            'away_starters': away_starters,
            'home_starters': home_starters
        }
        parsed_players.append(parsed_player)

    return pd.DataFrame(parsed_players)


def filter_players(df):
    filtered_data = []
    for i, row in df.iterrows():
        if row['injured'] and not row['starter']:
            continue
        if sum(row['dk_fantasy_points_last_games'][0:5]) / 5 < 5:
            continue

        filtered_data.append(row)

    return pd.DataFrame(filtered_data)


'''
.then(data => data.filter(datum => !datum.injured || datum.starter))
.then(data =>
  data.filter(datum => {
    return (
      _.mean(datum.pointsLastGames.slice(0, 7).map(it => (it ? it : 0))) > 5
    );
  })
)
'''


def predict_with_player_models(df):
    model_name = model.get_latest_model_name()
    default_model, default_losses = model.load_model(model_name)

    predictions = []
    widgets = [
        ' [', progressbar.Timer(), '] ',
        progressbar.Bar(),
        ' (', progressbar.ETA(), ') '
    ]
    for i in progressbar.progressbar(range(len(df)), widgets=widgets):
        d = df.iloc[i]
        player_model = default_model
        player_losses = default_losses
        try:
            player_model, player_losses = model.load_model(
                model_name + '/' + d['player_basketball_reference_id'])
        except:
            pass

        prediction = model.predict(player_model, player_losses, [d.to_dict()])
        predictions.append(prediction[0])

    return pd.DataFrame(predictions)


def add_computed_columns(df):
    new_data = []
    for i, row in df.iterrows():
        conf = max((
            row['_predictions']['dk_fantasy_points'] - row['_losses']['rmse'] * 0.5), 1)
        new_data.append({
            **row,
            '70_pct_conf': conf,
            'adjusted_dollars_per_fantasy_point': row['salary_dollars'] / conf,
            'dk_fantasy_points_expected': row['_predictions']['dk_fantasy_points']
        })
    return pd.DataFrame(new_data)


def output_table(df):
    def row_to_table_row(row):
        return {
            'name': row['name'],
            'roster_positions': ', '.join(row['roster_positions']),
            'salary': row['salary_dollars'],
            'difference': round(row['70_pct_conf'] - row['avg_points_per_game'], 2),
            'avg_points_per_game': row['avg_points_per_game'],
            'predicted': row['_predictions']['dk_fantasy_points'],
            '70_pct_conf': round(row['70_pct_conf'], 2),
            'rmse': round(row['_losses']['rmse'], 2),
            'adjusted_dpfp': round(row['adjusted_dollars_per_fantasy_point']),
            'dk_fantasy_points_last_games': ', '.join([str(d) if d else '0' for d in row['dk_fantasy_points_last_games']][0:5])
        }

    table_data = sorted([row_to_table_row(r)
                         for i, r in df.iterrows()], key=lambda d: d['adjusted_dpfp'])
    table_data = [list(table_data[0].keys())] + [list(r.values())
                                                 for r in table_data]
    table = AsciiTable(table_data)
    print(table.table)

    return df


MIN_SPEND = 45000
BUDGET = 50000
MIN_ACCEPTABLE_POINTS = 250
DIFFERENCE_BETWEEN_ROSTERS = 4
N = 30
k = 8


def _is_valid_roster(roster):
    positions = {
        'PG': None,
        'SG': None,
        'SF': None,
        'PF': None,
        'C': None,
        'G': None,
        'F': None,
        'UTIL': None
    }

    for player in roster:
        position_found_for_player = False
        for roster_position in player['roster_positions']:
            if positions[roster_position] is None:
                positions[roster_position] = player
                position_found_for_player = True
                break

        if not position_found_for_player:
            return False

    return True


def pick_lineups(df):
    rosters = []

    print({'nCk': scipy.misc.comb(N, k)})

    sorted_players = sorted(df.to_dict(
        'records'), key=lambda d: d['adjusted_dollars_per_fantasy_point'])
    combo_iter = itertools.combinations(sorted_players[0:N], k)
    rosters = []

    widgets = [
        ' [', progressbar.Timer(), '] ',
        progressbar.Bar(),
        ' (', progressbar.ETA(), ') '
    ]

    for i in progressbar.progressbar(range(int(scipy.misc.comb(N, k))), widgets=widgets):
        roster = next(combo_iter)

        salary = sum([int(d['salary_dollars']) for d in roster])
        if salary < MIN_SPEND or salary > BUDGET:
            continue

        expected_points = sum(
            [int(d['dk_fantasy_points_expected']) for d in roster])
        if (expected_points < MIN_ACCEPTABLE_POINTS):
            continue

        if not _is_valid_roster(roster):
            continue

        rosters.append({
            'df': roster,
            'total_salary': salary,
            'expected_points': expected_points
        })

    print({
        'rosters found': len(rosters)
    })

    rosters = sorted(rosters, key=lambda d: d['expected_points'])
    rosters.reverse()
    rosters = rosters[0:10000]

    def get_roster_players(roster):
        return set([p['player_basketball_reference_id'] for p in roster['df']])

    different_rosters = []
    for roster in rosters:
        if len(different_rosters) == 0:
            different_rosters.append(roster)
            continue

        if (len(list(get_roster_players(different_rosters[-1]) - get_roster_players(roster))) < DIFFERENCE_BETWEEN_ROSTERS):
            continue

        different_rosters.append(roster)

    print({
        'different rosters found': len(different_rosters)
    })

    different_rosters = different_rosters[0:5]

    def row_to_table_row(row):
        return {
            'name': row['name'],
            'roster_positions': ', '.join(row['roster_positions']),
            'salary': row['salary_dollars'],
            'difference': round(row['70_pct_conf'] - row['avg_points_per_game'], 2),
            'avg_points_per_game': row['avg_points_per_game'],
            'predicted': row['_predictions']['dk_fantasy_points'],
            '70_pct_conf': round(row['70_pct_conf'], 2),
            'rmse': round(row['_losses']['rmse'], 2),
            'adjusted_dpfp': round(row['adjusted_dollars_per_fantasy_point']),
            'dk_fantasy_points_last_games': ', '.join([str(d) if d else '0' for d in row['dk_fantasy_points_last_games']][0:5])
        }

    print('\n\n\n\n')

    for roster in different_rosters:
        print('Expected points {}'.format(roster['expected_points']))
        print('Total salary {}'.format(roster['total_salary']))
        table_data = [row_to_table_row(r) for r in roster['df']]
        table_data = [list(table_data[0].keys())] + \
            [list(r.values()) for r in table_data]
        table = AsciiTable(table_data)
        print(table.table)
        print('\n\n')

    return df


memory = Memory(location='./tmp', verbose=1)
embellish_salary_data = memory.cache(embellish_salary_data)
predict_with_player_models = memory.cache(predict_with_player_models)


def main():
    df = scraping.parse_salary_file().pipe(embellish_salary_data).pipe(filter_players).pipe(
        predict_with_player_models).pipe(add_computed_columns).pipe(output_table).pipe(pick_lineups)

    # print(df)


if __name__ == '__main__':
    main()

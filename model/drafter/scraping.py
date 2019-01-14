import pprint
import datetime
import re
import dateparser

from requests_html import HTMLSession
from joblib import Memory
import pandas as pd

import services
import data


def parse_salary_file():
    def get_away_team(game_info):
        m = re.search(r'(\w\w\w?)@\w\w\w?', game_info)
        return data.ABBREVIATIONS[m.group(1)]

    def get_home_team(game_info):
        m = re.search(r'\w\w\w?@(\w\w\w?)', game_info)
        return data.ABBREVIATIONS[m.group(1)]

    def get_time_of_game(game_info):
        m = re.search(r'\w\w\w?@\w\w\w?\s(.*)', game_info)
        return dateparser.parse(m.group(1))

    df = pd.read_csv('./tmp/DKSalaries.csv')
    df['Position'] = df['Position'].map(lambda p: p.split('/'))
    df['Name'] = df['Name'].map(lambda n: n.strip())
    df['Roster Position'] = df['Roster Position'].map(lambda p: p.split('/'))
    df['Away Team'] = df['Game Info'].map(get_away_team)
    df['Home Team'] = df['Game Info'].map(get_home_team)
    df['Time of Game'] = df['Game Info'].map(get_time_of_game)
    df['Player Team'] = df['TeamAbbrev'].map(lambda ta: data.ABBREVIATIONS[ta])

    parsed_data = []

    for i, row in df.iterrows():
        in_db_player = services.pgr.query("""
            select
              p.basketball_reference_id,
              p.birth_country,
              p.date_of_birth,
              tp.experience,
              tp.position
            from players p
            inner join teams_players tp
              on tp.player_basketball_reference_id = p.basketball_reference_id
              and tp.season = '2019'
            where p.name = :name
            and tp.team_basketball_reference_id = :team
        """, name=row['Name'], team=row['Player Team']).first(as_dict=True)

        if not in_db_player:
            continue
            raise Exception(f"No player found with name {row['Name']}")

        parsed_data.append({
            **row.to_dict(),
            **in_db_player
        })

    return pd.DataFrame(parsed_data)


def map_player_name(player_name):
    if (player_name == 'W. Hernangomez'):
        return 'G. Hernangomez'

    return player_name


# NOTE Using date to bust joblib cache
def get_lineups(date):
    session = HTMLSession()
    r = session.get('https://www.rotowire.com/basketball/nba-lineups.php')
    game_els = r.html.find('.lineup.is-nba:not(.is-tools)')
    team_lineups = {}
    for game_el in game_els:
        away_team_abbreviation = data.ABBREVIATIONS[game_el.find('.lineup__abbr')[0].text.strip()]
        team_lineups[away_team_abbreviation] = {'starters': [], 'injured': []}

        away_roster_player_els = game_el.find(
            '.lineup__list.is-visit .lineup__player')
        for i, player_el in enumerate(away_roster_player_els):
            key = 'starters' if i < 5 else 'injured'
            team_lineups[away_team_abbreviation][key].append({
                'team': away_team_abbreviation,
                'position': player_el.find('.lineup__pos')[0].text.strip(),
                'name': map_player_name(data.format_player_name(player_el.find('.lineup__pos + a')[0].text.strip())),
                'injury': None if len(player_el.find('.lineup__inj')) == 0 else player_el.find('.lineup__inj')[0].text.strip()
            })

        home_team_abbreviation = data.ABBREVIATIONS[game_el.find('.lineup__abbr')[1].text.strip()]
        team_lineups[home_team_abbreviation] = {'starters': [], 'injured': []}

        home_roster_player_els = game_el.find(
            '.lineup__list.is-home .lineup__player')
        for i, player_el in enumerate(home_roster_player_els):
            key = 'starters' if i < 5 else 'injured'
            team_lineups[home_team_abbreviation][key].append({
                'team': home_team_abbreviation,
                'position': player_el.find('.lineup__pos')[0].text.strip(),
                'name': map_player_name(data.format_player_name(player_el.find('.lineup__pos + a')[0].text.strip())),
                'injury': None if len(player_el.find('.lineup__inj')) == 0 else player_el.find('.lineup__inj')[0].text.strip()
            })

    return team_lineups

memory = Memory(location='./tmp', verbose=1)
get_lineups = memory.cache(get_lineups)


if __name__ == '__main__':
    pprint.pprint(get_lineups(datetime.datetime.now().date()))

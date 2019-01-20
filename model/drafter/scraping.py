import pprint
import datetime
import re
import dateparser
import sys
import os

from requests_html import HTMLSession
from joblib import Memory
import pandas as pd

import services
import data


# DKSalaries.csv


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


def _map_player_name(player_name):
    if (player_name == 'W. Hernangomez'):
        return 'G. Hernangomez'

    return player_name


# Starting lineups


# NOTE Using date to bust joblib cache
def get_lineups(date=None):
    pbtfn = data.get_players_by_team_and_formatted_name()

    session = HTMLSession()
    r = session.get('https://www.rotowire.com/basketball/nba-lineups.php')
    game_els = r.html.find('.lineup.is-nba:not(.is-tools)')
    team_lineups = {}
    for game_el in game_els:
        away_team_abbreviation = data.ABBREVIATIONS[game_el.find('.lineup__abbr')[
            0].text.strip()]
        team_lineups[away_team_abbreviation] = {'starters': [], 'injured': []}

        away_roster_player_els = game_el.find(
            '.lineup__list.is-visit .lineup__player')
        for i, player_el in enumerate(away_roster_player_els):
            player_formatted_name = _map_player_name(data.format_player_name(
                player_el.find('.lineup__pos + a')[0].text.strip()))

            # Ensure player exists
            pbtfn[away_team_abbreviation + ' ' + player_formatted_name]

            key = 'starters' if i < 5 else 'injured'
            team_lineups[away_team_abbreviation][key].append({
                'team': away_team_abbreviation,
                'position': player_el.find('.lineup__pos')[0].text.strip(),
                'name': player_formatted_name,
                'injury': None if len(player_el.find('.lineup__inj')) == 0 else player_el.find('.lineup__inj')[0].text.strip()
            })

        assert len(team_lineups[away_team_abbreviation]['starters']) == 5

        home_team_abbreviation = data.ABBREVIATIONS[game_el.find('.lineup__abbr')[
            1].text.strip()]
        team_lineups[home_team_abbreviation] = {'starters': [], 'injured': []}

        home_roster_player_els = game_el.find(
            '.lineup__list.is-home .lineup__player')
        for i, player_el in enumerate(home_roster_player_els):
            player_formatted_name = _map_player_name(data.format_player_name(
                player_el.find('.lineup__pos + a')[0].text.strip()))

            # Ensure player exists
            pbtfn[home_team_abbreviation + ' ' + player_formatted_name]

            key = 'starters' if i < 5 else 'injured'
            team_lineups[home_team_abbreviation][key].append({
                'team': home_team_abbreviation,
                'position': player_el.find('.lineup__pos')[0].text.strip(),
                'name': player_formatted_name,
                'injury': None if len(player_el.find('.lineup__inj')) == 0 else player_el.find('.lineup__inj')[0].text.strip()
            })

        assert len(team_lineups[home_team_abbreviation]['starters']) == 5

    return team_lineups


# Scrape teams


def _scrape_team(url):
    print(url)

    session = HTMLSession()
    r = session.get(url)

    team_name = r.html.find('#meta [itemprop="name"] span')[0].text.strip()
    basketball_reference_id = r.html.find('[rel="canonical"]')[
        0].attrs['href'].split('/')[4]

    team = services.pgr.query(
        '''
            select *
            from teams
            where basketball_reference_id = :basketball_reference_id
        ''',
        basketball_reference_id=basketball_reference_id
    ).first(as_dict=True)

    if not team:
        team = services.pgw.query(
            '''
                insert into teams (name, basketball_reference_id)
                values (:name, :basketball_reference_id)
                returning *
            ''',
            name=team_name,
            basketball_reference_id=basketball_reference_id
        ).first(as_dict=True)

    # pprint.pprint(team)

    return team


def _scrape_team_roster(team, url):
    session = HTMLSession()
    r = session.get(url)

    print(url)

    season = int(r.html.find(
        '#meta [itemprop="name"] span:first-of-type')[0].text.split('-')[0]) + 1

    roster_trs = r.html.find('#roster tr')[1:]
    for tr in roster_trs:

        player_number_text = tr.find('[data-stat="number"]')[0].text.strip()
        player_number = int(
            player_number_text) if player_number_text != '' else None
        name = tr.find(
            '[data-stat="player"] a')[0].text.split('(TW)')[0].strip()
        player_basketball_reference_id = tr.find(
            '[data-stat="player"] a')[0].attrs['href'].split('/')[3].split('.html')[0].strip()
        position = tr.find('[data-stat="pos"]')[0].text.strip()
        height_inches_parts = list(
            map(int, tr.find('[data-stat="height"]')[0].text.split('-')))
        height_inches = height_inches_parts[0] * 12 + height_inches_parts[1]
        weight_lbs = int(tr.find('[data-stat="weight"]')[0].text.strip())
        date_of_birth = dateparser.parse(
            tr.find('[data-stat="birth_date"]')[0].text.strip())
        birth_country = tr.find('[data-stat="birth_country"]')[0].text.strip()
        experience = int(
            tr.find('[data-stat="years_experience"]')[0].text.strip())

        # pprint.pprint({
        #     'name': name,
        #     'player_basketball_reference_id': player_basketball_reference_id,
        #     'position': position,
        #     'height_inches': height_inches,
        #     'weight_lbs': weight_lbs,
        #     'date_of_birth': date_of_birth,
        #     'birth_country': birth_country,
        #     'experience': experience
        # })

        in_db_player = services.pgr.query(
            '''
                select *
                from players
                where basketball_reference_id = :player_basketball_reference_id
            ''',
            player_basketball_reference_id=player_basketball_reference_id
        ).first(as_dict=True)

        if not in_db_player:
            in_db_player = services.pgw.query(
                '''
                    insert into players (
                        name,
                        date_of_birth,
                        birth_country
                    )
                    values (
                        :name,
                        :date_of_birth,
                        :birth_country
                    )
                ''',
                name=name,
                date_of_birth=date_of_birth,
                birth_country=birth_country
            ).first(as_dict=True)

        services.pgw.query(
            '''
                delete from teams_players
                where player_basketball_reference_id = :player_basketball_reference_id
                and team_basketball_reference_id = :team_basketball_reference_id
                and season = :season
            ''',
            player_basketball_reference_id=player_basketball_reference_id,
            team_basketball_reference_id=team['basketball_reference_id'],
            season=season
        )

        in_db_team_player = services.pgw.query(
            '''
                insert into teams_players (
                    player_basketball_reference_id,
                    team_basketball_reference_id,
                    season,
                    player_number,
                    position,
                    height_inches,
                    weight_lbs,
                    experience,
                    currently_on_this_team
                )
                values (
                    :player_basketball_reference_id,
                    :team_basketball_reference_id,
                    :season,
                    :player_number,
                    :position,
                    :height_inches,
                    :weight_lbs,
                    :experience,
                    :currently_on_this_team
                )
                returning *
            ''',
            player_basketball_reference_id=player_basketball_reference_id,
            team_basketball_reference_id=team['basketball_reference_id'],
            season=season,
            player_number=player_number,
            position=position,
            height_inches=height_inches,
            weight_lbs=weight_lbs,
            experience=experience,
            currently_on_this_team=(True if season == 2019 else False)
        ).first(as_dict=True)

        return in_db_team_player


def scrape_teams():
    MIN_SEASON = 2019
    if os.environ.get('ALL'):
        MIN_SEASON = 2003

    session = HTMLSession()
    r = session.get('https://www.basketball-reference.com/teams')

    active_team_as = r.html.find('#teams_active a')
    for active_team_a in active_team_as:
        team_href = active_team_a.attrs['href']
        team_url = f'https://www.basketball-reference.com{team_href}'
        team = _scrape_team(team_url)

        session = HTMLSession()
        team_r = session.get(team_url)

        season_as = team_r.html.find('[data-stat="season"] a')
        for season_a in season_as:
            season_a_href = season_a.attrs['href']
            season = int(season_a_href.split('/')[3].split('.')[0])
            if season < MIN_SEASON:
                continue
            _scrape_team_roster(
                team, f'https://www.basketball-reference.com{season_a_href}')


def _time_to_seconds(time):
    parts = time.split(':')
    return int(parts[0]) * 60 + int(parts[1])


def _scrape_games_players(game_id, player_trs):

    starter = True
    players = []

    for tr in player_trs:
        ths = tr.find('th')
        if len(ths) > 0 and ths[0].text.strip().lower() == 'reserves':
            starter = False
            continue

        tds = tr.find('td')

        if tds[0].text.strip().lower() == 'did not play':
            continue

        player = {
            'starter': starter,
            'game_basketball_reference_id': game_id,
            'player_basketball_reference_id': ths[0].find('a')[0].attrs['href'].split('/')[3].split('.')[0],
            'seconds_played': _time_to_seconds(tds[0].text.strip()),
            'field_goals': int(tds[1].text.strip()),
            'field_goals_attempted': int(tds[2].text.strip()),
            'three_point_field_goals': int(tds[4].text.strip()),
            'three_point_field_goals_attempted': int(tds[5].text.strip()),
            'free_throws': int(tds[7].text.strip()),
            'free_throws_attempted': int(tds[8].text.strip()),
            'offensive_rebounds': int(tds[10].text.strip()),
            'defensive_rebounds': int(tds[11].text.strip()),
            'total_rebounds': int(tds[12].text.strip()),
            'assists': int(tds[13].text.strip()),
            'steals': int(tds[14].text.strip()),
            'blocks': int(tds[15].text.strip()),
            'turnovers': int(tds[16].text.strip()),
            'personal_fouls': int(tds[17].text.strip()),
            'points': int(tds[18].text.strip()),
            'plus_minus': int(tds[19].text.strip())
        }

        players.append(player)

    return players


def scrape_games():
    now = datetime.datetime.now()

    time_of_last_game = datetime.datetime(2002, 10, 1)

    if not os.environ.get('ALL'):
        last_game = services.pgr.query(
            '''
                select g.time_of_game
                from games g
                order by g.time_of_game desc
                limit 1
            '''
        ).first(as_dict=True)

        time_of_last_game = last_game['time_of_game'] if last_game else time_of_last_game

    delta = now - time_of_last_game
    number_of_days = delta.days + 1
    if os.environ.get('DEBUG'):
        number_of_days = 1
    for i in range(number_of_days):
        date = time_of_last_game + datetime.timedelta(i)
        url = f'https://www.basketball-reference.com/boxscores/?month={date.month}&day={date.day}&year={date.year}'

        print(url)

        session = HTMLSession()
        r = session.get(url)
        game_links = r.html.find('.game_summary .gamelink a')
        game_ids = list(map(lambda gl: gl.attrs['href'].split(
            '/')[2].split('.')[0], game_links))

        if os.environ.get('DEBUG'):
            game_ids = game_ids[0:1]

        for game_id in game_ids:
            game_url = f'https://www.basketball-reference.com/boxscores/{game_id}.html'
            print(game_url)

            game_session = HTMLSession()
            r = game_session.get(game_url)

            time_of_game = dateparser.parse(
                f"{r.html.find('.scorebox_meta div:nth-of-type(1)')[0].text.strip()} EST")

            game_data = {
                'basketball_reference_id': game_id,
                'away_team_basketball_reference_id': r.html.find(
                    '.scorebox div:first-of-type [itemprop="name"]')[0].attrs['href'].split('/')[2],
                'home_team_basketball_reference_id': r.html.find(
                    '.scorebox div:nth-of-type(2) [itemprop="name"]')[0].attrs['href'].split('/')[2],
                'away_score': int(r.html.find('.scorebox div:first-of-type .score')[0].text.strip()),
                'home_score': int(r.html.find('.scorebox div:nth-of-type(2) .score')[0].text.strip()),
                'time_of_game': time_of_game,
                'season': time_of_game.year + 1 if time_of_game.month > 7 else time_of_game.year,
                'arena': r.html.find('.scorebox_meta div:nth-of-type(2)')[0].text.strip(),
                'away_games_players': _scrape_games_players(game_id, r.html.find('#all_four_factors + div + div tbody tr')),
                'home_games_players': _scrape_games_players(game_id, r.html.find('#all_four_factors + div + div + div + div tbody tr'))
            }

            pprint.pprint(game_data)

            game = services.pgr.query(
                '''
                    select *
                    from games
                    where basketball_reference_id = :basketball_reference_id
                ''',
                basketball_reference_id=game_id
            ).first(as_dict=True)

            if not game:
                game = services.pgw.query(
                    '''
                        insert into games (
                            basketball_reference_id,
                            home_team_basketball_reference_id,
                            away_team_basketball_reference_id,
                            home_score,
                            away_score,
                            arena,
                            time_of_game,
                            season
                        )
                        values (
                            :basketball_reference_id,
                            :home_team_basketball_reference_id,
                            :away_team_basketball_reference_id
                            :home_score,
                            :away_score,
                            :arena,
                            :time_of_game,
                            :season
                        )
                    ''',
                    **game_data
                )

            for games_player in [*game_data['away_games_players'], *game_data['home_games_players']]:
                in_db_gp = services.pgr.query(
                    '''
                        select *
                        from games_players
                        where game_basketball_reference_id = :game_basketball_reference_id
                        and player_basketball_reference_id = :player_basketball_reference_id
                    ''',
                    **games_player
                ).first(as_dict=True)

                if (in_db_gp):
                    continue

                in_db_gb = services.pgw.query(
                    '''
                        insert into games_players (
                            starter,
                            game_basketball_reference_id,
                            player_basketball_reference_id,
                            seconds_played,
                            field_goals,
                            field_goals_attempted,
                            three_point_field_goals,
                            three_point_field_goals_attempted,
                            free_throws,
                            free_throws_attempted,
                            offensive_rebounds,
                            defensive_rebounds,
                            total_rebounds,
                            assists,
                            steals,
                            blocks,
                            turnovers,
                            personal_fouls,
                            points,
                            plus_minus
                        )
                        values (
                            :starter,
                            :game_basketball_reference_id,
                            :player_basketball_reference_id,
                            :seconds_played,
                            :field_goals,
                            :field_goals_attempted,
                            :three_point_field_goals,
                            :three_point_field_goals_attempted,
                            :free_throws,
                            :free_throws_attempted,
                            :offensive_rebounds,
                            :defensive_rebounds,
                            :total_rebounds,
                            :assists,
                            :steals,
                            :blocks,
                            :turnovers,
                            :personal_fouls,
                            :points,
                            :plus_minus
                        )
                    ''',
                    **gp
                )

                data.cache_single_games_player({
                    **in_db_gp,
                    'season': game['season'],
                    'time_of_game': game['time_of_game']
                })

            # NOTE Caching the game depends on the games_players already being cached

            data.cache_single_game(game)


memory = Memory(location='./tmp', verbose=1)
get_lineups = memory.cache(get_lineups)


if __name__ == '__main__':
    arg = sys.argv[1]
    if arg == 'lineups':
        get_lineups()
    elif arg == 'teams':
        scrape_teams()
    elif arg == 'games':
        scrape_games()
    else:
        print(f'Argument not recognized: {arg}')

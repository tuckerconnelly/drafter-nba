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
        in_db_player = services.sqw.query("""
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

    team_name = r.html.find('#meta [itemprop="name"] span')[1].text.strip()
    basketball_reference_id = r.html.find('[rel="canonical"]')[
        0].attrs['href'].split('/')[4]

    team = services.sqw.query(
        '''
            select *
            from teams
            where basketball_reference_id = :basketball_reference_id
        ''',
        basketball_reference_id=basketball_reference_id
    ).first(as_dict=True)

    if not team:
        team = services.sqw.query(
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

    season = int(r.html.find(
        '#meta [itemprop="name"] span:first-of-type')[0].text.split('-')[0]) + 1

    roster_trs = r.html.find('#roster tr')[1:]
    for tr in roster_trs:

        player_number_text = tr.find('[data-stat="number"]')[0].text.strip()
        try:
            player_number = int(
                player_number_text) if player_number_text != '' else None
        except ValueError:
            player_number = int(player_number_text.split('-')[0])

        name = tr.find(
            '[data-stat="player"] a')[0].text.split('(TW)')[0].strip()
        player_basketball_reference_id = tr.find(
            '[data-stat="player"] a')[0].attrs['href'].split('/')[3].split('.html')[0].strip()
        position = tr.find('[data-stat="pos"]')[0].text.strip()
        height_inches = _height_to_inchdes(tr.find('[data-stat="height"]')[0].text)
        weight_lbs = int(tr.find('[data-stat="weight"]')[0].text.strip())
        date_of_birth = dateparser.parse(
            tr.find('[data-stat="birth_date"]')[0].text.strip())
        birth_country = tr.find('[data-stat="birth_country"]')[0].text.strip()
        experience = int(
            tr.find('[data-stat="years_experience"]')[0].text.strip()) if tr.find('[data-stat="years_experience"]')[0].text.strip() != 'R' else 0

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

        assert player_basketball_reference_id


        in_db_player = services.sqw.query(
            '''
                select *
                from players
                where basketball_reference_id = :player_basketball_reference_id
            ''',
            player_basketball_reference_id=player_basketball_reference_id
        ).first(as_dict=True)

        if not in_db_player:
            in_db_player = services.sqw.query(
                '''
                    insert into players (
                        basketball_reference_id,
                        name,
                        date_of_birth,
                        birth_country
                    )
                    values (
                        :basketball_reference_id,
                        :name,
                        :date_of_birth,
                        :birth_country
                    )
                    returning *
                ''',
                basketball_reference_id=player_basketball_reference_id,
                name=name,
                date_of_birth=date_of_birth,
                birth_country=birth_country
            ).first(as_dict=True)

        services.sqw.query(
            '''
                update teams_players
                set currently_on_this_team = false
                where player_basketball_reference_id = :player_basketball_reference_id
                and team_basketball_reference_id = :team_basketball_reference_id
                and season = :season
            ''',
            player_basketball_reference_id=player_basketball_reference_id,
            team_basketball_reference_id=team['basketball_reference_id'],
            season=season
        )

        in_db_team_player = services.sqw.query(
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


MIN_SEASON = 2019
if os.environ.get('ALL'):
    MIN_SEASON = 1984

def scrape_teams():
    session = HTMLSession()
    r = session.get('https://www.basketball-reference.com/teams')

    active_team_as = r.html.find('#teams_active a')
    for active_team_a in active_team_as:
        team_href = active_team_a.attrs['href']
        team_url = f'https://www.basketball-reference.com{team_href}'

        session = HTMLSession()
        team_r = session.get(team_url)

        season_as = team_r.html.find('[data-stat="season"] a')
        for season_a in season_as:
            season_a_href = season_a.attrs['href']
            season = int(season_a_href.split('/')[3].split('.')[0])
            if season < MIN_SEASON:
                continue

            season_url = f'https://www.basketball-reference.com{season_a_href}'
            team = _scrape_team(season_url)
            _scrape_team_roster(team, season_url)


def _scrape_player_page(player_basketball_reference_id):
    url = f"https://www.basketball-reference.com/players/{player_basketball_reference_id[0]}/{player_basketball_reference_id}.html"
    print(url)
    session = HTMLSession()
    r = session.get(url)

    player = {
        'name': r.html.find('#meta [itemprop="name"]')[0].text.strip(),
        'height_inches': _height_to_inches(r.html.find('#meta [itemprop="height"]')[0].text.strip()),
        'weight_lbs': int(r.html.find('#meta [itemprop="weight"]')[0].text.strip().split('lb')[0]),
        'date_of_birth': dateparser.parse(r.html.find('#meta [itemprop="birthDate"]')[0].attrs['data-birth'].strip()),
        'birth_country': r.html.find('#meta [itemprop="birthPlace"] + span')[0].text.strip()
    }

    player_total_trs = r.html.find('#all_per_game tr')[1:]

    experience = 0
    seasons = []
    for tr in player_total_trs:
        tds = tr.find('td')
        if 'did not' in tds[2].text.strip().lower():
            continue
        if 'not with' in tds[2].text.strip().lower():
            continue
        if 'player suspended' in tds[2].text.strip().lower():
            continue
        if tds[1].text.strip() == 'TOT':
            continue
        if tr.find('th')[0].text.strip() == 'Career':
            break

        season = int(tr.find('th')[0].text.strip().split('-')[0]) + 1

        if len(seasons) > 0 and seasons[-1]['season'] != season:
            experience += 1

        seasons.append({
            'season': season,
            'team_basketball_reference_id': tds[1].text.strip(),
            'position': tds[2].text.strip(),
            'experience': experience
        })

    player['seasons'] = seasons

    pprint.pprint(player)

    return player


def _ensure_player_exists_on_roster(player_basketball_reference_id, team_basketball_reference_id, season):
    in_db_gp = services.sqw.query(
        '''
            select id
            from teams_players
            where player_basketball_reference_id = :player_basketball_reference_id
            and team_basketball_reference_id = :team_basketball_reference_id
            and season = :season
        ''',
        player_basketball_reference_id=player_basketball_reference_id,
        team_basketball_reference_id=team_basketball_reference_id,
        season=season
    ).first(as_dict=True)

    if in_db_gp:
        return

    print(f'Player {player_basketball_reference_id} not found, adding')

    player_data = _scrape_player_page(player_basketball_reference_id)

    in_db_p = services.sqw.query(
        '''
            select id
            from players
            where basketball_reference_id = :basketball_reference_id
        ''',
        basketball_reference_id=player_basketball_reference_id
    ).first(as_dict=True)

    if not in_db_p:
        services.sqw.query(
            '''
                insert into players (
                    basketball_reference_id,
                    name,
                    date_of_birth,
                    birth_country
                )
                values (
                    :basketball_reference_id,
                    :name,
                    :date_of_birth,
                    :birth_country
                )
            ''',
            basketball_reference_id=player_basketball_reference_id,
            name=player_data['name'],
            date_of_birth=player_data['date_of_birth'],
            birth_country=player_data['birth_country']
        )

    for season in player_data['seasons']:
        existing_tp = services.sqw.query(
            '''
                select id
                from teams_players
                where player_basketball_reference_id = :player_basketball_reference_id
                and team_basketball_reference_id = :team_basketball_reference_id
                and season = :season
            ''',
            player_basketball_reference_id=player_basketball_reference_id,
            team_basketball_reference_id=season['team_basketball_reference_id'],
            season=season['season']
        ).first(as_dict=True)

        if existing_tp:
            continue

        # NOTE No player number, difficult (but not impossible) to scrape off
        # of player plage

        services.sqw.query(
            '''
                insert into teams_players (
                    player_basketball_reference_id,
                    team_basketball_reference_id,
                    season,
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
                    :position,
                    :height_inches,
                    :weight_lbs,
                    :experience,
                    :currently_on_this_team
                )
            ''',
            player_basketball_reference_id=player_basketball_reference_id,
            team_basketball_reference_id=season['team_basketball_reference_id'],
            season=season['season'],
            position=season['position'],
            height_inches=player_data['height_inches'],
            weight_lbs=player_data['weight_lbs'],
            experience=season['experience'],
            currently_on_this_team=False
        )


def _height_to_inches(height_text):
    clean_height_text = height_text.split(',')[0]
    height_inches_parts = list(map(int, clean_height_text.split('-')))
    return height_inches_parts[0] * 12 + height_inches_parts[1]

def _time_to_seconds(time):
    if time == '':
        return None
    parts = time.split(':')
    return int(parts[0]) * 60 + int(parts[1])

def _td_int(tds, i):
    try:
        return int(tds[i].text.strip())
    except ValueError:
        return None
    except IndexError:
        return None


def _scrape_games_players(game_id, player_trs):
    starter = True
    players = []

    for tr in player_trs:
        ths = tr.find('th')
        if len(ths) > 0 and ths[0].text.strip().lower() == 'reserves':
            starter = False
            continue

        tds = tr.find('td')

        if 'did not' in tds[0].text.strip().lower():
            continue

        if 'not with' in tds[0].text.strip().lower():
            continue

        if 'player suspended' in tds[0].text.strip().lower():
            continue

        player = {
            'starter': starter,
            'game_basketball_reference_id': game_id,
            'player_basketball_reference_id': ths[0].find('a')[0].attrs['href'].split('/')[3].split('.')[0],
            'seconds_played': _time_to_seconds(tds[0].text.strip()),
            'field_goals': _td_int(tds, 1),
            'field_goals_attempted': _td_int(tds, 2),
            'three_point_field_goals': _td_int(tds, 4),
            'three_point_field_goals_attempted': _td_int(tds, 5),
            'free_throws': _td_int(tds, 7),
            'free_throws_attempted': _td_int(tds, 8),
            'offensive_rebounds': _td_int(tds, 10),
            'defensive_rebounds': _td_int(tds, 11),
            'total_rebounds': _td_int(tds, 12),
            'assists': _td_int(tds, 13),
            'steals': _td_int(tds, 14),
            'blocks': _td_int(tds, 15),
            'turnovers': _td_int(tds, 16),
            'personal_fouls': _td_int(tds, 17),
            'points': _td_int(tds, 18),
            'plus_minus': _td_int(tds, 19)
        }

        assert player['points'] is not None

        players.append(player)

    return players


def _insert_games_player(games_player_data, game, player_team_basketball_reference_id, opp_team_basketball_reference_id):
    _ensure_player_exists_on_roster(
        games_player_data['player_basketball_reference_id'],
        player_team_basketball_reference_id,
        game['season']
    )

    in_db_gp = services.sqw.query(
        '''
            select id
            from games_players
            where game_basketball_reference_id = :game_basketball_reference_id
            and player_basketball_reference_id = :player_basketball_reference_id
        ''',
        game_basketball_reference_id=games_player_data['game_basketball_reference_id'],
        player_basketball_reference_id=games_player_data['player_basketball_reference_id']
    ).first(as_dict=True)

    if not in_db_gp:
        services.sqw.query(
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
            starter=games_player_data['starter'],
            game_basketball_reference_id=games_player_data['game_basketball_reference_id'],
            player_basketball_reference_id=games_player_data['player_basketball_reference_id'],
            seconds_played=games_player_data['seconds_played'],
            field_goals=games_player_data['field_goals'],
            field_goals_attempted=games_player_data['field_goals_attempted'],
            three_point_field_goals=games_player_data['three_point_field_goals'],
            three_point_field_goals_attempted=games_player_data['three_point_field_goals_attempted'],
            free_throws=games_player_data['free_throws'],
            free_throws_attempted=games_player_data['free_throws_attempted'],
            offensive_rebounds=games_player_data['offensive_rebounds'],
            defensive_rebounds=games_player_data['defensive_rebounds'],
            total_rebounds=games_player_data['total_rebounds'],
            assists=games_player_data['assists'],
            steals=games_player_data['steals'],
            blocks=games_player_data['blocks'],
            turnovers=games_player_data['turnovers'],
            personal_fouls=games_player_data['personal_fouls'],
            points=games_player_data['points'],
            plus_minus=games_player_data['plus_minus']
        )

    # data.cache_single_games_player(
    #     in_db_gp,
    #     player_team_basketball_reference_id,
    #     opp_team_basketball_reference_id,
    #     game['season'],
    #     game['time_of_game']
    # )

    return in_db_gp


def scrape_games():
    from_date = None
    if os.environ.get('FROM_DATE'):
        from_date = dateparser.parse(os.environ.get('FROM_DATE'))
    else:
        last_game = services.sqw.query(
            '''
                select g.time_of_game
                from games g
                order by g.time_of_game desc
                limit 1
            '''
        ).first(as_dict=True)

        from_date = last_game['time_of_game'] if last_game else None

    if not from_date:
        print('No FROM_DATE set, and no games in database')
        raise

    if os.environ.get('TO_DATE'):
        to_date = dateparser.parse(os.environ.get('TO_DATE'))
    else:
        to_date = datetime.datetime.now()

    print(f'Scraping from {from_date} to {to_date}')

    delta = to_date - from_date
    number_of_days = delta.days + 1
    if os.environ.get('DEBUG'):
        number_of_days = 1
    for i in range(number_of_days):
        date = from_date + datetime.timedelta(i)
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

            # pprint.pprint(game_data)

            game = services.sqw.query(
                '''
                    select *
                    from games
                    where basketball_reference_id = :basketball_reference_id
                ''',
                basketball_reference_id=game_id
            ).first(as_dict=True)

            if not game:
                game = services.sqw.query(
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
                            :away_team_basketball_reference_id,
                            :home_score,
                            :away_score,
                            :arena,
                            :time_of_game,
                            :season
                        )
                        returning *
                    ''',
                    **game_data
                ).first(as_dict=True)

            for games_player_data in game_data['away_games_players']:
                _insert_games_player(
                    games_player_data, game, game_data['away_team_basketball_reference_id'], game_data['home_team_basketball_reference_id'])

            for games_player_data in game_data['home_games_players']:
                _insert_games_player(
                    games_player_data, game, game_data['home_team_basketball_reference_id'], game_data['away_team_basketball_reference_id'])


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

import drafter.data


def test_calculate_fantasy_score():
    assert drafter.data.calculate_fantasy_score({
        'seconds_played': 10,

        'points': 15,
        'three_point_field_goals': 2,
        'total_rebounds': 4,
        'assists': 10,
        'steals': 10,
        'blocks': 1,
        'turnovers': 2
    }) == 58.5


def test_get_stats_last_games_from_pg():
    print(drafter.data.get_stats_last_games_from_pg(
        player_basketball_reference_id='jamesle01',
        season=2019,
        current_game_date='2019-01-01'
    ))


def test_team_get_stats_from_last_games_from_pg():
    print(drafter.data.team_get_stats_last_games_from_pg(
        team_basketball_reference_id='DEN',
        season=2019,
        current_game_date='2019-01-01'
    ))


def test_get_data_from_pg():
    print(drafter.data.get_data_from_pg(
        limit=10,
        offset=1000
    ))


def test_get_game_stats():
    print(drafter.data.get_game_stats())


def test_get_stats():
    print(drafter.data.get_stats())


def test_get_players():
    print(drafter.data.get_players())


def test_get_players():
    print(drafter.data.format_player_name('Lebron James Jr.'))


def test_get_players_by_team_and_formatted_name():
    print(drafter.data.get_players_by_team_and_formatted_name())


def test_get_teams():
    print(drafter.data.get_teams())


def test_get_positions():
    print(drafter.data.get_positions())


def test_standardized_average_of_normal_last_games():
    print(drafter.data.standardized_average_of_normal_last_games(
        4, [None, 1, 2, 3, 4, 5]))


def test_mapper():
    player = drafter.data.get_data_from_pg(
        player_basketball_reference_id='jamesle01'
    )[0]
    mappers = drafter.data.make_mapper()
    print(mappers.datum_to_x(player).tolist())
    print(mappers.datum_to_y(player))
    print(mappers.datum_to_w(player))
    print(mappers.y_to_datum([0.01]))
    print(mappers.y_to_datum(mappers.datum_to_y(player)))


def test_load_data():
    print(drafter.data.load_data(max_samples=10))


def test_load_player_data():
    print(drafter.data.load_player_data(player_basketball_reference_id='jamesle01'))
    raise

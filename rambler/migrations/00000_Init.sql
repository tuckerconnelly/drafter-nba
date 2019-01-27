-- rambler up

create table games (
    id integer primary key autoincrement,
    created_at datetime default current_timestamp not null,
    updated_at datetime default current_timestamp not null,
    basketball_reference_id text unique,
    season integer,
    home_team_basketball_reference_id text,
    away_team_basketball_reference_id text,
    home_score integer,
    away_score integer,
    arena text,
    time_of_game datetime
);

create table games_players (
    id integer primary key autoincrement,
    created_at datetime default current_timestamp not null,
    updated_at datetime default current_timestamp not null,
    game_basketball_reference_id text,
    player_basketball_reference_id text,
    starter boolean,
    seconds_played integer,
    field_goals integer,
    field_goals_attempted integer,
    three_point_field_goals integer,
    three_point_field_goals_attempted integer,
    free_throws integer,
    free_throws_attempted integer,
    offensive_rebounds integer,
    defensive_rebounds integer,
    total_rebounds integer,
    assists integer,
    steals integer,
    blocks integer,
    turnovers integer,
    personal_fouls integer,
    points integer,
    plus_minus integer,
    unique(game_basketball_reference_id,player_basketball_reference_id)
);

create table games_players_computed (
    id integer primary key autoincrement,
    created_at datetime default current_timestamp not null,
    updated_at datetime default current_timestamp not null,
    game_basketball_reference_id text,
    player_basketball_reference_id text,
    dk_fantasy_points real,
    dk_fantasy_points_last_games text default '[]',
    seconds_played_last_games text default '[]',
    times_of_last_games text default '[]',
    times_of_last_games_against_opp_away text default '[]',
    times_of_last_games_against_opp_home text default '[]',
    dk_fantasy_points_last_games_against_opp_away text default '[]',
    dk_fantasy_points_last_games_against_opp_home text default '[]',
    seconds_played_last_games_against_opp_away text default '[]',
    seconds_played_last_games_against_opp_home text default '[]',
    plus_minus_last_games text default '[]',
    plus_minus_last_games_against_opp_away text default '[]',
    plus_minus_last_games_against_opp_home text default '[]',
    dk_fantasy_points_per_minute real,
    dk_fantasy_points_per_minute_last_games text default '[]',
    dk_fantasy_points_per_minute_last_games_against_opp_away text default '[]',
    dk_fantasy_points_per_minute_last_games_against_opp_home text default '[]',
    opp_dk_fantasy_points_allowed_vs_position_last_game_only real,
    opp_dk_fantasy_points_allowed_vs_position_last_games text default '[]',
    opp_dk_fantasy_points_allowed_vs_position_last_games_home text default '[]',
    opp_dk_fantasy_points_allowed_vs_position_last_games_away text default '[]',
    unique(game_basketball_reference_id, player_basketball_reference_id)
);

create table players (
    id integer primary key autoincrement,
    created_at datetime default current_timestamp not null,
    updated_at datetime default current_timestamp not null,
    basketball_reference_id text unique,
    name text,
    date_of_birth date,
    birth_country text
);

create table teams (
    id integer primary key autoincrement,
    created_at datetime default current_timestamp not null,
    updated_at datetime default current_timestamp not null,
    basketball_reference_id text unique,
    name text
);

create table teams_players (
    id integer primary key autoincrement,
    created_at datetime default current_timestamp not null,
    updated_at datetime default current_timestamp not null,
    team_basketball_reference_id text,
    player_basketball_reference_id text,
    season integer,
    player_number integer,
    position text,
    height_inches integer,
    weight_lbs integer,
    experience integer,
    currently_on_this_team boolean,
    unique(team_basketball_reference_id, player_basketball_reference_id, season)
);

-- rambler down

drop table teams_players;
drop table teams;
drop table players;
drop table games_players_computed;
drop table games_players;
drop table games;

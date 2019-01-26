-- rambler up

alter table games_players
  add column plus_minus int;

alter table games_players_computed
  add column times_of_last_games timestamp[],
  add column times_of_last_games_against_opp_away timestamp[],
  add column times_of_last_games_against_opp_home timestamp[],

  add column dk_fantasy_points_last_games_against_opp_away jsonb,
  add column dk_fantasy_points_last_games_against_opp_home jsonb,

  add column seconds_played_last_games_against_opp_away jsonb,
  add column seconds_played_last_games_against_opp_home jsonb,

  add column plus_minus_last_games jsonb,
  add column plus_minus_last_games_against_opp_away jsonb,
  add column plus_minus_last_games_against_opp_home jsonb,

  add column dk_fantasy_points_per_minute real,
  add column dk_fantasy_points_per_minute_last_games jsonb,
  add column dk_fantasy_points_per_minute_last_games_against_opp_away jsonb,
  add column dk_fantasy_points_per_minute_last_games_against_opp_home jsonb,

  add column opp_dk_fantasy_points_allowed_vs_position_last_games jsonb,
  add column opp_dk_fantasy_points_allowed_vs_position_last_games_home jsonb,
  add column opp_dk_fantasy_points_allowed_vs_position_last_games_away jsonb;

drop table games_computed;

-- rambler down

alter table games_players_computed
  drop column times_of_last_games,
  drop column times_of_last_games_against_opp_away,
  drop column times_of_last_games_against_opp_home,

  drop column dk_fantasy_points_last_games_against_opp_away,
  drop column dk_fantasy_points_last_games_against_opp_home,

  drop column seconds_played_last_games_against_opp_away,
  drop column seconds_played_last_games_against_opp_home,

  drop column plus_minus_last_games,
  drop column plus_minus_last_games_against_opp_away,
  drop column plus_minus_last_games_against_opp_home,

  drop column dk_fantasy_points_per_minute,
  drop column dk_fantasy_points_per_minute_last_games,
  drop column dk_fantasy_points_per_minute_last_games_against_opp_away,
  drop column dk_fantasy_points_per_minute_last_games_against_opp_home,

  drop column opp_dk_fantasy_points_allowed_vs_position_last_games,
  drop column opp_dk_fantasy_points_allowed_vs_position_last_games_home,
  drop column opp_dk_fantasy_points_allowed_vs_position_last_games_away;

create table games_computed (
  id serial not null primary key,
  created_at timestamp not null default now(),
  updated_at timestamp not null default now(),

  game_basketball_reference_id text references games(basketball_reference_id) on delete cascade,
  away_starters jsonb default '[]',
  home_starters jsonb default '[]',

  away_wins int,
  away_losses int,
  home_wins int,
  home_losses int,
  away_dk_fantasy_points_allowed real,
  home_dk_fantasy_points_allowed real,
  away_dk_fantasy_points_allowed_last_games jsonb default '[]',
  home_dk_fantasy_points_allowed_last_games jsonb default '[]'
);

create trigger update_updated_at before update
  on games_computed for each row execute procedure update_updated_at();

alter table games_players
  drop column plus_minus;

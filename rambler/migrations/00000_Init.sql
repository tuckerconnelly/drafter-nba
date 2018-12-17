-- rambler up

-- Every table has
-- * id
-- * created_at
-- * updated_at
--
-- Foreign keys use `table_name_id` format, with `table_name` in the plural. Examples:
-- * workspaces_id
-- * users_id
--
-- Enums are text fields with constraints, which are much easier to update than traditional enums

create or replace function update_updated_at() returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language 'plpgsql';

create table teams (
  id serial not null primary key,
  created_at timestamp not null default now(),
  updated_at timestamp not null default now(),

  basketball_reference_id text unique,
  name text
);

create trigger update_updated_at before update
  on teams for each row execute procedure update_updated_at();

create table players (
  id serial not null primary key,
  created_at timestamp not null default now(),
  updated_at timestamp not null default now(),

  basketball_reference_id text unique,
  name text,
  date_of_birth date,
  birth_country text
);

create trigger update_updated_at before update
  on players for each row execute procedure update_updated_at();

create table teams_players (
  id serial not null primary key,
  created_at timestamp not null default now(),
  updated_at timestamp not null default now(),

  team_basketball_reference_id text references teams(basketball_reference_id),
  player_basketball_reference_id text references players(basketball_reference_id),

  season int,
  player_number int,
  position text,
  height_inches int,
  weight_lbs int,
  experience int
);

create trigger update_updated_at before update
  on teams_players for each row execute procedure update_updated_at();

create table games (
  id serial not null primary key,
  created_at timestamp not null default now(),
  updated_at timestamp not null default now(),

  season int,
  basketball_reference_id text unique,
  home_team_basketball_reference_id text references teams(basketball_reference_id),
  away_team_basketball_reference_id text references teams(basketball_reference_id),
  home_score int,
  away_score int,
  arena text,
  time_of_game timestamp without time zone
);

create trigger update_updated_at before update
  on games for each row execute procedure update_updated_at();

create table games_players (
  id serial not null primary key,
  created_at timestamp not null default now(),
  updated_at timestamp not null default now(),

  player_basketball_reference_id text references players(basketball_reference_id),
  game_basketball_reference_id text references games(basketball_reference_id),

  starter boolean,
  seconds_played int,
  field_goals int,
  field_goals_attempted int,
  three_point_field_goals int,
  three_point_field_goals_attempted int,
  free_throws int,
  free_throws_attempted int,
  offensive_rebounds int,
  defensive_rebounds int,
  total_rebounds int,
  assists int,
  steals int,
  blocks int,
  turnovers int,
  personal_fouls int,
  points int
);

create trigger update_updated_at before update
  on games_players for each row execute procedure update_updated_at();

-- rambler down

drop table games_players;
drop table games;
drop table teams_players;
drop table players;
drop table teams;

drop function update_updated_at;

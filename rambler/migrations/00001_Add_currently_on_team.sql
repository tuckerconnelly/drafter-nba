-- rambler up

alter table teams_players
  add column currently_on_this_team boolean;

update teams_players set currently_on_this_team = true;


-- rambler down

alter table teams_players
  drop column currently_on_this_team;

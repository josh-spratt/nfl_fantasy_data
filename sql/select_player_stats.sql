SELECT
    player_ids."name"
  , player_ids."position"
  , seasonal_stats.season
  , seasonal_stats.completions
  , seasonal_stats.attempts
  , seasonal_stats.passing_yards
  , seasonal_stats.passing_tds
  , seasonal_stats.interceptions
  , seasonal_stats.sacks
  , seasonal_stats.sack_yards
  , seasonal_stats.sack_fumbles
  , seasonal_stats.sack_fumbles_lost
  , seasonal_stats.passing_2pt_conversions
  , seasonal_stats.carries
  , seasonal_stats.rushing_yards
  , seasonal_stats.rushing_tds
  , seasonal_stats.rushing_fumbles
  , seasonal_stats.rushing_fumbles_lost
  , seasonal_stats.rushing_2pt_conversions
  , seasonal_stats.receptions
  , seasonal_stats.targets
  , seasonal_stats.receiving_yards
  , seasonal_stats.receiving_tds
  , seasonal_stats.receiving_fumbles
  , seasonal_stats.receiving_fumbles_lost
  , seasonal_stats.receiving_2pt_conversions
  , seasonal_stats.target_share
  , seasonal_stats.fantasy_points
  , seasonal_stats.fantasy_points_ppr
  , seasonal_stats.games
FROM seasonal_stats
INNER JOIN player_ids
ON seasonal_stats.player_id = player_ids.gsis_id
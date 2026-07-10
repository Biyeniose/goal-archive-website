STAT_COLUMNS: set[str] = {
    "goals",
    "assists",
    "goals_assists",
    "goals_p90",
    "assists_p90",
    "goals_assists_p90",
    "shots",
    "minutes",
    "gp",
    "mpg",
    "clean_sheets",
    "goals_conceded",
    "goals_conceded_p90",
    "penalty_goals",
    "pens_att",
    "cards_yellow",
    "cards_red",
}

BYDATE_STAT_COLUMNS: set[str] = {
    "goals",
    "assists",
    "goals_assists",
    "goals_p90",
    "assists_p90",
    "goals_assists_p90",
    "shots",
    "minutes",
    "gp",
    "pens_made",
    "pens_att",
}

DEFAULT_LEAGUE_IDS: list[int] = [1, 2, 3, 4, 5, 99, 66, 33, 81]

POSITION_GROUPS: dict = {
    "defender": ["Left-Back", "Right-Back", "Centre-Back", "Sweeper"],
    "midfielder": ["Central Midfield", "Defensive Midfield", "Attacking Midfield"],
    "wingers": ["Right Winger", "Left Winger", "Left Midfield", "Right Midfield"],
    "forwards": ["Centre-Forward", "Second Striker"],
}

# Maps the canonical team name (teams.name) to additional short-form aliases
# used when matching market names from Kalshi / Polymarket against team names.
# Add entries here when a prediction API uses a shortened or alternate team name.
TEAM_NAME_ALIASES: dict[str, list[str]] = {
    "Nottingham Forest": ["Nottingham"],
}

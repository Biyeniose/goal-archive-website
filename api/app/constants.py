STAT_COLUMNS: set[str] = {
    "goals", "assists", "goals_assists",
    "goals_p90", "assists_p90", "goals_assists_p90",
    "shots", "minutes", "gp", "mpg",
    "clean_sheets", "goals_conceded", "goals_conceded_p90",
    "penalty_goals", "pens_att", "cards_yellow", "cards_red",
}

BYDATE_STAT_COLUMNS: set[str] = {
    "goals", "assists", "goals_assists",
    "goals_p90", "assists_p90", "goals_assists_p90",
    "shots", "minutes", "gp", "pens_made", "pens_att",
}

DEFAULT_LEAGUE_IDS: list[int] = [1,2,3,4,5,7,8, 111]

DEFAULT_SEASON_YEAR: int = 2025

POSITION_GROUPS: dict = {
    "defender":   ["Left-Back", "Right-Back", "Centre-Back", "Sweeper"],
    "midfielder": ["Central Midfield", "Defensive Midfield", "Attacking Midfield"],
    "wingers":    ["Right Winger", "Left Winger", "Left Midfield", "Right Midfield"],
    "forwards":   ["Centre-Forward", "Second Striker"],
}

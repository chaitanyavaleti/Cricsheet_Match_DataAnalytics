-- =========================================================
-- Cricsheet MySQL Analysis Queries
-- Author: Your Name
-- Purpose: Player, Team, and Match Insights
-- =========================================================

-- 1. Top 10 Run Scorers (all formats)
SELECT striker, SUM(runs_batter) AS total_runs
FROM deliveries
GROUP BY striker
ORDER BY total_runs DESC
LIMIT 10;

-- 2. Top 10 Wicket Takers
SELECT bowler, COUNT(player_dismissed) AS wickets
FROM deliveries
WHERE player_dismissed IS NOT NULL
GROUP BY bowler
ORDER BY wickets DESC
LIMIT 10;

-- 3. Total Matches Played per Team
SELECT team_name, COUNT(*) AS matches_played
FROM (
    SELECT team1 AS team_name FROM matches
    UNION ALL
    SELECT team2 FROM matches
) AS all_teams
GROUP BY team_name
ORDER BY matches_played DESC;

-- 4. Team Wins Count
SELECT winner, COUNT(*) AS wins
FROM matches
WHERE winner IS NOT NULL
GROUP BY winner
ORDER BY wins DESC;

-- 5. Win % by Team
SELECT winner AS team, 
       COUNT(*) / (SELECT COUNT(*) FROM matches) * 100 AS win_percentage
FROM matches
GROUP BY winner;

-- 6. Average Runs per Match (all formats)
SELECT AVG(total_runs) AS avg_runs
FROM innings;

-- 7. Highest Team Score
SELECT team, MAX(total_runs) AS highest_score
FROM innings
GROUP BY team
ORDER BY highest_score DESC
LIMIT 1;

-- 8. Matches Decided by Margin of < 10 Runs
SELECT match_id, winner, venue
FROM matches
WHERE result_margin < 10 AND result_type = 'runs';

-- 9. Toss Win vs Match Win
SELECT toss_winner, COUNT(*) AS matches_won_after_toss
FROM matches
WHERE toss_winner = winner
GROUP BY toss_winner;

-- 10. Most Common Dismissal Type
SELECT dismissal_kind, COUNT(*) AS dismissals
FROM deliveries
WHERE dismissal_kind IS NOT NULL
GROUP BY dismissal_kind
ORDER BY dismissals DESC;

-- 11. Sixes per Player
SELECT striker, COUNT(*) AS sixes
FROM deliveries
WHERE runs_batter = 6
GROUP BY striker
ORDER BY sixes DESC
LIMIT 10;

-- 12. Fours per Player
SELECT striker, COUNT(*) AS fours
FROM deliveries
WHERE runs_batter = 4
GROUP BY striker
ORDER BY fours DESC
LIMIT 10;

-- 13. Strike Rate by Player (min 100 balls faced)
SELECT striker, 
       SUM(runs_batter) * 100.0 / COUNT(*) AS strike_rate
FROM deliveries
GROUP BY striker
HAVING COUNT(*) >= 100
ORDER BY strike_rate DESC
LIMIT 10;

-- 14. Economy Rate by Bowler (min 100 balls bowled)
SELECT bowler, 
       SUM(runs_total) * 6.0 / COUNT(*) AS economy
FROM deliveries
GROUP BY bowler
HAVING COUNT(*) >= 100
ORDER BY economy ASC
LIMIT 10;

-- 15. Matches per Venue
SELECT venue, COUNT(*) AS matches
FROM matches
GROUP BY venue
ORDER BY matches DESC;

-- 16. Player of Match Frequency
SELECT player_of_match, COUNT(*) AS awards
FROM matches
GROUP BY player_of_match
ORDER BY awards DESC
LIMIT 10;

-- 17. Yearly Runs Trend
SELECT YEAR(date) AS year, SUM(runs_total) AS total_runs
FROM matches m
JOIN deliveries d ON m.match_id = d.match_id
GROUP BY YEAR(date)
ORDER BY year;

-- 18. Format Comparison (Avg Runs by Match Type)
SELECT format, AVG(total_runs) AS avg_runs
FROM matches m
JOIN innings i ON m.match_id = i.match_id
GROUP BY format;

-- 19. Super Over Matches
SELECT match_id, venue, date
FROM matches
WHERE super_over = 'Y';

-- 20. Win % Batting First vs Chasing
SELECT batting_first, 
       COUNT(*) AS matches, 
       SUM(CASE WHEN winner = batting_first THEN 1 ELSE 0 END) / COUNT(*) * 100 AS win_pct
FROM (
    SELECT match_id, 
           team1 AS batting_first, 
           winner
    FROM matches
    WHERE toss_decision = 'bat'
    UNION ALL
    SELECT match_id, 
           team2 AS batting_first, 
           winner
    FROM matches
    WHERE toss_decision = 'field'
) AS t
GROUP BY batting_first;
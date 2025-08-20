-- 📘 Cricsheet Analysis SQL Queries (MySQL)
-- Database: cricket_analysis_db

-- 1. Top 10 run scorers across all matches
SELECT batter, SUM(runs_batter) AS total_runs
FROM deliveries
GROUP BY batter
ORDER BY total_runs DESC
LIMIT 10;

-- 2. Top 10 wicket takers
SELECT bowler, COUNT(*) AS total_wickets
FROM deliveries
WHERE wicket_kind IS NOT NULL
GROUP BY bowler
ORDER BY total_wickets DESC
LIMIT 10;

-- 3. Match results by team (wins count)
SELECT winner, COUNT(*) AS wins
FROM matches
WHERE winner IS NOT NULL
GROUP BY winner
ORDER BY wins DESC;

-- 4. Total runs scored by each team
SELECT batting_team, SUM(runs_total) AS runs_scored
FROM deliveries
GROUP BY batting_team
ORDER BY runs_scored DESC;

-- 5. Average runs per match by format
SELECT m.match_type, AVG(t.runs_scored) AS avg_runs_per_match
FROM (
    SELECT match_id, SUM(runs_total) AS runs_scored
    FROM deliveries
    GROUP BY match_id
) t
JOIN matches m ON t.match_id = m.match_id
GROUP BY m.match_type;

-- 6. Most common dismissal types
SELECT wicket_kind, COUNT(*) AS dismissals
FROM deliveries
WHERE wicket_kind IS NOT NULL
GROUP BY wicket_kind
ORDER BY dismissals DESC;

-- 7. Win percentage by team
SELECT team,
       CONCAT(ROUND(100.0 * SUM(CASE WHEN winner = team THEN 1 ELSE 0 END) / COUNT(*), 2), '%') AS win_percentage
FROM (
    SELECT team1 AS team, winner FROM matches
    UNION ALL
    SELECT team2 AS team, winner FROM matches
) t
GROUP BY team
ORDER BY win_percentage DESC;

-- 8. Boundary count (fours and sixes) by player
SELECT batter,
       SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END) AS fours,
       SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END) AS sixes
FROM deliveries
GROUP BY batter
ORDER BY sixes DESC, fours DESC;

-- 9. Highest run matches
SELECT m.match_id, m.match_type, SUM(d.runs_total) AS total_runs
FROM deliveries d
JOIN matches m ON d.match_id = m.match_id
GROUP BY m.match_id, m.match_type
ORDER BY total_runs DESC
LIMIT 5;

-- 10. Players with most appearances (batting)
SELECT batter, COUNT(DISTINCT match_id) AS matches_played
FROM deliveries
GROUP BY batter
ORDER BY matches_played DESC
LIMIT 10;

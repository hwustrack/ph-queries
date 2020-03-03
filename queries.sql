SELECT * FROM posts;
SELECT * FROM topics;
SELECT * FROM post_topics;

-- DELETE FROM posts;
-- DELETE FROM topics;
-- DELETE FROM post_topics;

SELECT COUNT(*) from post_topics WHERE topic_id = 2

SELECT t.name, count(m.topic_id) count, sum(p.votesCount) votes, 
    sum(p.votesCount) / count(m.topic_id) norm
FROM post_topics m 
JOIN topics t ON t.rowid = m.topic_id
JOIN posts p on p.rowid = m.post_id
GROUP BY m.topic_id
ORDER BY norm DESC
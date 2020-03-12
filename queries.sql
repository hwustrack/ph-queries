SELECT * FROM posts LIMIT 5;
SELECT * FROM topics LIMIT 5;
SELECT * FROM post_topics LIMIT 5;

SELECT count(*) FROM posts;
SELECT count(*) FROM topics;
SELECT count(*) FROM post_topics;

-- DELETE FROM posts;
-- DELETE FROM topics;
-- DELETE FROM post_topics;

-- topics with most average votes
SELECT p.id, t.name, count(m.topic_id) count, sum(p.votes_count) votes, 
    round(avg(p.votes_count), 2) avg
FROM post_topics m 
JOIN topics t ON t.rowid = m.topic_id
JOIN posts p on p.rowid = m.post_id
GROUP BY m.topic_id
HAVING count > 9
ORDER BY avg DESC
LIMIT 20;

-- topic that appears most often in a month
SELECT t.name, m.topic_id, count(m.topic_id) count
FROM post_topics m 
JOIN topics t ON t.rowid = m.topic_id
JOIN posts p on p.rowid = m.post_id
-- WHERE p.createdAt < "2019-02-01T00:00:00Z" and p.createdAt > "2019-01-01T00:00:00Z"
GROUP BY m.topic_id
ORDER BY count DESC
LIMIT 20;

SELECT id, name, votes_count
FROM posts
ORDER BY votes_count DESC
LIMIT 20;


SELECT t.name, p.votes_count votes
FROM post_topics m 
JOIN topics t ON t.rowid = m.topic_id
JOIN posts p on p.rowid = m.post_id
LIMIT 20;
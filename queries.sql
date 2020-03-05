SELECT * FROM posts LIMIT 20;
SELECT * FROM topics LIMIT 5;
SELECT * FROM post_topics LIMIT 5;

SELECT count(*) FROM posts;
SELECT count(*) FROM topics;
SELECT count(*) FROM post_topics;

DELETE FROM posts;
DELETE FROM topics;
DELETE FROM post_topics;

-- CREATE UNIQUE INDEX idx_posts_id ON posts(id);

SELECT * FROM post_topics m
JOIN topics t on t.rowid = m.topic_id
JOIN posts p on p.rowid = m.post_id
LIMIT 5;

SELECT t.name, count(m.topic_id) count, sum(p.votes_count) votes, 
    sum(p.votes_count) / count(m.topic_id) norm
FROM post_topics m 
JOIN topics t ON t.rowid = m.topic_id
JOIN posts p on p.rowid = m.post_id
GROUP BY m.topic_id
ORDER BY norm DESC
LIMIT 20;

SELECT t.name, count(m.topic_id) count, sum(p.votes_count) votes
FROM post_topics m 
JOIN topics t ON t.rowid = m.topic_id
JOIN posts p on p.rowid = m.post_id
GROUP BY m.topic_id
ORDER BY count ASC, votes DESC 
LIMIT 20;
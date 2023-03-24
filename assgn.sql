CREATE STREAM users_stream (id INT, firstName VARCHAR, lastName VARCHAR)
    WITH (kafka_topic='users', value_format='JSON');

CREATE STREAM users_name_stream AS 
    SELECT id, firstName, lastName 
    FROM users_stream
    EMIT CHANGES;
	
CREATE STREAM users_age_stream AS 
    SELECT * 
    FROM users_stream 
    WHERE gender = 'male'
    EMIT CHANGES;
	
CREATE MATERIALIZED VIEW people_with_generation AS
SELECT id, 
  CASE 
    WHEN age < 26 THEN 'Gen Z'
    WHEN age BETWEEN 27 AND 42 THEN 'Millennials'
    WHEN age BETWEEN 43 AND 58 THEN 'Gen X'
    WHEN age > 59 THEN 'Boomers'
    ELSE NULL
  END AS generation
FROM people;
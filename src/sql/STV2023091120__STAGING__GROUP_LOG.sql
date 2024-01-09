DROP TABLE IF EXISTS STV2023091120__STAGING.group_log;

CREATE TABLE STV2023091120__STAGING.group_log
(
    group_id BIGINT PRIMARY KEY,
    user_id BIGINT,
    user_id_from BIGINT,
    group_event VARCHAR,
    event_timestamp TIMESTAMP
)
ORDER BY user_id, user_id_from, group_id
SEGMENTED BY HASH(group_id) ALL NODES;

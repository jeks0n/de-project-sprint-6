DROP TABLE IF EXISTS STV2023091120__DWH.s_auth_history;

CREATE TABLE STV2023091120__DWH.s_auth_history (
    hk_l_user_group_activity BIGINT NOT NULL CONSTRAINT s_auth_history_hk_l_user_group_activity_fkey REFERENCES STV2023091120__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from BIGINT,
    group_event VARCHAR,
    event_dt TIMESTAMP,
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY hk_l_user_group_activity
SEGMENTED BY HASH(hk_l_user_group_activity) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);

INSERT INTO STV2023091120__DWH.s_auth_history (
    hk_l_user_group_activity,
    user_id_from,
    group_event,
    event_dt,
    load_dt,
    load_src
)
SELECT
    luga.hk_l_user_group_activity,
    gl.user_id_from,
    gl.group_event,
    gl.event_timestamp AS event_dt,
    NOW() AS load_dt,
    's3' AS load_src
FROM STV2023091120__STAGING.group_log AS gl
LEFT JOIN STV2023091120__DWH.h_groups AS hg ON gl.group_id = hg.group_id
LEFT JOIN STV2023091120__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN STV2023091120__DWH.l_user_group_activity AS luga ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id;

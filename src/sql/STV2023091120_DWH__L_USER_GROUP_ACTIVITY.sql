DROP TABLE IF EXISTS STV2023091120__DWH.l_user_group_activity CASCADE;

CREATE TABLE STV2023091120__DWH.l_user_group_activity (
    hk_l_user_group_activity BIGINT PRIMARY KEY,
    hk_user_id BIGINT CONSTRAINT l_user_group_activity_hk_user_id_fkey REFERENCES STV2023091120__DWH.h_users(hk_user_id),
    hk_group_id BIGINT CONSTRAINT l_user_group_activity_hk_group_id_fkey REFERENCES STV2023091120__DWH.h_groups(hk_group_id),
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY hk_user_id, hk_group_id, hk_l_user_group_activity
SEGMENTED BY HASH(hk_l_user_group_activity) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);

INSERT INTO STV2023091120__DWH.l_user_group_activity (
    hk_l_user_group_activity,
    hk_user_id,
    hk_group_id,
    load_dt,
    load_src
)
SELECT DISTINCT
    HASH(hu.user_id, hg.group_id) AS hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    NOW() AS load_dt,
    's3' AS load_src
FROM STV2023091120__STAGING.group_log gl
LEFT JOIN STV2023091120__DWH.h_users hu ON hu.user_id = gl.user_id
LEFT JOIN STV2023091120__DWH.h_groups hg ON hg.group_id = gl.group_id
WHERE HASH(hu.user_id, hg.group_id) NOT IN (
    SELECT HASH(g.hk_l_user_group_activity)
    FROM STV2023091120__DWH.l_user_group_activity g
);

-- Шаг 7.1. Подготовить CTE user_group_messages
WITH user_group_messages AS (
    SELECT
        hg.hk_group_id,
        COUNT(DISTINCT hu.hk_user_id) AS cnt_users_in_group_with_messages
    FROM STV2023091120__DWH.h_groups hg
    INNER JOIN STV2023091120__DWH.l_groups_dialogs gd ON hg.hk_group_id = gd.hk_group_id
    INNER JOIN STV2023091120__DWH.l_user_message um ON um.hk_message_id = gd.hk_message_id
    INNER JOIN STV2023091120__DWH.h_users hu ON hu.hk_user_id = um.hk_user_id
    GROUP BY hg.hk_group_id
)

SELECT
    hk_group_id,
    cnt_users_in_group_with_messages
FROM user_group_messages
ORDER BY cnt_users_in_group_with_messages
LIMIT 10;


-- Шаг 7.2. Подготовить CTE user_group_log
WITH user_group_log AS (
    SELECT
        hg.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM (
        SELECT
            hk_group_id
        FROM STV2023091120__DWH.h_groups
        ORDER BY registration_dt ASC
        LIMIT 10
    ) hg
    INNER JOIN STV2023091120__DWH.l_user_group_activity luga ON luga.hk_group_id = hg.hk_group_id
    INNER JOIN STV2023091120__DWH.s_auth_history ah ON ah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    LEFT JOIN STV2023091120__DWH.l_user_message um ON um.hk_user_id = luga.hk_user_id
    WHERE ah.group_event = 'add' AND um.hk_user_id IS NULL
    GROUP BY hg.hk_group_id
)

SELECT
    hk_group_id,
    cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users
LIMIT 10;


-- Шаг 7.3. Написать запрос и ответить на вопрос бизнеса
WITH user_group_messages AS (
    SELECT
        hg.hk_group_id,
        COUNT(DISTINCT hu.hk_user_id) AS cnt_users_in_group_with_messages
    FROM STV2023091120__DWH.h_groups hg
    INNER JOIN STV2023091120__DWH.l_groups_dialogs gd ON hg.hk_group_id = gd.hk_group_id
    INNER JOIN STV2023091120__DWH.l_user_message um ON um.hk_message_id = gd.hk_message_id
    INNER JOIN STV2023091120__DWH.h_users hu ON hu.hk_user_id = um.hk_user_id
    GROUP BY hg.hk_group_id
),
user_group_log AS (
    SELECT
        hg.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM (
        SELECT
            hk_group_id
        FROM STV2023091120__DWH.h_groups
        ORDER BY registration_dt ASC
        LIMIT 10
    ) hg
    INNER JOIN STV2023091120__DWH.l_user_group_activity luga ON luga.hk_group_id = hg.hk_group_id
    INNER JOIN STV2023091120__DWH.s_auth_history ah ON ah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    LEFT JOIN STV2023091120__DWH.l_user_message um ON um.hk_user_id = luga.hk_user_id
    WHERE ah.group_event = 'add'
        AND um.hk_user_id IS NULL
    GROUP BY hg.hk_group_id
)

SELECT
    ugl.hk_group_id,
    ugl.cnt_added_users,
    ugm.cnt_users_in_group_with_messages,
    ugl.cnt_added_users / NULLIF(ugm.cnt_users_in_group_with_messages, 0) AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC;

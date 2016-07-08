SELECT B.id, jsonb_build_object( 'id', B.id )::jsonb AS feature FROM
(
    SELECT (rec).pos, (rec).id as id  FROM (
        SELECT search_json_index('${collection}', '${lang}', ${intersection}, '${query}'::jsonb) AS rec
    ) AS T
) AS B
ORDER BY id
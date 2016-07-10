SELECT jsonb_build_object( 'id', A.id, 'pos', A.pos)::jsonb AS feature  FROM
(
    SELECT (rec).pos - 1 as pos, (rec).id as id  FROM (
        SELECT search_json_index('${collection}', '${lang}', '${query}'::jsonb) AS rec
    ) AS T
) AS A
WHERE A.id IN (${ids})
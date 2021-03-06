SELECT B.* FROM 
(
    SELECT (rec).pos, (rec).id as id  FROM (
        SELECT search_json_index('${collection}', '${lang}', '${query}'::jsonb) AS rec
    ) AS T
) AS A LEFT JOIN (
    SELECT
        id AS _id,
        ${projection}
        geometry AS geometry
    FROM collection_${collection}_${lang}
) AS B ON A.id = B._id
ORDER BY ${order} _id
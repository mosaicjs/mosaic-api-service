SELECT A.id AS id, jsonb_build_object( 'id', B.id, 'type', 'Feature', 'properties', B.properties, 'geometry', B.geometry)::jsonb AS feature FROM
(
    SELECT (rec).pos, (rec).id as id  FROM (
        SELECT search_json_index('${collection}', '${lang}', '${query}'::jsonb) AS rec
    ) AS T
) AS A LEFT JOIN (
    SELECT id, properties, ST_AsGeoJson(geometry)::jsonb AS geometry FROM collection_${collection}_${lang}
) AS B ON A.id = B.id
ORDER BY id
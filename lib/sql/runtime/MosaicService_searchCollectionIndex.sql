SELECT A.id AS id, jsonb_build_object( 'id', B.id, 'type', 'Feature', 'properties', B.properties, 'geometry', B.geometry)::jsonb AS feature FROM
(
    SELECT I.*
    FROM ${collection}_view_${index}_${lang} AS I
    WHERE I.document @@ to_tsquery('${query}')
) AS A LEFT JOIN (
    SELECT id, properties, ST_AsGeoJson(geometry)::jsonb AS geometry FROM ${collection}
) AS B ON A.id = B.id
ORDER BY id
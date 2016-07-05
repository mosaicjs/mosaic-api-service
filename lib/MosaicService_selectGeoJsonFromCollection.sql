SELECT jsonb_build_object( 'id', id, 'type', 'Feature', 'properties', properties, 'geometry', geometry ) as feature
FROM (
    SELECT T.id, T.properties, ST_AsGeoJson(T.geometry)::jsonb AS geometry 
    FROM ${collection}  as T
) as N
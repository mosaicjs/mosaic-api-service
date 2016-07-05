SELECT jsonb_build_object( 'id', id ) as feature
FROM (
    SELECT T.id, T.properties, ST_AsGeoJson(T.geometry)::jsonb AS geometry 
    FROM ${collection}  as T
) as N
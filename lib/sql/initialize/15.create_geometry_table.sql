CREATE OR REPLACE FUNCTION create_geometry_table(t varchar(255)) RETURNS jsonb AS $$
    var sql = '' + 
    'CREATE TABLE IF NOT EXISTS ' + t + '(' + 
        'id UUID PRIMARY KEY DEFAULT gen_random_uuid(), ' +
        'geometry geometry, ' +
        'CHECK(st_srid(geometry) = 4326) ' +
    ')';
    plv8.execute(sql);
    plv8.execute('CREATE INDEX IF NOT EXISTS ' + t + '_idx ON ' + t + ' USING GIST(geometry)');
$$ LANGUAGE plv8 STRICT IMMUTABLE
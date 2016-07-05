CREATE OR REPLACE FUNCTION create_json_table(t varchar(255)) RETURNS jsonb AS $$
    var sql = '' + 
    'CREATE TABLE IF NOT EXISTS ' + t + '(' + 
        'id UUID PRIMARY KEY DEFAULT gen_random_uuid(), ' +
        'type varchar(255), ' +
        'properties jsonb ' +
    ')';
    return plv8.execute(sql);
$$ LANGUAGE plv8 STRICT IMMUTABLE
CREATE OR REPLACE FUNCTION create_json_index(tbl text, name text, lang text, fields jsonb) RETURNS boolean AS $$
    var viewName = tbl + '_view_' + name + '_' + lang;
    plv8.execute('DROP MATERIALIZED VIEW IF EXISTS ' + viewName + ' CASCADE');
    plv8.execute('CREATE MATERIALIZED VIEW ' + viewName + ' AS ' +
       "SELECT id, convert_to_tsvector(properties, \'" + 
       lang + "', '" + 
       JSON.stringify(fields) +
       "'::jsonb) AS document FROM " + 
       tbl
    );
    plv8.execute('CREATE INDEX ON ' + viewName + ' USING gist(document)');
    return true;
$$ LANGUAGE plv8 STRICT IMMUTABLE

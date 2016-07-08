CREATE OR REPLACE FUNCTION create_json_index(tbl text, name text, lang text, fields jsonb) RETURNS boolean AS $$
    lang = lang || 'default';
    var viewName = 'collection_' + tbl + '_' + lang;
    var collectionName = viewName + '_json';
    var ftsViewName = 'fts_' + tbl + '_' + lang + '_' + name;
    plv8.execute('DROP MATERIALIZED VIEW IF EXISTS ' + ftsViewName + ' CASCADE');
    plv8.execute('CREATE MATERIALIZED VIEW ' + ftsViewName + ' AS ' +
       "SELECT id, convert_to_tsvector(properties, \'" + 
       lang + "', '" + 
       JSON.stringify(fields) +
       "'::jsonb) AS document FROM " + 
       collectionName
    );
    plv8.execute('CREATE INDEX ON ' + ftsViewName + ' USING gist(document)');
    return true;
$$ LANGUAGE plv8 STRICT IMMUTABLE

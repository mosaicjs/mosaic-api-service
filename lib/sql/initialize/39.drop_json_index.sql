CREATE OR REPLACE FUNCTION drop_json_index(tbl text, name text, lang text) RETURNS boolean AS $$
    var ftsViewName = 'fts_' + tbl + '_' + lang + '_' + name;
    plv8.execute('DROP MATERIALIZED VIEW IF EXISTS ' + ftsViewName + ' CASCADE');
    return true;
$$ LANGUAGE plv8 STRICT IMMUTABLE

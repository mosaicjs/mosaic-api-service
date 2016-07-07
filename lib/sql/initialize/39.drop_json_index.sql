CREATE OR REPLACE FUNCTION drop_json_index(tbl text, name text, lang text) RETURNS boolean AS $$
    var viewName = tbl + '_view_' + name + '_' + lang;
    plv8.execute('DROP MATERIALIZED VIEW IF EXISTS ' + viewName + ' CASCADE');
    return true;
$$ LANGUAGE plv8 STRICT IMMUTABLE

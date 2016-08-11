CREATE OR REPLACE FUNCTION create_json_index(tbl text, name text, lang text, fields jsonb) RETURNS boolean AS $$
    lang = lang || 'default';
    var viewName = 'collection_' + tbl + '_' + lang;
    var collectionName = viewName + '_json';
    var ftsViewName = 'fts_' + tbl + '_' + lang + '_' + name;
    plv8.execute('DROP MATERIALIZED VIEW IF EXISTS ' + ftsViewName + ' CASCADE');
    
    var json_field_selector = plv8.find_function('json_field_selector');
    
    var queries = [];
    var fieldsInfo = [];
    Object.keys(fields).forEach(function(field, i){
        var info = fields[field];
        if (typeof info !== 'object') {
            info = {
                weight : info
            };
        }
        info.field = field;
        fieldsInfo.push(info);

        var x = json_field_selector(info);
        var expr = "COALESCE(" + x.field + ",'{}'::jsonb)";
        expr = "to_string(" + expr + ",',')::text";
        queries.push(expr);
    });    
    
    var sql = 'SELECT id, (' + queries.join('||') + ') AS txt FROM ' + collectionName;   
    sql = "SELECT id, to_tsvector('" + lang + "'::regconfig, unaccent(txt)||' '||txt) AS document FROM (" + sql + ") AS T";
    // plv8.elog(NOTICE, sql);
    sql = 'CREATE MATERIALIZED VIEW ' + ftsViewName + ' AS ' + sql;
    plv8.execute(sql);
    
    plv8.execute('CREATE INDEX ON ' + ftsViewName + ' USING gist(document)');
    return true;
$$ LANGUAGE plv8 STRICT IMMUTABLE

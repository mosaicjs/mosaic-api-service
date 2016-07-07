CREATE OR REPLACE FUNCTION search_json_index(tbl text, lang text, queries jsonb) RETURNS SETOF json_result_rec AS $$
    var viewAliases = []; 
    var array = [];
    Object.keys(queries).forEach(function(idxName, i){
        var viewAlias = 'T' + i;
        viewAliases.push(viewAlias);
        var line = getSqlLine(idxName, queries[idxName]);
        line = '(' + line + ') AS ' + viewAlias;
        array.push(line);
    });
    // "OR" logic
    var sql = array.join(' UNION ALL ');
    sql = 'SELECT ROW_NUMBER() OVER (ORDER BY id) AS pos, id FROM (' +
      ' SELECT distinct(id) AS id FROM ' + sql + ') AS A';
    // plv8.elog(NOTICE, sql);
    
    return plv8.execute(sql);
    
    function getSqlLine(idxName, query){
        var viewName = tbl + '_view_' + idxName + '_' + lang;
        return 'SELECT id AS id FROM ' + viewName + ' ' + 
        "WHERE document @@ to_tsquery('" + query + "')";
    }
$$ LANGUAGE plv8 STRICT IMMUTABLE
-- select (rec).pos, (rec).id as id from ( select search_json_index('organizations', 'french', '{"q":"web"}'::jsonb) as rec ) as N


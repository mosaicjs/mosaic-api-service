CREATE OR REPLACE FUNCTION search_json_index(tbl text, lang text, intersection boolean, queries jsonb) RETURNS SETOF json_result_rec AS $$
    var array = [];
    Object.keys(queries).forEach(function(idxName, i){
        var line = getSqlLine(idxName, queries[idxName]);
        line = '(' + line + ')';
        array.push(line);
    });

        // "AND" logic
    var sql; 
    if (intersection) {
        var where = [];
        sql = array.map(function(line, i){
            if (i > 0) {
                where.push('(T0.id=T' + i + '.id)');
            }
            return line + ' AS T' + i;
        }).join(', ');
        if (where.length){
            sql += ' WHERE ' + where.join(' AND ');   
        }
        sql = 'SELECT T0.id FROM ' + sql; 
        sql = 'SELECT ROW_NUMBER() OVER (ORDER BY id) AS pos, id FROM (' + sql + ') AS B';
    } else {

        // "OR" logic
        sql = array.join(' UNION ALL ');
        // FIXME: add GROUP BY id here and order everything by the number of results (?) 
        sql = 'SELECT ROW_NUMBER() OVER (ORDER BY id) AS pos, id FROM (' +
          ' SELECT distinct(id) AS id FROM (' + sql + ') AS A) AS B';
    }
    // plv8.elog(NOTICE, sql);
    
    return plv8.execute(sql);
    
    function getSqlLine(idxName, query){
        var viewName = tbl + '_view_' + idxName + '_' + lang;
        var sql = 'SELECT id AS id FROM ' + viewName;
        if (query) {
            sql += " WHERE document @@ to_tsquery('" + query + "')"; 
        }
        return sql;
    }
$$ LANGUAGE plv8 STRICT IMMUTABLE
CREATE OR REPLACE FUNCTION search_stats_for_json_index(tbl text, lang text, intersection boolean, queries jsonb) RETURNS INT AS $$
    var search_json_index_sql = plv8.find_function('search_json_index_sql');
    var sql = search_json_index_sql(tbl, lang, intersection, queries);
    sql = 'SELECT COUNT(*) AS size FROM (' + sql + ') AS T';
    // plv8.elog(NOTICE, sql);
    var size = plv8.execute(sql)[0].size; 
    return size;
$$ LANGUAGE plv8 STRICT IMMUTABLE
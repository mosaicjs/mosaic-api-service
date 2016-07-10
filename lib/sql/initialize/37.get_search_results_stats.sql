CREATE OR REPLACE FUNCTION get_search_results_stats(tbl varchar(255), lang varchar(255), queries jsonb, fields jsonb) RETURNS jsonb AS $$
    var get_geojson_stats = plv8.find_function('get_geojson_stats');
    var search_json_index_sql = plv8.find_function('search_json_index_sql');
    lang = lang || 'default';
    var viewName = 'collection_' + tbl + '_' + lang;
    var sql = search_json_index_sql(tbl, lang, queries);
    sql = '(SELECT N.id, N.properties FROM (' + sql + ') AS T, ' + viewName + ' AS N ' +
      'WHERE T.id=N.id) AS A'
    plv8.elog(NOTICE, sql);
    return get_geojson_stats(sql, fields);
$$ LANGUAGE plv8 STRICT IMMUTABLE
CREATE OR REPLACE FUNCTION search_json_index(tbl text, lang text, queries jsonb) RETURNS SETOF json_result_rec AS $$
    var search_json_index_sql = plv8.find_function('search_json_index_sql');
    var sql = search_json_index_sql(tbl, lang, queries);
    // plv8.elog(NOTICE, sql);
    return plv8.execute(sql);
$$ LANGUAGE plv8 STRICT IMMUTABLE
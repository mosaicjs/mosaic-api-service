SELECT EXISTS (
   SELECT 1
   FROM pg_class
   WHERE relkind in ('m', 'v') AND relname = '${view}' 
)
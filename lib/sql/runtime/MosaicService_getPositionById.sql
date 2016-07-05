

select rn, id, properties from (
    select row_number() over (order by properties->>'taxId') as rn, * from organizations_json
) as T
where properties->>'taxId' = '38883445900058'

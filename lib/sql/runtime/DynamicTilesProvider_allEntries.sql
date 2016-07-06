SELECT * FROM (
SELECT
    id AS _id,
    ${projection}
    geometry AS geometry
FROM ${collection}
) AS T
ORDER BY ${order} _id

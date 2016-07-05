SELECT * FROM (
SELECT
    id AS id,
    ${projection}
    geometry AS geometry
FROM ${collection}
) AS T
${order}

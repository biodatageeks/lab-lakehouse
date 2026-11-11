-- Homework S3 probe: ON VIOLATION FAIL UPDATE stops the update on the first bad row.
-- Also used to check Free Edition's "one active pipeline" limit (started while the
-- main pipeline is running).
CREATE OR REFRESH MATERIALIZED VIEW probe_fail_update (
  CONSTRAINT positive_amount EXPECT (amount > 0) ON VIOLATION FAIL UPDATE
)
AS SELECT CAST(amount AS DOUBLE) AS amount
FROM read_files('${landing}/orders', format => 'json', schemaHints => 'amount DOUBLE');

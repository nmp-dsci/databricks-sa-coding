-- Built-in AI functions: an LLM call from SQL, with no endpoint to deploy and
-- no model to register. Worth knowing before reaching for a custom model —
-- for classification, extraction or summarisation over a table, this is
-- usually the right answer and it is one function call.
--
-- These run against the workspace's pay-per-token foundation model endpoints,
-- so they work on Free Edition without any serving slot being consumed.

-- Classify free text into a fixed label set. Deterministic label vocabulary
-- passed as an array, which is what stops the model inventing a sixth category.
SELECT
  status,
  ai_classify(
    CONCAT('An order with status ', status, ' worth $', CAST(ROUND(amount) AS STRING)),
    ARRAY('needs_attention', 'routine')
  ) AS triage
FROM workspace.sa_coding.silver_events
LIMIT 10;

-- Generate a narrative summary of the mart. `ai_query` is the general-purpose
-- escape hatch: any prompt, any served model, one row per call.
SELECT ai_query(
  'databricks-meta-llama-3-3-70b-instruct',
  CONCAT(
    'In two sentences, describe the trend in this daily revenue series. ',
    'Data: ',
    (SELECT string_agg(CONCAT(CAST(event_date AS STRING), '=', CAST(revenue AS STRING)), '; ')
     FROM (SELECT event_date, revenue FROM workspace.sa_coding.gold_daily_revenue
           ORDER BY event_date DESC LIMIT 30))
  )
) AS summary;

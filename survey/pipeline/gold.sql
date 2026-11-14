-- Gold: small aggregates for charts and reports, written in SQL (same pipeline, other language).

CREATE MATERIALIZED VIEW survey.gold.respondents_by_country
COMMENT 'Respondents per year and country'
AS SELECT year, country, count(*) AS respondents
FROM survey.silver.responses
WHERE country IS NOT NULL
GROUP BY year, country;

CREATE MATERIALIZED VIEW survey.gold.language_trend
COMMENT 'Share of respondents who worked with a language, per year'
AS WITH answered AS (
  SELECT year, count(*) AS respondents FROM survey.silver.responses
  WHERE size(languages_worked) > 0 GROUP BY year
)
SELECT l.year, l.language, count(*) AS users, round(count(*) / a.respondents, 4) AS share
FROM survey.silver.languages l JOIN answered a ON l.year = a.year
WHERE l.kind = 'worked'
GROUP BY l.year, l.language, a.respondents;

CREATE MATERIALIZED VIEW survey.gold.salary_by_country
COMMENT 'Yearly pay in USD of employed respondents, countries with at least 200 answers'
AS SELECT year, country, count(*) AS respondents,
  round(percentile(comp_usd_yearly, 0.25)) AS p25,
  round(percentile(comp_usd_yearly, 0.5)) AS median,
  round(percentile(comp_usd_yearly, 0.75)) AS p75
FROM survey.silver.responses
WHERE is_employed AND comp_usd_yearly BETWEEN 1000 AND 1000000 AND country IS NOT NULL
GROUP BY year, country
HAVING count(*) >= 200;

CREATE MATERIALIZED VIEW survey.gold.ai_adoption
COMMENT 'Answers to "Do you use AI tools in your development process?" per year'
AS SELECT year, ai_use, count(*) AS respondents,
  round(count(*) / sum(count(*)) OVER (PARTITION BY year), 4) AS share
FROM survey.silver.responses
WHERE ai_use IS NOT NULL
GROUP BY year, ai_use;

CREATE MATERIALIZED VIEW survey.gold.remote_work
COMMENT 'Remote / hybrid / in-person share per year'
AS SELECT year, work_mode, count(*) AS respondents,
  round(count(*) / sum(count(*)) OVER (PARTITION BY year), 4) AS share
FROM survey.silver.responses
WHERE work_mode IS NOT NULL
GROUP BY year, work_mode;

CREATE MATERIALIZED VIEW survey.gold.databases_pro
COMMENT 'Databases used by professional developers, per year'
AS SELECT year, db AS database, count(*) AS users
FROM survey.silver.responses LATERAL VIEW explode(databases_worked) t AS db
WHERE main_branch = 'I am a developer by profession'
GROUP BY year, db;

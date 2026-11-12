# Databricks notebook source
# MAGIC %md
# MAGIC # 3 · Analysis: Spark SQL → pandas → charts
# MAGIC
# MAGIC The heavy lifting (filter, group, aggregate) runs in **Spark**; only the small result goes to
# MAGIC **pandas** (`toPandas()`) and **matplotlib** for the chart.
# MAGIC
# MAGIC > `toPandas()` / `collect()` copy every row to the driver's memory. Use them on aggregates or
# MAGIC > samples, never on the full table.
# MAGIC
# MAGIC Gold tables answer the common questions directly; silver is there for anything else.

# COMMAND ----------

import json
import matplotlib.pyplot as plt
import pandas as pd

S = "survey.silver.responses"
G = "survey.gold"
pd.set_option("display.max_columns", None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark DataFrame vs pandas DataFrame

# COMMAND ----------

dist_df = spark.sql(f"SELECT * FROM {S} LIMIT 10")   # a recipe: distributed, lazy
local_df = dist_df.toPandas()                          # data: copied into this notebook's memory
print(type(dist_df), type(local_df))
spark.sql(f"SELECT year, count(*) FROM {S} GROUP BY year").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Respondents per year, and the top 10 countries

# COMMAND ----------

per_year = spark.sql(f"SELECT year, sum(respondents) AS n FROM {G}.respondents_by_country GROUP BY year ORDER BY year").toPandas()
top = spark.sql(f"""SELECT country, sum(respondents) AS n FROM {G}.respondents_by_country
                    GROUP BY country ORDER BY n DESC LIMIT 10""").toPandas()
fig, ax = plt.subplots(1, 2, figsize=(14, 4))
ax[0].bar(per_year["year"].astype(str), per_year["n"]); ax[0].set_title("Respondents per year")
ax[1].barh(top["country"][::-1], top["n"][::-1]); ax[1].set_title("Top 10 countries, 2023-2025")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Age of respondents

# COMMAND ----------

age = spark.sql(f"""SELECT year, age_band, count(*) AS n FROM {S}
                    WHERE age_band IS NOT NULL AND age_band != 'Prefer not to say' GROUP BY ALL""").toPandas()
age.pivot(index="age_band", columns="year", values="n").plot.bar(figsize=(10, 4), title="Age band per year", rot=30)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Languages: who is growing?
# MAGIC In bronze the languages were one string, `"Python;SQL;Rust"`. Silver split it into an array and
# MAGIC `silver.languages` has one row per language — the `posexplode(split(...))` step, done once in the pipeline.

# COMMAND ----------

trend = spark.sql(f"""SELECT year, language, share FROM {G}.language_trend
                      WHERE language IN (SELECT language FROM {G}.language_trend WHERE year = 2025 ORDER BY share DESC LIMIT 10)""").toPandas()
trend.pivot(index="year", columns="language", values="share").plot(marker="o", figsize=(10, 5), title="Share of respondents using a language")
plt.xticks([2023, 2024, 2025]); plt.legend(bbox_to_anchor=(1, 1)); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data scientists: languages they use vs. languages they want

# COMMAND ----------

ds = spark.sql(f"""SELECT l.kind, l.language, count(*) AS n
                   FROM survey.silver.languages l JOIN {S} r USING (year, response_id)
                   WHERE r.dev_type = 'Data scientist' GROUP BY ALL""").toPandas()
ds = ds.pivot(index="language", columns="kind", values="n").fillna(0).sort_values("worked").tail(15)
ds.plot.barh(figsize=(8, 7), title="Data scientists, 2023-2025"); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pay: median and middle 50 % by country (employed respondents, yearly USD)

# COMMAND ----------

pay = spark.sql(f"""SELECT country, median, p25, p75 FROM {G}.salary_by_country
                    WHERE year = (SELECT max(year) FROM {G}.salary_by_country) ORDER BY median""").toPandas()
plt.figure(figsize=(8, 8))
plt.barh(pay["country"], pay["median"], xerr=[pay["median"] - pay["p25"], pay["p75"] - pay["median"]], capsize=2)
plt.title("Median yearly pay (USD), 25th-75th percentile"); plt.show()

# COMMAND ----------

box = spark.sql(f"""SELECT country, comp_usd_yearly FROM {S}
                    WHERE is_employed AND comp_usd_yearly BETWEEN 1000 AND 1000000
                      AND country IN ('United States', 'Germany', 'United Kingdom', 'Poland', 'India', 'Brazil')""").toPandas()
box.boxplot(column="comp_usd_yearly", by="country", showfliers=False, rot=30, figsize=(10, 5)); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI tools and remote work: what changed in three years

# COMMAND ----------

fig, ax = plt.subplots(1, 2, figsize=(14, 4))
for a, table, col in [(ax[0], "ai_adoption", "ai_use"), (ax[1], "remote_work", "work_mode")]:
    df = spark.sql(f"SELECT year, {col}, share FROM {G}.{table}").toPandas()
    df.pivot(index="year", columns=col, values="share").plot.bar(stacked=True, ax=a, rot=0, title=table)
    a.legend(fontsize=7)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⭐ Heatmap: how often people visit Stack Overflow, by country (2024)

# COMMAND ----------

order = ["Multiple times per day", "Daily or almost daily", "A few times per week",
         "A few times per month or weekly", "Less than once per month or monthly"]
hm = spark.sql(f"""SELECT country, so_visit_freq, count(*) / sum(count(*)) OVER (PARTITION BY country) AS share
                   FROM {S} WHERE year = 2024 AND so_visit_freq IS NOT NULL
                     AND country IN ('Poland', 'United States', 'Germany', 'India', 'Brazil', 'Japan', 'Ukraine')
                   GROUP BY ALL""").toPandas()
grid = hm.pivot(index="country", columns="so_visit_freq", values="share")[order]
plt.figure(figsize=(10, 4)); plt.imshow(grid, cmap="BuGn"); plt.colorbar()
plt.xticks(range(len(order)), order, rotation=30, ha="right"); plt.yticks(range(len(grid)), grid.index); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## <span style='background:yellow'> TASK 3 </span>
# MAGIC * Bar chart: the 10 databases professional developers use most in 2025 (`gold.databases_pro`).
# MAGIC   Which one grew most since 2023?
# MAGIC * Pie chart: share of Bachelor / Master / Professional degree among 2025 respondents (`silver.responses.ed_level`).
# MAGIC * Pay by `work_mode` for employed respondents in Poland — is remote work paid more?

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "per_year": dict(zip(per_year["year"].astype(str), per_year["n"].astype(int))),
    "top_country": top["country"].iloc[0],
    "python_share": {str(r.year): r.share for r in spark.sql(f"SELECT year, share FROM {G}.language_trend WHERE language = 'Python'").collect()},
    "pay_countries": len(pay), "ds_languages": len(ds),
}))

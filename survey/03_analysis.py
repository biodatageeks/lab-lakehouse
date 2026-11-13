# Databricks notebook source
# MAGIC %md
# MAGIC # 3 · Analysis: DataFrame API, Spark SQL, lazy evaluation and charts
# MAGIC
# MAGIC The pipeline left clean tables behind: `silver.responses` (one row per answer sheet and year),
# MAGIC `silver.languages` and the small `gold.*` aggregates. Now we ask questions of them, in four parts:
# MAGIC
# MAGIC | part | what | tools |
# MAGIC |---|---|---|
# MAGIC | **A** | read, inspect, select, filter, group, new columns, arrays, write | Spark **DataFrame API** (Python) |
# MAGIC | **B** | the same kind of questions in SQL; one plan for both APIs | **Spark SQL** |
# MAGIC | **C** | when does Spark actually do the work? Spark vs pandas | lazy vs eager evaluation |
# MAGIC | **D** | charts | pandas, **matplotlib**, **seaborn** |
# MAGIC
# MAGIC The rule for the whole notebook: the heavy lifting (filter, group, aggregate) runs in **Spark**; only the
# MAGIC small result goes to **pandas** (`toPandas()`) for a chart.
# MAGIC
# MAGIC > `toPandas()` / `collect()` copy every row to the driver's memory. Use them on aggregates or
# MAGIC > samples, never on the full table.

# COMMAND ----------

import json, time, warnings
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.window import Window

S = "survey.silver.responses"   # clean and typed: one row per respondent and year
B = "survey.bronze.responses"   # raw strings exactly as in the CSV files, partitioned by year
G = "survey.gold"
LAB = "survey.lab"              # our own tables (the pipeline owns bronze, silver and gold)
pd.set_option("display.max_columns", None)
sns.set_theme(style="whitegrid")
warnings.filterwarnings("ignore", message="The figure layout has changed to tight")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part A · Spark DataFrame API
# MAGIC A Spark **DataFrame** is a table with named, typed columns, split into **partitions** that are processed
# MAGIC in parallel. In Python you work with it through methods: `select`, `filter`, `groupBy`, `withColumn`, …
# MAGIC
# MAGIC ## A1 · A DataFrame by hand

# COMMAND ----------

people = spark.createDataFrame([
    (1, "Poland", "Data scientist", 12.0),
    (2, "Germany", "Data engineer", 5.0),
    (3, "Poland", "Developer, back-end", None),
    (4, "India", "Data scientist", 2.0),
])
people.show()
people.printSchema()   # names _1, _2, … and types guessed from the values

# COMMAND ----------

people = spark.createDataFrame(
    [(1, "Poland", "Data scientist", 12.0), (2, "Germany", "Data engineer", 5.0),
     (3, "Poland", "Developer, back-end", None), (4, "India", "Data scientist", 2.0)],
    schema="id INT NOT NULL, country STRING, dev_type STRING, years_code DOUBLE")
people.printSchema()   # our names and types; Python None became NULL
people.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## A2 · Read a table and look at it
# MAGIC Usually the data is already somewhere — here, in a Unity Catalog table.

# COMMAND ----------

df = spark.read.table(S)          # the same as spark.table(S)
print(type(df))
print(len(df.columns), "columns:", df.columns)
df.printSchema()

# COMMAND ----------

print(df.count(), "rows")
df.describe("years_code", "work_exp", "comp_usd_yearly").show()    # count, mean, stddev, min, max
df.summary("count", "25%", "50%", "75%").select("summary", "years_code", "work_exp", "comp_usd_yearly").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Compare the **mean** and the **50 %** (the median) of `comp_usd_yearly`: a handful of absurd answers
# MAGIC (tens of millions of dollars a year) pull the mean up. For pay we will use the median.
# MAGIC
# MAGIC Every row lives in one **partition**. Serverless compute has no RDD API (`df.rdd.getNumPartitions()`
# MAGIC does not work), but `spark_partition_id()` tells where each row is:

# COMMAND ----------

df.groupBy(F.spark_partition_id().alias("partition")).count().orderBy("partition").show()

# COMMAND ----------

raw = spark.read.table(B)
print(len(raw.columns), "columns in bronze; ConvertedCompYearly is", raw.schema["ConvertedCompYearly"].dataType)
raw.select("year", "ResponseId", "Country", "ConvertedCompYearly", "LanguageHaveWorkedWith").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A3 · Select columns, unique values, filter rows

# COMMAND ----------

df.select("year", "country", "dev_type", "years_code").show(5)
df.select(df.country, df.age_band).show(5)                               # columns as attributes …
df.select(F.col("country"), (F.col("years_code") * 12).alias("months_coding")).show(5)   # … or F.col(), for expressions

# COMMAND ----------

countries = df.select("country").distinct()     # a new DataFrame: the unique countries
print(countries.count(), "countries (NULL included)")

# COMMAND ----------

# MAGIC %md
# MAGIC The same filter written three ways. With `Column` objects use `&`, `|`, `~` (not `and`, `or`, `not`)
# MAGIC and put every condition in parentheses.

# COMMAND ----------

df.filter("country = 'Poland' AND years_code > 30").select("year", "dev_type", "years_code", "comp_usd_yearly").show(5)
df.filter((F.col("country") == "Poland") & (F.col("years_code") > 30)).select("year", "dev_type", "years_code").show(5)
print(df.where(F.col("dev_type").isin("Data scientist", "Data engineer") & F.col("comp_usd_yearly").isNotNull()).count(),
      "data scientists and data engineers who gave their pay")

# COMMAND ----------

df.filter("country = 'Poland'").select("year", "dev_type").explain()   # the plan: a filter and a scan of 3 columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## A4 · Group and aggregate

# COMMAND ----------

df.groupBy("year").count().orderBy("year").show()
df.groupBy("year").avg("years_code").orderBy("year").show()
df.groupBy("year", "work_mode").count().orderBy("year", F.desc("count")).show()

# COMMAND ----------

pay_by_country = (df.filter("is_employed AND comp_usd_yearly IS NOT NULL")
    .groupBy("country")
    .agg(F.count("*").alias("n"),
         F.round(F.avg("comp_usd_yearly")).alias("mean_pay"),
         F.median("comp_usd_yearly").alias("median_pay"))
    .filter("n >= 1000")                       # a filter on an aggregate: HAVING in SQL
    .orderBy(F.desc("median_pay")))
pay_by_country.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## A5 · New columns
# MAGIC `withColumn` adds (or replaces) a column; the value can be computed from other columns, a constant
# MAGIC (`lit`), a text built from pieces (`concat_ws`) or a condition (`when` … `otherwise`).

# COMMAND ----------

pl = df.filter("country = 'Poland'").select("year", "response_id", "dev_type", "years_code", "comp_usd_yearly")
pl.withColumn("comp_usd_monthly", F.round(F.col("comp_usd_yearly") / 12)).show(5)
pl.withColumn("source", F.lit("Stack Overflow survey")).show(5)
pl.withColumn("label", F.concat_ws(" · ", F.col("year").cast("string"), "dev_type")).show(5, truncate=False)

# COMMAND ----------

seniority = (F.when(F.col("years_code") < 5, "junior")
              .when(F.col("years_code") < 15, "mid")
              .when(F.col("years_code") >= 15, "senior"))   # NULL years_code stays NULL instead of becoming "senior"
pl.withColumn("seniority", seniority).groupBy("year").pivot("seniority").count().orderBy("year").show()

# COMMAND ----------

smaller = pl.drop("dev_type")
print("smaller:", smaller.columns)
print("pl:     ", pl.columns)   # pl still has dev_type: DataFrames are immutable, every method returns a new one

# COMMAND ----------

# MAGIC %md
# MAGIC ## A6 · Multi-select answers: split and explode
# MAGIC In the CSV a multi-select answer is one string, `"Python;SQL;Rust"`. `split` makes an array, `explode` one
# MAGIC row per element, `posexplode` also the position. Here on the raw bronze strings — the pipeline does the
# MAGIC same once for `silver.languages`.

# COMMAND ----------

langs = raw.filter("year = 2025 AND LanguageHaveWorkedWith != 'NA'").select("ResponseId", "LanguageHaveWorkedWith")
langs.select("ResponseId", F.posexplode(F.split("LanguageHaveWorkedWith", ";")).alias("pos", "language")).show(8)

# COMMAND ----------

top_langs = (langs.select(F.explode(F.split("LanguageHaveWorkedWith", ";")).alias("language"))
             .groupBy("language").count()
             .orderBy(F.desc("count")))
top_langs.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Silver already holds arrays (`languages_worked`), so array functions work directly:

# COMMAND ----------

lang_stats = (df.filter(F.size("languages_worked") > 0)
    .groupBy("year")
    .agg(F.round(F.avg(F.size("languages_worked")), 1).alias("languages_per_person"),
         F.round(F.avg(F.array_contains("languages_worked", "Python").cast("int")), 4).alias("python_share"))
    .orderBy("year"))
lang_stats.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## A7 · Save the result
# MAGIC As a **Delta table** in Unity Catalog (the lakehouse way), or as **files** in a volume. Not into
# MAGIC `/Volumes/survey/raw/landing`: the job's trigger watches that folder and would start the pipeline.

# COMMAND ----------

pay_by_country.write.mode("overwrite").saveAsTable(f"{LAB}.pay_by_country")
spark.table(f"{LAB}.pay_by_country").show(5)

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {LAB}.exports")
out = "/Volumes/survey/lab/exports/pay_by_country"
pay_by_country.write.mode("overwrite").option("header", True).csv(out)
display(dbutils.fs.ls(out))    # a folder: one part-*.csv per partition, plus Spark's bookkeeping files

# COMMAND ----------

back = spark.read.load(out, format="csv", header=True, inferSchema=True)   # and read it back
back.printSchema()
back.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## <span style='background:yellow'> TASK 3A · DataFrame API </span>
# MAGIC a) How many different job titles (`dev_type`) are there in 2025? How many countries?
# MAGIC
# MAGIC b) Show year, country, job title, years of coding and pay of respondents from Poland, Czech Republic and
# MAGIC    Slovakia who have been coding for more than 30 years.
# MAGIC
# MAGIC c) How many respondents per job title and year? Put the years into columns (`groupBy(…).pivot("year").count()`),
# MAGIC    sort by 2025 and save the result as the table `survey.lab.dev_types`.

# COMMAND ----------

# TASK 3A: your code


# COMMAND ----------

# MAGIC %md
# MAGIC # Part B · Spark SQL
# MAGIC The same engine, in SQL. A catalog table can be queried right away; a DataFrame needs a name first —
# MAGIC a **temporary view**, which lives only in this session.

# COMMAND ----------

pl.createOrReplaceTempView("poland")
spark.sql("""SELECT year, count(*) AS respondents, median(comp_usd_yearly) AS median_pay
             FROM poland GROUP BY year ORDER BY year""").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A SQL cell: no Python at all. The view `poland` from the Python cell above is visible here too.
# MAGIC -- Under the result: "+" -> Visualization turns it into a chart without any code.
# MAGIC SELECT dev_type, count(*) AS n
# MAGIC FROM poland
# MAGIC WHERE year = 2025 AND dev_type IS NOT NULL
# MAGIC GROUP BY dev_type
# MAGIC ORDER BY n DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## B1 · One question, two APIs, one plan
# MAGIC Where do the data scientists of 2025 live?

# COMMAND ----------

q_sql = spark.sql(f"""
    SELECT country, count(*) AS n
    FROM {S}
    WHERE year = 2025 AND dev_type = 'Data scientist' AND country IS NOT NULL
    GROUP BY country
    ORDER BY n DESC, country
    LIMIT 5""")
q_df = (spark.table(S)
        .filter((F.col("year") == 2025) & (F.col("dev_type") == "Data scientist") & F.col("country").isNotNull())
        .groupBy("country").agg(F.count("*").alias("n"))
        .orderBy(F.desc("n"), "country")
        .limit(5))
q_sql.show()
same_result = q_sql.collect() == q_df.collect()
print("same result:", same_result)

# COMMAND ----------

q_sql.explain()
q_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Both go through the same optimizer (**Catalyst**) and end as the same physical plan, so pick whichever
# MAGIC reads better — and mix them: `spark.sql(…)` returns a DataFrame, and `selectExpr` / `F.expr` put SQL inside
# MAGIC DataFrame code.

# COMMAND ----------

q_sql.filter("n > 20").selectExpr("country", "n", "round(100 * n / sum(n) OVER (), 1) AS percent").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B2 · SQL worth knowing: CTE, HAVING, window functions, subqueries
# MAGIC The three countries with the highest median pay, every year:

# COMMAND ----------

top3 = spark.sql(f"""
    WITH pay AS (                                     -- a CTE: a named subquery
      SELECT year, country, count(*) AS n, median(comp_usd_yearly) AS median_pay
      FROM {S}
      WHERE is_employed AND comp_usd_yearly BETWEEN 1000 AND 1000000
      GROUP BY year, country
      HAVING count(*) >= 300                          -- a condition on the aggregate
    )
    SELECT * FROM (
      SELECT *, rank() OVER (PARTITION BY year ORDER BY median_pay DESC) AS place   -- a window function
      FROM pay)
    WHERE place <= 3
    ORDER BY year, place""")
top3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## <span style='background:yellow'> TASK 3B · Spark SQL </span>
# MAGIC * In SQL: the mean and the median of `years_code` per country in 2025, only countries with at least
# MAGIC   500 answers, sorted by the median. Then write the same query with the DataFrame API and check that
# MAGIC   both return the same rows.
# MAGIC * Rewrite `top3` with the DataFrame API: `Window.partitionBy("year").orderBy(F.desc("median_pay"))` and
# MAGIC   `F.rank().over(…)`.

# COMMAND ----------

# TASK 3B: your code


# COMMAND ----------

# MAGIC %md
# MAGIC # Part C · Lazy evaluation: Spark vs pandas
# MAGIC Spark methods are of two kinds:
# MAGIC * **transformations** — `select`, `filter`, `withColumn`, `groupBy(…).agg`, `join`, `orderBy` — only add a step
# MAGIC   to a **plan** and return a new DataFrame. Nothing is read, nothing is computed.
# MAGIC * **actions** — `show`, `count`, `collect`, `toPandas`, `write` — send the whole plan to the cluster, which
# MAGIC   optimizes it and runs it.
# MAGIC
# MAGIC pandas is **eager**: every line runs at once, on data that is already in the notebook's memory.
# MAGIC
# MAGIC ## C1 · Writing the plan costs nothing; the action does the work
# MAGIC A plan is only a description, so its size does not matter: every bronze row paired with every other
# MAGIC row is 40 billion rows, and the plan is ready at once. (Do not run an action on it.)

# COMMAND ----------

t0 = time.perf_counter()
pairs = spark.table(B).crossJoin(spark.table(B)).groupBy().count()      # 200 k × 200 k = 40 billion pairs
print(f"a plan over 40 billion rows: {(time.perf_counter() - t0) * 1000:.0f} ms")

# COMMAND ----------

# MAGIC %md
# MAGIC A real question now: the median pay per country in 2025, straight from the raw bronze strings.

# COMMAND ----------

t0 = time.perf_counter()
plan = (spark.table(B)                               # 200 k rows × 249 string columns
        .filter("year = 2025")
        .select("Country", "ConvertedCompYearly")
        .withColumn("pay", F.expr("try_cast(ConvertedCompYearly AS DOUBLE)"))
        .groupBy("Country")
        .agg(F.count("*").alias("n"), F.median("pay").alias("median_pay")))
build_ms = (time.perf_counter() - t0) * 1000
print(f"5 transformations: {build_ms:.0f} ms, no data has been read yet")

t0 = time.perf_counter()
rows = plan.collect()                                # the action: now Spark reads and computes
run_s = time.perf_counter() - t0
print(f"collect(): {run_s:.1f} s, {len(rows)} countries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C2 · Spark saw the whole plan before running it
# MAGIC Read the plan from the bottom. The scan of `bronze.responses` reads **only the columns the plan needs**
# MAGIC (2 of 249: *column pruning*) and **only the `year=2025` partition** (`PartitionFilters`: the
# MAGIC `PARTITIONED BY year` from notebook 1 pays off). Spark can do that only because nothing ran before the
# MAGIC action: it optimizes the chain as a whole, whatever order we wrote the steps in.

# COMMAND ----------

plan.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C3 · …and that is why errors show up late
# MAGIC On serverless (Spark Connect) even column names are checked only when an action runs, so a typo made in
# MAGIC one cell can fail in a later one. A classic cluster checks names when the plan is built.

# COMMAND ----------

error_at, stage = None, "building the plan"
try:
    broken = spark.table(S).select("country", "salary")     # there is no column "salary"
    stage = "the action"
    broken.show()
except Exception as e:
    error_at = stage
    print(f"error during {stage}: {type(e).__name__}: {str(e).splitlines()[0][:150]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C4 · The same question in pandas: every line runs now
# MAGIC pandas can read the CSV straight from the volume. `usecols` keeps 2 columns in memory, but the whole file
# MAGIC is still parsed: pandas has no plan to optimize and no partitions to skip.

# COMMAND ----------

path = "/Volumes/survey/raw/landing/year=2025/results.csv"
t0 = time.perf_counter()
pdf = pd.read_csv(path, usecols=["Country", "ConvertedCompYearly"])          # runs NOW
read_s = time.perf_counter() - t0
t0 = time.perf_counter()
pd_result = pdf.groupby("Country")["ConvertedCompYearly"].agg(["size", "median"])   # runs NOW, in this notebook
group_ms = (time.perf_counter() - t0) * 1000
print(f"read_csv: {read_s:.1f} s for {len(pdf):,} rows, {pdf.memory_usage(deep=True).sum() / 1e6:.1f} MB in memory")
print(f"groupby + median: {group_ms:.0f} ms")
pd_result.sort_values("size", ascending=False).head()

# COMMAND ----------

spark_median = {r.Country: r.median_pay for r in rows}
# bronze keeps a missing country as the text 'NA'; pandas reads it as NaN and leaves it out of groupby
biggest = [r.Country for r in sorted(rows, key=lambda r: -r.n) if r.Country in pd_result.index][:10]
pandas_matches_spark = all(abs(spark_median[c] - pd_result.loc[c, "median"]) < 0.01 for c in biggest)
print("pandas and Spark give the same median pay for the 10 biggest countries:", pandas_matches_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C5 · Between the two worlds

# COMMAND ----------

dist_df = spark.sql(f"SELECT * FROM {S} LIMIT 10")    # a recipe: distributed, lazy
local_df = dist_df.toPandas()                           # an action: the data is copied into this notebook's memory
again = spark.createDataFrame(local_df)                 # and back: pandas -> Spark
print(type(dist_df), type(local_df), type(again), sep="\n")
local_df

# COMMAND ----------

# MAGIC %md
# MAGIC | | Spark DataFrame | pandas DataFrame |
# MAGIC |---|---|---|
# MAGIC | where the data is | in files, spread over the cluster | in this notebook's memory |
# MAGIC | one line of code | adds a step to a plan (**lazy**) | runs now (**eager**) |
# MAGIC | the work happens at | actions: `show`, `count`, `collect`, `toPandas`, `write` | every line |
# MAGIC | optimizer | sees the whole plan: reads only the needed columns and partitions | none: `read_csv` parses the whole file |
# MAGIC | size limit | the cluster and the storage | the driver's memory |
# MAGIC | changing data | never in place, every method returns a new DataFrame | in place (`df["x"] = …`) |
# MAGIC | the same action twice | runs the plan twice (save what you reuse, as `gold` does) | the data is already there |
# MAGIC | charts | no (aggregate first, then `toPandas()`) | yes: matplotlib, seaborn |

# COMMAND ----------

# MAGIC %md
# MAGIC ## <span style='background:yellow'> TASK 3C · lazy or not? </span>
# MAGIC * Which of these start Spark work, and which only add to a plan? `spark.table(S)`, `.filter(…)`,
# MAGIC   `.groupBy("year").count()`, `.printSchema()`, `.explain()`, `.show()`, `.toPandas()`, `.write.saveAsTable(…)`.
# MAGIC   Time them with `time.perf_counter()`.
# MAGIC * Run `plan.collect()` twice and time both. Then change `plan` to filter on `Country = 'Poland'`
# MAGIC   instead of the year: what happens to `PartitionFilters` in `explain`? Why?

# COMMAND ----------

# TASK 3C: your code


# COMMAND ----------

# MAGIC %md
# MAGIC # Part D · Charts
# MAGIC Chart libraries need the data **locally**: Python lists, dicts or pandas DataFrames — not Spark DataFrames.
# MAGIC So every chart has the same two steps: prepare a small result in Spark, `toPandas()`, draw.
# MAGIC Two libraries: **matplotlib** (low level, full control; pandas' `.plot` is built on it) and **seaborn**
# MAGIC (statistical charts in one call, on top of matplotlib).
# MAGIC
# MAGIC ## Respondents per year, and the top 10 countries

# COMMAND ----------

per_year = spark.sql(f"SELECT year, sum(respondents) AS n FROM {G}.respondents_by_country GROUP BY year ORDER BY year").toPandas()
top = spark.sql(f"""SELECT country, sum(respondents) AS n FROM {G}.respondents_by_country
                    GROUP BY country ORDER BY n DESC LIMIT 10""").toPandas()
fig, ax = plt.subplots(1, 2, figsize=(14, 4))
ax[0].bar(per_year["year"].astype(str), per_year["n"]); ax[0].set_title("Respondents per year")
ax[1].barh(top["country"][::-1], top["n"][::-1]); ax[1].set_title("Top 10 countries, 2023-2025 (matplotlib)")
plt.show()

# COMMAND ----------

sns.catplot(data=top, x="country", y="n", kind="bar", color="tab:blue", height=4, aspect=2.5)
plt.title("Top 10 countries, 2023-2025 (seaborn)"); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Histogram: years of coding
# MAGIC One numeric column of one year (~45 k values) is small enough to bring over as it is.

# COMMAND ----------

yc = spark.sql(f"SELECT years_code FROM {S} WHERE year = 2025 AND years_code BETWEEN 0 AND 50").toPandas()
fig, ax = plt.subplots(1, 2, figsize=(14, 4))
yc.hist("years_code", bins=25, ax=ax[0]); ax[0].set_title("pandas .hist()")
sns.histplot(data=yc, x="years_code", bins=25, kde=True, ax=ax[1]); ax[1].set_title("seaborn histplot, with a density line")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC For a really big table, **bin in Spark** and bring only the counts: here all three years, 26 rows to pandas.

# COMMAND ----------

bins = spark.sql(f"""SELECT floor(years_code / 2) * 2 AS bin, count(*) AS n FROM {S}
                     WHERE years_code BETWEEN 0 AND 51 GROUP BY ALL ORDER BY bin""").toPandas()
plt.figure(figsize=(10, 3.5)); plt.bar(bins["bin"], bins["n"], width=1.8, align="edge")
plt.title(f"Years of coding, 2023-2025, binned in Spark ({len(bins)} rows came to pandas)"); plt.show()

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
# MAGIC ## Pie charts: who answers the survey? (2025)

# COMMAND ----------

short = {"I am a developer by profession": "professional developer",
         "I am not primarily a developer, but I write code sometimes as part of my work/studies": "codes for work or studies",
         "I am learning to code": "learning to code",
         "I code primarily as a hobby": "hobby",
         "I used to be a developer by profession, but no longer am": "former developer",
         "I work with developers or my work supports developers but am not a developer by profession": "works with developers"}
mb = spark.sql(f"""SELECT 'All' AS grp, main_branch, count(*) AS n FROM {S}
                   WHERE year = 2025 AND main_branch IS NOT NULL GROUP BY ALL
                   UNION ALL
                   SELECT country, main_branch, count(*) FROM {S}
                   WHERE year = 2025 AND main_branch IS NOT NULL AND country IN ('Poland', 'India') GROUP BY ALL""").toPandas()
mb["main_branch"] = mb["main_branch"].replace(short)
colors = dict(zip(sorted(mb["main_branch"].unique()), sns.color_palette("Set2")))
fig, ax = plt.subplots(1, 3, figsize=(16, 5))
for a, g in zip(ax, ["All", "Poland", "India"]):
    part = mb[mb.grp == g].sort_values("main_branch")
    a.pie(part["n"], colors=[colors[c] for c in part["main_branch"]], autopct=lambda p: f"{p:.0f}%" if p >= 3 else "")
    a.set_title(f"{g} ({part['n'].sum():,} answers)")
fig.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=c) for c in colors.values()], labels=list(colors),
           loc="lower center", ncol=3)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Languages: who is growing?
# MAGIC In bronze the languages were one string, `"Python;SQL;Rust"`; `gold.language_trend` counts them per year.

# COMMAND ----------

trend = spark.sql(f"""SELECT year, language, share FROM {G}.language_trend
                      WHERE language IN (SELECT language FROM {G}.language_trend WHERE year = 2025 ORDER BY share DESC LIMIT 10)""").toPandas()
trend.pivot(index="year", columns="language", values="share").plot(marker="o", figsize=(10, 5), title="Share of respondents using a language")
plt.xticks([2023, 2024, 2025]); plt.legend(bbox_to_anchor=(1, 1)); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The `top_langs` plan from part A (split + explode on the raw strings) has not run for this chart yet —
# MAGIC `toPandas()` runs it now.

# COMMAND ----------

langs_pd = top_langs.limit(25).toPandas()
plt.figure(figsize=(8, 8)); plt.barh(langs_pd["language"][::-1], langs_pd["count"][::-1])
plt.title("The 25 most used languages, 2025"); plt.show()

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

# MAGIC %md
# MAGIC ## Mean vs. median pay in countries with more than 1000 answers
# MAGIC `HAVING` keeps only the big countries. No upper limit on pay here — see what a few absurd answers do to the mean.

# COMMAND ----------

cs = spark.sql(f"""SELECT country, round(avg(comp_usd_yearly)) AS mean, median(comp_usd_yearly) AS median
                   FROM {S} WHERE is_employed AND comp_usd_yearly IS NOT NULL
                   GROUP BY country HAVING count(*) > 1000 ORDER BY median""").toPandas()
cs.set_index("country").plot.barh(figsize=(8, 7), title="Yearly pay (USD), 2023-2025: mean vs. median")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Box plots: the spread of pay in the same countries
# MAGIC The list of countries comes from a **subquery**.

# COMMAND ----------

box = spark.sql(f"""SELECT country, comp_usd_yearly FROM {S}
                    WHERE is_employed AND comp_usd_yearly BETWEEN 1000 AND 1000000
                      AND country IN (SELECT country FROM {S} WHERE is_employed AND comp_usd_yearly IS NOT NULL
                                      GROUP BY country HAVING count(*) > 1000)""").toPandas()
box_order = box.groupby("country")["comp_usd_yearly"].median().sort_values().index
fig, ax = plt.subplots(1, 2, figsize=(16, 6))
box.boxplot(column="comp_usd_yearly", by="country", showfliers=False, rot=60, ax=ax[0])
ax[0].set_title("pandas .boxplot()"); fig.suptitle("")
sns.boxplot(data=box, x="country", y="comp_usd_yearly", order=box_order, showfliers=False, color="lightsteelblue", ax=ax[1])
ax[1].set_title("seaborn boxplot, sorted by the median"); ax[1].tick_params(axis="x", rotation=60)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Line chart: pay and years of work experience
# MAGIC Experience in 2-year bins, computed in Spark; only bins with at least 20 answers.

# COMMAND ----------

exp = spark.sql(f"""SELECT country, floor(work_exp / 2) * 2 AS experience, median(comp_usd_yearly) AS median_pay
                    FROM {S}
                    WHERE is_employed AND comp_usd_yearly BETWEEN 1000 AND 1000000 AND work_exp BETWEEN 0 AND 30
                      AND country IN ('United States', 'Germany', 'Poland', 'India')
                    GROUP BY ALL HAVING count(*) >= 20 ORDER BY country, experience""").toPandas()
exp.pivot(index="experience", columns="country", values="median_pay").plot(
    marker="o", figsize=(10, 4), title="Median yearly pay (USD) by years of work experience, 2023-2025 (pandas)")
plt.show()

# COMMAND ----------

sns.relplot(data=exp, x="experience", y="median_pay", hue="country", kind="line", marker="o", height=4, aspect=2.2)
plt.title("The same chart in seaborn"); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Education: strip, violin and boxen plots (2025, employed)

# COMMAND ----------

edu = spark.sql(f"""
    SELECT CASE WHEN ed_level LIKE 'Bachelor%' THEN 'Bachelor'
                WHEN ed_level LIKE 'Master%' THEN 'Master'
                ELSE 'PhD / professional' END AS education,
           years_code, comp_usd_yearly
    FROM {S}
    WHERE year = 2025 AND is_employed AND comp_usd_yearly BETWEEN 1000 AND 300000
      AND (ed_level LIKE 'Bachelor%' OR ed_level LIKE 'Master%' OR ed_level LIKE 'Professional degree%')""").toPandas()
edu_order = ["Bachelor", "Master", "PhD / professional"]
fig, ax = plt.subplots(1, 3, figsize=(18, 5))
sns.stripplot(data=edu.sample(min(3000, len(edu)), random_state=1), x="education", y="years_code",
              order=edu_order, size=2, alpha=0.4, ax=ax[0])
ax[0].set_title("Years of coding (a sample of 3000 points)")
sns.violinplot(data=edu, x="education", y="comp_usd_yearly", order=edu_order, cut=0, ax=ax[1]); ax[1].set_title("Pay: violin")
sns.boxenplot(data=edu, x="education", y="comp_usd_yearly", order=edu_order, ax=ax[2]); ax[2].set_title("Pay: boxen")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI tools and remote work: what changed in three years

# COMMAND ----------

fig, ax = plt.subplots(1, 2, figsize=(16, 4))
for a, table, col in [(ax[0], "ai_adoption", "ai_use"), (ax[1], "remote_work", "work_mode")]:
    df_ = spark.sql(f"SELECT year, {col}, share FROM {G}.{table}").toPandas()
    df_.pivot(index="year", columns=col, values="share").plot.bar(stacked=True, ax=a, rot=0, title=table)
    a.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1, 1))
plt.tight_layout(); plt.show()

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
plt.figure(figsize=(10, 4))
sns.heatmap(grid, annot=True, fmt=".0%", cmap="BuGn")
plt.xlabel(""); plt.ylabel(""); plt.xticks(rotation=30, ha="right"); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## <span style='background:yellow'> TASK 3D · charts </span>
# MAGIC * Bar chart: the 10 databases professional developers use most in 2025 (`gold.databases_pro`).
# MAGIC   Which one grew most since 2023?
# MAGIC * Pie chart: share of Bachelor / Master / Professional degree among 2025 respondents (`silver.responses.ed_level`).
# MAGIC * Pay by `work_mode` for employed respondents in Poland — is remote work paid more?
# MAGIC * Violin plot: pay by company size (`org_size`), employed respondents in 2025. Mind the order of the
# MAGIC   categories, and that 2025 changed them (`Less than 20 employees`).

# COMMAND ----------

# TASK 3D: your code


# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "per_year": dict(zip(per_year["year"].astype(str), per_year["n"].astype(int))),
    "top_country": top["country"].iloc[0],
    "python_share": {str(r.year): r.share for r in spark.sql(f"SELECT year, share FROM {G}.language_trend WHERE language = 'Python'").collect()},
    "pay_countries": len(pay), "ds_languages": len(ds),
    "countries": countries.count(),
    "python_share_df": {str(r.year): r.python_share for r in lang_stats.collect()},
    "sql_equals_df": same_result,
    "lazy": {"build_ms": round(build_ms, 1), "run_s": round(run_s, 1), "error_at": error_at,
             "pandas_matches_spark": pandas_matches_spark},
    "exports": {"table_rows": spark.table(f"{LAB}.pay_by_country").count(), "csv_rows": back.count()},
    "charts": {"big_countries": len(cs), "box_countries": int(box["country"].nunique()),
               "experience_points": len(exp), "education_rows": len(edu)},
}, default=str))

# Silver: one clean, typed row per respondent and year.
#
# * "NA" and empty strings -> NULL (the same lesson as NULLVALUE "NA" in the Spark notebook)
# * multi-select answers ("Python;SQL;Rust") -> arrays
# * "Less than 1 year" / "More than 50 years" -> numbers
# * answers whose wording changed between years are mapped to one value (country, job title,
#   employment, AI use, remote work) — the raw answer is kept next to it
# * duplicates removed
# * expectations: rows without an id are dropped; pay and country are checked and reported
#
# Materialized views (not streaming tables): silver is recomputed from bronze, so a new year
# with new columns never breaks it.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

COUNTRY = {
    "United States of America": "United States",
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    "Russian Federation": "Russia",
    "Iran, Islamic Republic of...": "Iran",
    "Viet Nam": "Vietnam",
    "Republic of Korea": "South Korea",
    "The former Yugoslav Republic of Macedonia": "North Macedonia",
}

# Same job, different wording in different years.
DEV_TYPE = {
    "Engineer, data": "Data engineer",
    "Data scientist or machine learning specialist": "Data scientist",
    "Database administrator or engineer": "Database administrator",
    "DevOps specialist": "DevOps engineer or professional",
    "Senior Executive (C-Suite, VP, etc.)": "Senior executive (C-suite, VP, etc.)",
    "Developer, AI apps or physical AI": "Developer, AI",
    "Security professional": "Cybersecurity or InfoSec professional",
}


def na(c: str):
    v = F.trim(F.col(f"`{c}`"))
    return F.when(v.isin("NA", ""), F.lit(None)).otherwise(v)


def multi(c: str):
    v = na(c)
    return F.when(v.isNull(), F.array().cast("array<string>")).otherwise(F.split(v, ";"))


def years(c: str):
    v = na(c)
    return (
        F.when(v == "Less than 1 year", F.lit(0.5))
        .when(v == "More than 50 years", F.lit(51.0))
        .otherwise(F.expr(f"try_cast(trim(`{c}`) AS DOUBLE)"))
    )


def mapped(c: str, mapping: dict):
    v = na(c)
    m = F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])
    return F.coalesce(m[v], v)


def ai_use():
    # 2025 split "Yes" into "Yes, I use AI tools daily / weekly / monthly or infrequently"
    v = na("AISelect")
    return F.when(v.startswith("Yes"), F.lit("Yes")).otherwise(v)


def work_mode():
    # 2025 replaced "Hybrid (some remote, some in-person)" by two hybrid answers and "Your choice"
    v = na("RemoteWork")
    return (F.when(v.startswith("Hybrid"), F.lit("Hybrid"))
            .when(v.startswith("Your choice"), F.lit("Flexible"))
            .otherwise(v))


@dp.materialized_view(name="survey.silver.responses", comment="Cleaned survey answers, 2023 onwards")
@dp.expect_or_drop("has_response_id", "response_id IS NOT NULL")
@dp.expect("pay_in_range", "comp_usd_yearly IS NULL OR comp_usd_yearly BETWEEN 1000 AND 1000000")
@dp.expect("has_country", "country IS NOT NULL")
def responses():
    b = spark.read.table("survey.bronze.responses")
    employment = multi("Employment")
    return (
        b.select(
            F.col("year").cast("int").alias("year"),
            F.expr("try_cast(ResponseId AS INT)").alias("response_id"),
            na("MainBranch").alias("main_branch"),
            mapped("Country", COUNTRY).alias("country"),
            na("Age").alias("age_band"),
            employment.alias("employment"),
            # "Employed, full-time" (2023-2024) became "Employed" (2025)
            F.exists(employment, lambda e: e.startswith("Employed")).alias("is_employed"),
            na("RemoteWork").alias("remote_work_answer"),
            work_mode().alias("work_mode"),
            na("EdLevel").alias("ed_level"),
            mapped("DevType", DEV_TYPE).alias("dev_type"),
            na("OrgSize").alias("org_size"),
            years("YearsCode").alias("years_code"),
            years("WorkExp").alias("work_exp"),
            F.expr("try_cast(ConvertedCompYearly AS DOUBLE)").alias("comp_usd_yearly"),
            multi("LanguageHaveWorkedWith").alias("languages_worked"),
            multi("LanguageWantToWorkWith").alias("languages_wanted"),
            multi("DatabaseHaveWorkedWith").alias("databases_worked"),
            na("AISelect").alias("ai_answer"),
            ai_use().alias("ai_use"),
            na("SOVisitFreq").alias("so_visit_freq"),
            F.col("_source_file").alias("source_file"),
        )
        .dropDuplicates(["year", "response_id"])
    )


@dp.materialized_view(name="survey.silver.languages", comment="One row per respondent, year and language")
def languages():
    r = spark.read.table("survey.silver.responses")
    worked = r.select("year", "response_id", F.lit("worked").alias("kind"), F.explode("languages_worked").alias("language"))
    wanted = r.select("year", "response_id", F.lit("wanted").alias("kind"), F.explode("languages_wanted").alias("language"))
    return worked.unionByName(wanted)

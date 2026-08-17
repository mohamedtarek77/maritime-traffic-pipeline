"""
silver_to_gold_port_congestion.py
-----------------------------------
يقرأ هذا الكود بيانات Silver (مواقع السفن النظيفة)، يحدد أي سفينة موجودة
داخل نطاق جغرافي (Geofence) حول كل ميناء إماراتي رئيسي، ثم يحسب لكل ميناء:
    - عدد السفن الحالية داخل النطاق
    - متوسط مدة بقاء السفن (Dwell Time) بالساعات
    - أعلى تكرار زيارات في اليوم

يعمل هذا كـ Batch Job (وليس Streaming) ويُفضّل جدولته يوميًا عبر Airflow.

التشغيل:
    docker exec -it maritime-spark-master spark-submit \
        /opt/spark-apps/silver_to_gold_port_congestion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, min as spark_min, max as spark_max,
    count, unix_timestamp, round as spark_round, current_date
)

SILVER_PATH = "/opt/spark-data/silver/ais_positions"
GOLD_PATH = "/opt/spark-data/gold/port_congestion"

# مناطق الموانئ الرئيسية (مركز تقريبي + نصف قطر بالدرجات ~ يعادل تقريبًا 10-15 كم)
PORT_ZONES = [
    {"port_name": "Jebel Ali",       "lat": 24.9857, "lon": 55.0273, "radius_deg": 0.15},
    {"port_name": "Port Rashid",     "lat": 25.2867, "lon": 55.2830, "radius_deg": 0.10},
    {"port_name": "Zayed Port",      "lat": 24.5205, "lon": 54.3773, "radius_deg": 0.12},
    {"port_name": "Khalifa Port",    "lat": 24.8103, "lon": 54.6350, "radius_deg": 0.12},
    {"port_name": "Fujairah Port",   "lat": 25.1164, "lon": 56.3478, "radius_deg": 0.12},
]


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("AIS-Silver-To-Gold-Port-Congestion").getOrCreate()


def tag_port_zone(df):
    """يضيف عمود port_name لكل سفينة إذا كانت داخل نطاق أحد الموانئ، وإلا يضع 'At Sea'."""
    result = df.withColumn("port_name", lit("At Sea"))
    for zone in PORT_ZONES:
        is_inside = (
            (col("latitude").between(zone["lat"] - zone["radius_deg"], zone["lat"] + zone["radius_deg"])) &
            (col("longitude").between(zone["lon"] - zone["radius_deg"], zone["lon"] + zone["radius_deg"]))
        )
        result = result.withColumn(
            "port_name",
            when(is_inside, lit(zone["port_name"])).otherwise(col("port_name"))
        )
    return result


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.parquet(SILVER_PATH)
    tagged_df = tag_port_zone(silver_df).filter(col("port_name") != "At Sea")

    # حساب أول وآخر ظهور لكل سفينة داخل كل ميناء = مدة البقاء التقريبية
    dwell_df = (
        tagged_df.groupBy("port_name", "mmsi")
        .agg(
            spark_min("processed_at").alias("first_seen"),
            spark_max("processed_at").alias("last_seen"),
        )
        .withColumn(
            "dwell_hours",
            spark_round(
                (unix_timestamp("last_seen") - unix_timestamp("first_seen")) / 3600.0, 2
            )
        )
    )

    port_summary_df = (
        dwell_df.groupBy("port_name")
        .agg(
            count("mmsi").alias("vessel_count"),
            spark_round(spark_min("dwell_hours"), 2).alias("min_dwell_hours"),
            spark_round(
                spark_max("dwell_hours"), 2
            ).alias("max_dwell_hours"),
        )
        .withColumn("report_date", current_date())
    )

    (
        port_summary_df.write
        .mode("overwrite")
        .partitionBy("report_date")
        .parquet(GOLD_PATH)
    )

    port_summary_df.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

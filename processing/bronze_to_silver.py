"""
bronze_to_silver.py
--------------------
يقرأ هذا الكود رسائل AIS الخام من Kafka (طبقة Bronze)، يفك تشفير JSON،
يستخرج الحقول المهمة (رقم السفينة MMSI، الموقع، السرعة، الاتجاه)،
ينظف البيانات (استبعاد الإحداثيات غير الصالحة)، ثم يكتب النتيجة كطبقة
Silver منظمة بصيغة Parquet مقسّمة حسب التاريخ.

التشغيل داخل حاوية Spark:
    docker exec -it maritime-spark-master spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        /opt/spark-apps/bronze_to_silver.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_date,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "ais-raw-messages"
SILVER_PATH = "/opt/spark-data/silver/ais_positions"
CHECKPOINT_PATH = "/opt/spark-data/checkpoints/bronze_to_silver"

# مخطط رسالة AIS القادمة من aisstream.io (PositionReport)
position_report_schema = StructType([
    StructField("Latitude", DoubleType()),
    StructField("Longitude", DoubleType()),
    StructField("Sog", DoubleType()),          # السرعة فوق سطح الأرض (عقدة بحرية)
    StructField("Cog", DoubleType()),          # اتجاه الحركة (درجة)
    StructField("TrueHeading", IntegerType()),
])

meta_data_schema = StructType([
    StructField("MMSI", IntegerType()),
    StructField("ShipName", StringType()),
    StructField("time_utc", StringType()),
])

raw_message_schema = StructType([
    StructField("MetaData", meta_data_schema),
    StructField("Message", StructType([
        StructField("PositionReport", position_report_schema),
    ])),
])

envelope_schema = StructType([
    StructField("ingested_at", StringType()),
    StructField("raw", raw_message_schema),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AIS-Bronze-To-Silver")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), envelope_schema).alias("data"))
        .select(
            col("data.ingested_at").alias("ingested_at"),
            col("data.raw.MetaData.MMSI").alias("mmsi"),
            col("data.raw.MetaData.ShipName").alias("ship_name"),
            col("data.raw.MetaData.time_utc").alias("position_time_utc"),
            col("data.raw.Message.PositionReport.Latitude").alias("latitude"),
            col("data.raw.Message.PositionReport.Longitude").alias("longitude"),
            col("data.raw.Message.PositionReport.Sog").alias("speed_knots"),
            col("data.raw.Message.PositionReport.Cog").alias("course_deg"),
        )
    )

    # تنظيف: استبعاد الإحداثيات غير الصالحة (0,0 أو خارج النطاق الطبيعي)
    cleaned_df = (
        parsed_df
        .filter(col("mmsi").isNotNull())
        .filter(col("latitude").between(-90, 90))
        .filter(col("longitude").between(-180, 180))
        .filter(~((col("latitude") == 0) & (col("longitude") == 0)))
        .withColumn("processed_at", current_timestamp())
        .withColumn("event_date", to_date(col("ingested_at")))
    )

    query = (
        cleaned_df.writeStream
        .format("parquet")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

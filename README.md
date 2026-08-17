# Maritime Traffic Intelligence Pipeline

مشروع بورتفوليو لتحليل حركة السفن وازدحام الموانئ في الإمارات باستخدام بيانات
AIS الحية (نظام تحديد هوية السفن)، ببنية بيانات كاملة من الاستيعاب إلى العرض.

## المعمارية

```
aisstream.io (WebSocket)
        │
        ▼
  ingestion/ais_ingest.py  ──►  Kafka (ais-raw-messages)
        │
        ▼
processing/bronze_to_silver.py   (Spark Structured Streaming)
        │
        ▼
   Silver Layer (Parquet منظّف ومقسّم حسب التاريخ)
        │
        ▼
processing/silver_to_gold_port_congestion.py   (يُجدوَل يوميًا عبر Airflow)
        │
        ▼
   Gold Layer (تقرير ازدحام كل ميناء)
        │
        ▼
   dashboard/app.py (Streamlit) — خريطة حية + جدول ازدحام
```

## المتطلبات قبل البدء

1. Docker و Docker Compose مثبتين على جهازك
2. حساب مجاني على [aisstream.io](https://aisstream.io) للحصول على API Key

## خطوات التشغيل

### 1) إعداد مفتاح API

```bash
cd ingestion
cp .env.example .env
# افتح .env وضع مفتاح aisstream.io الخاص بك
```

### 2) تشغيل كل الخدمات

```bash
docker-compose up -d
```

هيشتغل عندك:
| الخدمة | الرابط |
|---|---|
| Kafka UI | http://localhost:8090 |
| Spark Master UI | http://localhost:8080 |
| MinIO Console | http://localhost:9001 (minioadmin / minioadmin123) |
| Airflow | http://localhost:8081 (admin / admin) |
| Streamlit Dashboard | http://localhost:8501 |

### 3) تشغيل خط استيعاب بيانات AIS

السكريبت ده بيشتغل خارج Docker مباشرة (أو تقدر تحوّله لحاوية لاحقًا):

```bash
cd ingestion
pip install -r requirements.txt
python ais_ingest.py
```

هتشوف في الـ Terminal رسائل بتأكد استلام بيانات مواقع السفن لحظة بلحظة.

### 4) تشغيل مهمة Bronze → Silver (معالجة مستمرة)

```bash
docker exec -it maritime-spark-master /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /opt/spark-apps/bronze_to_silver.py
```

سيب السكريبت شغال في نافذة منفصلة، لأنه Streaming Job مستمر.

### 5) تشغيل مهمة Silver → Gold (تقرير الازدحام)

```bash
docker exec -it maritime-spark-master /opt/spark/bin/spark-submit \
    /opt/spark-apps/silver_to_gold_port_congestion.py
```

هذه المهمة مجدولة تلقائيًا يوميًا الساعة 3 فجرًا عبر Airflow DAG
(`port_congestion_daily_report`)، لكن تقدر تشغّلها يدويًا للاختبار.

### 6) متابعة اللوحة

افتح http://localhost:8501 لمشاهدة خريطة السفن الحية وجدول ازدحام الموانئ.

## نقاط تستحق الذكر في المقابلات التقنية

- **لماذا Geofencing بسيط (Bounding Box) بدل مكتبات GIS معقدة؟**
  لأن دقة كيلومترات قليلة كافية جدًا لتحديد "هل السفينة داخل الميناء أم لا"،
  وده بيقلل الاعتمادية على مكتبات ثقيلة زي GeoPandas في مرحلة الـ MVP.

- **لماذا Kafka بين الاستيعاب والمعالجة بدل الكتابة المباشرة لـ Parquet؟**
  عشان يفصل سرعة استقبال البيانات (اللي ممكن تكون متقطعة أو سريعة جدًا)
  عن سرعة المعالجة، وده بيوفر إعادة التشغيل (Replay) لو حصل خطأ في المعالجة.

- **لماذا معمارية Bronze/Silver/Gold؟**
  عشان تفصل البيانات الخام (زي ما جاية) عن البيانات المنظفة عن التقارير
  الجاهزة للعرض، وده بيسهل تتبع أي خطأ ومعرفة في أي طبقة حصل.

## استكشاف الأخطاء الشائعة

**خطأ `manifest for bitnami/spark:3.5 not found`:**
شركة Bitnami أوقفت في 2025 توفير صور Docker الخاصة بها مجانًا على Docker Hub
وحولتها لاشتراك مدفوع (Bitnami Secure Images). لهذا السبب المشروع بيستخدم
الصورة الرسمية `apache/spark:3.5.1-python3` بدلًا منها. لو واجهت هذا الخطأ
تأكد إن `docker-compose.yml` بيستخدم `apache/spark` مش `bitnami/spark`.

## تطويرات مستقبلية مقترحة

- إضافة كشف شذوذ (Anomaly Detection) لسفن بتتحرك بسرعة غير طبيعية داخل الميناء
- ربط بيانات الطقس (رياح/أمواج) للربط بين حالة البحر وتأخر السفن
- استبدال Bounding Box البسيط بـ GeoPandas لحدود موانئ دقيقة (Polygons)
- نشر الـ Dashboard على Render أو Railway ليكون رابط حي يُعرض في البورتفوليو

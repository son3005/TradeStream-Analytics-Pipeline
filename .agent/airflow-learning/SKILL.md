---
name: airflow-learning
description: "Hướng dẫn học Apache Airflow từ zero cho dự án TradeStream. Orchestration, DAG design, scheduling, và monitoring pipelines."
---

# Apache Airflow Orchestration — TradeStream

## Mục đích

Hướng dẫn học và triển khai Apache Airflow để lên lịch, orchestrate, và monitor toàn bộ TradeStream pipeline.

## Khi nào sử dụng

- Khi bắt đầu Phase 5 của TradeStream
- Khi cần tạo/sửa DAGs
- Khi pipeline cần chạy tự động
- Khi debug scheduling issues

---

## Concepts cốt lõi

### Airflow là gì?
Apache Airflow là **workflow orchestrator** — "nhạc trưởng" điều phối các tasks chạy đúng thứ tự, đúng thời gian, và xử lý lỗi tự động.

### Thuật ngữ

| Thuật ngữ | Giải thích | Ví dụ TradeStream |
|-----------|-----------|-------------------|
| **DAG** | Directed Acyclic Graph — luồng công việc | `daily_batch_processing` |
| **Task** | Một bước trong DAG | `extract_data`, `transform`, `load` |
| **Operator** | Loại task | `PythonOperator`, `SparkSubmitOperator` |
| **Sensor** | Task chờ điều kiện | Chờ file data xuất hiện |
| **XCom** | Chia sẻ data giữa tasks | Task A trả kết quả cho Task B |
| **Schedule** | Lịch chạy | `@daily`, `0 */6 * * *` |
| **Backfill** | Chạy lại DAG cho ngày cũ | Xử lý lại data 7 ngày trước |
| **Pool** | Giới hạn tasks chạy đồng thời | Max 2 Spark jobs cùng lúc |

---

## Docker Setup

```yaml
# docker-compose.yml (profile: orchestration)
services:
  airflow-postgres:
    image: postgres:15
    profiles: ["orchestration"]
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - airflow-db:/var/lib/postgresql/data

  airflow-webserver:
    image: apache/airflow:2.8.0
    profiles: ["orchestration"]
    depends_on:
      - airflow-postgres
    ports:
      - "8083:8080"
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
    command: >
      bash -c "airflow db init && 
               airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@tradestream.local &&
               airflow webserver"

  airflow-scheduler:
    image: apache/airflow:2.8.0
    profiles: ["orchestration"]
    depends_on:
      - airflow-webserver
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow
    volumes:
      - ./dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
    command: airflow scheduler

volumes:
  airflow-db:
```

---

## Quick Demo: Hello DAG

```python
"""dags/hello_dag.py — DAG đầu tiên trên Airflow"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'tradestream',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def say_hello():
    print("👋 Hello từ Airflow! TradeStream đang chạy.")

def check_kafka():
    print("✅ Kafka: OK")

def check_spark():
    print("✅ Spark: OK")

with DAG(
    dag_id='hello_tradestream',
    default_args=default_args,
    description='Quick demo DAG',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['demo'],
) as dag:

    t1 = PythonOperator(task_id='say_hello', python_callable=say_hello)
    t2 = PythonOperator(task_id='check_kafka', python_callable=check_kafka)
    t3 = PythonOperator(task_id='check_spark', python_callable=check_spark)

    t1 >> [t2, t3]  # t1 chạy trước, t2 và t3 chạy song song
```

---

## Deep Build: Daily Batch DAG

```python
"""dags/daily_batch.py — Batch processing hàng ngày"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'tradestream',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
}

with DAG(
    dag_id='daily_batch_processing',
    default_args=default_args,
    description='Xử lý batch hàng ngày: compact, aggregate, quality check',
    schedule_interval='0 2 * * *',  # 2:00 AM mỗi ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['production', 'batch'],
) as dag:

    # Task 1: Compact Iceberg tables
    compact_tables = PythonOperator(
        task_id='compact_iceberg_tables',
        python_callable=lambda: print("Compacting Iceberg tables..."),
    )

    # Task 2: Tính toán daily aggregates
    daily_aggregates = PythonOperator(
        task_id='calculate_daily_aggregates',
        python_callable=lambda: print("Calculating daily aggregates..."),
    )

    # Task 3: Data quality check
    quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=lambda: print("Running data quality checks..."),
    )

    # Task 4: Update ML features
    update_features = PythonOperator(
        task_id='update_ml_features',
        python_callable=lambda: print("Updating ML feature store..."),
    )

    # Luồng: compact → [aggregates, quality] → features
    compact_tables >> [daily_aggregates, quality_check] >> update_features
```

---

## Checklist Phase 5

```
□ Airflow Webserver chạy tại http://localhost:8083
□ Login: admin/admin thành công
□ Quick Demo: hello_tradestream DAG chạy thành công
□ daily_batch_processing DAG đã tạo
□ data_quality DAG đã tạo
□ Backfill hoạt động
□ Alert khi task fail
□ Learning-log: Airflow concepts
```

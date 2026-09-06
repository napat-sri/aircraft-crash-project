from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
import numpy as np
import math
import mysql.connector

import json, logging, urllib.request

RAW_PATH   = "/opt/airflow/data/Airplane_Crashes_and_Fatalities_Since_1908_20190820105639.csv"
CLEAN_PATH = "/opt/airflow/data/airplane_crashes_cleaned.csv"
REPORT_PATH = "/opt/airflow/data/etl_quality_report.json"

# n8n รันบน host เครื่องเดียวกัน (พอร์ต 5678) → เรียกผ่าน host.docker.internal
N8N_WEBHOOK_URL = "http://host.docker.internal:5678/webhook-test/etl-done"

MYSQL_CONF = {
    "host": "host.docker.internal",   # MySQL อยู่บนเครื่อง host, ไม่ใช่ใน container
    "user": "root",
    "password": "",
    "database": "accidents",
}

default_args = {
    "owner": "napat",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---------- E : Extract ----------
def extract(**context):
    df = pd.read_csv(RAW_PATH)
    print(f"Extracted {df.shape[0]} rows")
    # ส่ง path ต่อผ่าน XCom (ไม่ส่ง DataFrame ทั้งก้อน)
    context["ti"].xcom_push(key="raw_rows", value=int(df.shape[0]))

# ---------- T : Transform (นำ cleansing จากโน้ตบุ๊ก 01 มาใส่) ----------
def transform(**context):
    df = pd.read_csv(RAW_PATH)

    # missing values
    for c in ["Flight #", "Route", "cn/ln"]:
        df[c] = df[c].fillna("Unknown")
    df["Time"]    = df["Time"].fillna("00:00")
    df["Ground"]  = pd.to_numeric(df["Ground"], errors="coerce").fillna(0)
    for c in ["Aboard", "Fatalities"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df[c] = df[c].fillna(df[c].median())
    df["Summary"] = df["Summary"].fillna("No summary available")
    df = df.dropna(subset=["Date", "Location"])

    df = df.drop(columns=["Aboard Passangers", "Aboard Crew",
                          "Fatalities Passangers", "Fatalities Crew"])

    # standardize
    df["Operator"] = df["Operator"].astype(str).str.strip().str.title()
    df["Location"] = df["Location"].astype(str).str.strip().str.title()
    df["AC Type"]  = df["AC Type"].astype(str).str.strip().str.title()

    # outlier: Fatalities > Aboard
    mask = df["Fatalities"] > df["Aboard"]
    df.loc[mask, "Fatalities"] = df.loc[mask, "Aboard"]

    # feature engineering
    df["Date"]   = pd.to_datetime(df["Date"], errors="coerce")
    df["Year"]   = df["Date"].dt.year
    df["Month"]  = df["Date"].dt.month
    df["Decade"] = (df["Year"] // 10 * 10).astype("Int64").astype(str) + "s"
    df["Survivors"]     = (df["Aboard"] - df["Fatalities"]).clip(lower=0)
    df["Survival_Rate"] = (df["Survivors"] / df["Aboard"].replace(0, np.nan)).round(2)
    df["Fatality_Rate"] = (df["Fatalities"] / df["Aboard"].replace(0, np.nan)).round(2)
    df["Country"]       = df["Location"].str.split(",").str[-1].str.strip()

    df.to_csv(CLEAN_PATH, index=False)   # index=False กันคอลัมน์เกิน
    print(f"Transformed & saved {df.shape[0]} rows -> {CLEAN_PATH}")

# ---------- L : Load เข้า MySQL ----------
def load(**context):
    df = pd.read_csv(CLEAN_PATH)

    # จัดการ NOT NULL columns (ตามข้อ 2.2)
    df['Operator'] = df['Operator'].fillna('Unknown')
    df['Country']  = df['Country'].fillna('Unknown')
    for c in ['Aboard', 'Fatalities', 'Ground', 'Survivors']:
        df[c] = df[c].astype(int)

    # ---------- แปลงค่าทีละตัว: NaN/NaT -> None ----------
    def clean(v):
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):   # จับ float NaN ทุกคอลัมน์
            return None
        return v

    rows = [tuple(clean(v) for v in row)
            for row in df.itertuples(index=False, name=None)]

    insert_sql = (
        "INSERT INTO accidents "
        "(date, time, location, operator, flight_no, route, ac_type, registration, cn_ln, "
        " aboard, fatalities, ground, summary, year, month, decade, survivors, "
        " survival_rate, fatality_rate, country) "
        "VALUES (" + ",".join(["%s"] * 20) + ")"
    )

    cnx = mysql.connector.connect(**MYSQL_CONF)
    cur = cnx.cursor()
    try:
        cur.execute("TRUNCATE TABLE accidents")
        CHUNK = 500
        total = 0
        for i in range(0, len(rows), CHUNK):
            cur.executemany(insert_sql, rows[i:i + CHUNK])
            total += cur.rowcount
        cnx.commit()
        print(f"✅ Loaded {total} rows")
    except mysql.connector.Error as err:
        cnx.rollback()
        raise
    finally:
        cur.close()
        cnx.close()

# ---------- validate ----------
def validate(**context):
    cnx = mysql.connector.connect(**MYSQL_CONF)
    cur = cnx.cursor()
    cur.execute("SELECT COUNT(*) FROM accidents")
    n = cur.fetchone()[0]
    cur.close(); cnx.close()
    if n == 0:
        raise ValueError("Load failed: ตารางว่าง")
    print(f"Validation OK: {n} rows")


# ---------- quality checks ----------
def quality_check(**context):
    # ตรวจคุณภาพบนข้อมูล "ดิบ" เพื่อรายงานปัญหาที่พบก่อน cleansing
    df = pd.read_csv(RAW_PATH)
    total = len(df)

    parsed_date = pd.to_datetime(df["Date"], errors="coerce")
    aboard = pd.to_numeric(df["Aboard"], errors="coerce")
    fatal  = pd.to_numeric(df["Fatalities"], errors="coerce")
    year   = parsed_date.dt.year
    current_year = pd.Timestamp.now().year

    # ---------- นับจำนวนแถวที่ผิดแต่ละเช็ค ----------
    is_duplicate         = int(df.duplicated().sum())
    invalid_date         = int(parsed_date.isna().sum())
    missing_operator     = int(df["Operator"].isna().sum())
    missing_location     = int(df["Location"].isna().sum())
    missing_aboard       = int(aboard.isna().sum())
    negative_fatalities  = int((fatal < 0).sum())
    invalid_year         = int(((year < 1908) | (year > current_year)).sum())
    fatalities_gt_aboard = int((fatal > aboard).sum())

    # ---------- rejected = ผิดเช็คสำคัญ (Date/Location หาย -> ถูก drop จริงใน transform) ----------
    rejected_mask = parsed_date.isna() | df["Location"].isna()
    rejected_rows = int(rejected_mask.sum())
    valid_rows    = total - rejected_rows

    # ---------- severity จากสัดส่วน rejected ----------
    ratio = rejected_rows / total if total else 0
    severity = "HIGH" if ratio > 0.20 else "MEDIUM" if ratio > 0.05 else "LOW"

    # ---------- ต้องให้คนตรวจไหม ----------
    human_review_required = (severity == "HIGH") or (fatalities_gt_aboard > 0)

    report = {
        "pipeline":             "airplane_crashes_etl",
        "total_rows":           total,
        "valid_rows":           valid_rows,
        "rejected_rows":        rejected_rows,
        "is_duplicate":         is_duplicate,
        "invalid_date":         invalid_date,
        "missing_operator":     missing_operator,
        "missing_location":     missing_location,
        "missing_aboard":       missing_aboard,
        "negative_fatalities":  negative_fatalities,
        "invalid_year":         invalid_year,
        "fatalities_gt_aboard": fatalities_gt_aboard,
        "severity":             severity,
        "human_review_required": human_review_required,
    }

    # ---------- log ให้ออกหน้าตาเหมือนตัวอย่าง ----------
    logging.info("Quality report:\n" + json.dumps(report, ensure_ascii=False, indent=2))

    # ---------- เก็บไฟล์ ----------
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # ---------- ส่งให้ n8n (เชื่อมส่วน B) ----------
    try:
        data = json.dumps(report).encode("utf-8")
        req = urllib.request.Request(N8N_WEBHOOK_URL, data=data,
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            logging.info(f"✅ ส่ง report ไป n8n (HTTP {resp.status})")
    except Exception as e:
        logging.warning(f"⚠️ ส่ง n8n ไม่สำเร็จ: {e}")

    return report

with DAG(
    dag_id="airplane_crashes_etl",
    default_args=default_args,
    description="ETL: airplane crashes CSV -> clean -> MySQL",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",      # 3.2: schedule รันทุกวัน
    catchup=False,
    tags=["capstone", "etl", "airplane"],
) as dag:
    t_extract   = PythonOperator(task_id="extract",   python_callable=extract)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_load      = PythonOperator(task_id="load",      python_callable=load)
    t_validate  = PythonOperator(task_id="validate",  python_callable=validate)
    t_quality_check = PythonOperator(task_id="quality_check", python_callable=quality_check)
    t_extract >> t_transform >> t_load >> t_validate >> t_quality_check
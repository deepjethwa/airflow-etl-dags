

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne

# ------------------- FUNCTION -------------------
def aggregate_hourly_events():
    MONGO_URI = "mongodb://mongo_etl:27017"
    DB_NAME = "test2"
    SOURCE_COLLECTION = "groupEventsnew"
    TARGET_COLLECTION = "hourly_event_summary"

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Pipeline: unwind events, group by hour only, push combinations
    pipeline = [
        {"$unwind": "$events"},
        {"$project": {
            "hourStart": {"$dateTrunc": {"date": {"$toDate": "$obsT"}, "unit": "hour"}},
            "efct": "$events.efct",
            "subj": "$events.subj"
        }},
        {"$group": {
            "_id": "$hourStart",
            "comb": {"$push": {"efct": "$efct", "subj": "$subj", "count": 1}},
            "totalCount": {"$sum": 1}
        }}
    ]

    results = db[SOURCE_COLLECTION].aggregate(pipeline)

    bulk_updates = []
    for doc in results:
        hour_start = doc["_id"]

        update_doc = {
            "hourStart": hour_start,
            "hourEnd": hour_start + timedelta(hours=1),
            "comb": doc["comb"],
            "totalCount": doc["totalCount"],
            "generatedAt": datetime.utcnow()
        }

        bulk_updates.append(UpdateOne(
            {"hourStart": hour_start},
            {"$set": update_doc},
            upsert=True
        ))

    if bulk_updates:
        db[TARGET_COLLECTION].bulk_write(bulk_updates)

    print(f"✅ Upserted {len(bulk_updates)} hourly summary documents (one doc per hour).")
    client.close()


# ------------------- DAG DEFINITION -------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 1),
}

with DAG(
    "hourly_event_summary_optimized",
    default_args=default_args,
    schedule_interval=None,  # manual trigger; can change to '@hourly' later
    max_active_runs=1,
    catchup=False,
    tags=["mongo", "hourly", "optimized"],
) as dag:

    hourly_task = PythonOperator(
        task_id="aggregate_hourly_events",
        python_callable=aggregate_hourly_events,
    )
























# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from pymongo import MongoClient, UpdateOne

# # ------------------- FUNCTION -------------------
# def aggregate_hourly_events():
#     MONGO_URI = "mongodb://mongo_etl:27017"
#     DB_NAME = "test2"
#     SOURCE_COLLECTION = "groupEventsnew"
#     TARGET_COLLECTION = "hourly_event_summary"

#     client = MongoClient(MONGO_URI)
#     db = client[DB_NAME]

#     pipeline = [
#         {"$unwind": "$events"},
#         {
#             "$group": {
#                 "_id": {
#                     "hourStart": {
#                         "$dateTrunc": {"date": {"$toDate": "$obsT"}, "unit": "hour"}
#                     },
#                     "efct": "$events.efct",
#                     "subj": "$events.subj"
#                 },
#                 "count": {"$sum": 1}
#             }
#         }
#     ]

#     results = db[SOURCE_COLLECTION].aggregate(pipeline)

#     bulk_updates = []
#     for doc in results:
#         hour_start = doc["_id"]["hourStart"]
#         efct = doc["_id"]["efct"]
#         subj = doc["_id"]["subj"]

#         update_doc = {
#             "efct": efct,
#             "subj": subj,
#             "hourStart": hour_start,
#             "hourEnd": hour_start + timedelta(hours=1),
#             "count": int(doc["count"]),
#             "generatedAt": datetime.utcnow()
#         }

#         bulk_updates.append(UpdateOne(
#             {"efct": efct, "subj": subj, "hourStart": hour_start},
#             {"$set": update_doc},
#             upsert=True
#         ))

#     if bulk_updates:
#         db[TARGET_COLLECTION].bulk_write(bulk_updates)

#     print(f"✅ Upserted {len(bulk_updates)} hourly summary documents (no duplicates).")
#     client.close()


# # ------------------- DAG DEFINITION -------------------
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2025, 10, 1),
# }

# with DAG(
#     "hourly_event_summary_optimized",
#     default_args=default_args,
#     schedule_interval=None,  # manual trigger; can change to '@hourly' later
#     max_active_runs=1,
#     catchup=False,
#     tags=["mongo", "hourly", "optimized"],
# ) as dag:

#     hourly_task = PythonOperator(
#         task_id="aggregate_hourly_events",
#         python_callable=aggregate_hourly_events,
#     )










# # File: /dags/mongo_historical_etl.py

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from pymongo import MongoClient
# from mongo_hourly_products_etl import aggregate_hourly_products_for_window  # Import your wrapped function

# # MongoDB collections
# SOURCE_COLLECTION = "groupEventsnew1"
# SUMMARY_COLLECTION = "hourly_event_summary"
# MONGO_URI = "mongodb://mongo_etl:27017"
# DB_NAME = "test2"

# def run_historical_backfill():
#     """
#     Historical backfill DAG task:
#     - Finds min/max timestamps
#     - Generates hourly windows
#     - Calls the wrapped aggregation function for each hour
#     """
#     client = MongoClient(MONGO_URI)
#     db = client[DB_NAME]
#     collection = db[SOURCE_COLLECTION]

#     # Find earliest and latest obsT in milliseconds
#     min_doc = collection.find_one(sort=[("_id", 1)])
#     max_doc = collection.find_one(sort=[("_id", -1)])

#     if not min_doc or not max_doc:
#         print("❌ No data found in collection.")
#         client.close()
#         return

#     start_time = datetime.utcfromtimestamp(min_doc["obsT"] / 1000)
#     end_time   = datetime.utcfromtimestamp(max_doc["obsT"] / 1000)

#     print(f"Processing historical data from {start_time} to {end_time}")

#     # Generate hourly windows
#     current_start = start_time
#     while current_start <= end_time:
#         current_end = current_start + timedelta(hours=1)
#         print(f"Processing window: {current_start} → {current_end}")

#         # Call the wrapped aggregation function
#         aggregate_hourly_products_for_window(current_start, current_end)

#         current_start = current_end

#     client.close()
#     print("✅ Historical hourly summary generation completed.")

# # ------------------- DAG Definition -------------------

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2025, 10, 1),
# }

# with DAG(
#     "mongo_historical_etl",
#     default_args=default_args,
#     schedule_interval=None,            # manual trigger '*/5 * * * *'
#     max_active_runs=1,  
#     catchup=False,
#     tags=["mongo", "historical"],
# ) as dag:

#     historical_task = PythonOperator(
#         task_id="run_historical_backfill",
#         python_callable=run_historical_backfill,
#     )





















# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# from mongo_hourly_products_etl import aggregate_hourly_products_for_window

# # ------------------- DAG Definition -------------------
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2025, 10, 1),
# }

# with DAG(
#     "mongo_historical_etl",
#     default_args=default_args,
#     schedule_interval='*/5 * * * *',   # you can test with '*/5 * * * *'
#     max_active_runs=1,
#     catchup=False,
#     tags=["mongo", "historical"],
# ) as dag:

#     historical_task = PythonOperator(
#         task_id="run_historical_backfill",
#         python_callable=aggregate_hourly_products_for_window,  # now no hourly loop needed
#     )

















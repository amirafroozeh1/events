import pandas as pd
import json
import requests
import psycopg2
from datetime import timedelta
from sqlalchemy import create_engine
from flask import jsonify, Flask
from psycopg2 import pool
import time

connection_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    host='localhost',
    database='mydb',
    user='myuser',
    password='mypassword',
    port=5433
)


def get_connection():
    return connection_pool.getconn()


def release_connection(conn):
    connection_pool.putconn(conn)


def get_events_and_return_data_frame():
    response = requests.get('https://storage.googleapis.com/xcc-de-assessment/events.json')

    lines = response.content.splitlines()

    res = []
    for line in lines:
        res.append(json.loads(line))
    return pd.json_normalize(res)


def sessionize(data_frame):
    dropped_data_frame = data_frame.dropna(subset=['event.customer-id'])
    sorted_data_frame = dropped_data_frame.sort_values(by=['event.customer-id', 'event.timestamp'])
    sorted_data_frame['timestamp'] = pd.to_datetime(sorted_data_frame['event.timestamp'])
    session_id = 0
    last_customer_id = None
    last_timestamp = None
    session_timeout = 4

    for index, row in sorted_data_frame.iterrows():
        if (last_customer_id != row['event.customer-id']) or \
                (last_timestamp is not None and (row['timestamp'] - last_timestamp) > timedelta(
                    minutes=session_timeout)):
            session_id += 1

        sorted_data_frame.at[index, 'session_id'] = session_id

        last_customer_id = row['event.customer-id']
        last_timestamp = row['timestamp']
    return sorted_data_frame


def fill_events_table(data_frame):

    conn = get_connection()

    try:
        engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)
        new_data_frame = data_frame.rename(columns={'id': 'id',
                                                    'type': 'type',
                                                    'event.user-agent': 'user_agent',
                                                    'event.ip': 'ip',
                                                    'event.customer-id': 'customer_id',
                                                    'event.page': 'page',
                                                    'event.product': 'product',
                                                    'event.query': 'query',
                                                    'event.referrer': 'referrer',
                                                    'event.position': 'position',
                                                    'session_id': 'session_id'
                                                    })

        new_data_frame.to_sql('events', engine, if_exists='replace', index=False)

    finally:
        release_connection(conn)


def median_visits_before_order_query():
    conn = get_connection()
    cur = conn.cursor()
    query = """ 
             WITH cte_one AS 
                (SELECT DISTINCT customer_id, session_id, CASE WHEN type = 'placed_order' THEN 1 ELSE 0 END as placed_order
                    FROM events
                    GROUP BY customer_id, session_id, type)

            , cte_two AS 
                (SELECT customer_id, session_id, sessions_before_place_order
                    FROM (
                          SELECT customer_id, session_id, placed_order,
                          COUNT(*) OVER(PARTITION BY customer_id ORDER BY session_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) sessions_before_place_order
                          FROM cte_one
                    ) AS subquery
                    WHERE placed_order = 1)

            , cte_three AS 
                (SELECT customer_id, sessions_before_place_order,
                    ROW_NUMBER() OVER(partition BY customer_id ORDER BY sessions_before_place_order ASC) row_number
                    FROM cte_two
                ) 

            , cte_four AS 
                (SELECT customer_id, sessions_before_place_order
                    FROM cte_three 
                    WHERE row_number = 1 
                )

            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sessions_before_place_order) AS median
            FROM cte_four;
        """

    try:
        cur.execute(query)
        return cur.fetchone()
    finally:
        cur.close()
        release_connection(conn)


def median_session_duration_minutes_before_order_query():
    conn = get_connection()
    cur = conn.cursor()
    query = """ 
             WITH cte_one AS 
                (SELECT DISTINCT customer_id, session_id, timestamp, CASE WHEN type = 'placed_order' THEN 1 ELSE 0 END as placed_order
                    FROM events
                    GROUP BY customer_id, session_id, type, timestamp
                )

            , cte_two AS 
                (SELECT customer_id, session_id, first_time_stamp, timestamp as last_time_stamp
                     FROM (
                        SELECT customer_id, session_id, placed_order, timestamp,
                        MIN(timestamp) OVER(PARTITION BY customer_id ORDER BY session_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) first_time_stamp
                        FROM cte_one
                     ) AS subquery
                    WHERE placed_order = 1)

            , cte_three AS 
                (SELECT customer_id, first_time_stamp, last_time_stamp,
                    ROW_NUMBER() OVER(partition BY customer_id ORDER BY last_time_stamp ASC) row_number
                    FROM cte_two
                )

            , cte_four AS 
                (SELECT customer_id, EXTRACT(EPOCH FROM (last_time_stamp - first_time_stamp)) / 60 AS difference_in_minutes
                FROM cte_three 
                WHERE row_number = 1 
            )

            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY difference_in_minutes) AS median
            FROM cte_four;
        """

    try:
        cur.execute(query)
        return cur.fetchone()
    finally:
        cur.close()
        release_connection(conn)


def call_metrics_endpoint():
    app = Flask(__name__)

    @app.route('/metrics/orders')
    def get_metrics():
        return jsonify([
            {'median_visits_before_order_query': median_visits_before_order},
            {'median_session_duration_minutes_before_order': median_session_duration_minutes_before_order}
        ])

    if __name__ == "__main__":
        from waitress import serve

        serve(app, host="0.0.0.0", port=8080)


current_time = time.time()
df = get_events_and_return_data_frame()
print("The event API call has finished in", time.time() - current_time, "seconds")

current_time = time.time()
sessionize_data = sessionize(df)
print("Sessionization completed in", time.time() - current_time, "seconds")

current_time = time.time()
fill_events_table(sessionize_data)
print("Filling the database completed in", time.time() - current_time, "seconds")

current_time = time.time()
median_visits_before_order = median_visits_before_order_query()
median_session_duration_minutes_before_order = median_session_duration_minutes_before_order_query()
print("Executing queries finished in", time.time() - current_time, "seconds")

print("Calling the metrics endpoint at 'http://localhost:8080/metrics/orders'")
call_metrics_endpoint()


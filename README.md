# Data Engineering Assessment

The main goal of this project is to analyze the events, sessionize them and execute the required queries.

# Overview of the Sessionize Function

The sessionize function first drops any rows from the input DataFrame where the `event.customer-id` column is missing. Then, it sorts the remaining rows in the ascending order, first by `event.customer-id` and then by `event.timestamp`.

Next, the function converts the `event.timestamp` column to a pandas datetime object.

The sessionize function then iterates over each row in the sorted DataFrame, assigning a session ID to each row based on the `event.customer-id` and `event.timestamp` columns. 
If the current row's `event.customer-id` is different from the previous row's `event.customer-id`, or if the time elapsed since the previous event is greater than the session timeout value, the function increments the session ID counter. It then assigns the current session ID to the current row.
For this assignment, I have chosen the value of the session time out be 4 minutes.

Finally, the function returns the sorted DataFrame with the new `session_id` column added.


# Overview of the Queries

## median_visits_before_order:

This SQL query calculates the median number of sessions per customer that occurred before the customer's first placed order (This includes the session in which the order is placed. For example, if the order is placed in the first session, I count it as one). It does this by using a series of CTEs (Common Table Expressions) to perform the necessary calculations, and then using the `PERCENTILE_CONT` function to calculate the median.

The first CTE, `cte_one`, selects the distinct `customer_id`, `session_id`, and if an event represents a placed order (1 when it represents a placed order and 0 otherwise) from the events table. The events are merged into one session by grouping customer_id, session_id, and event type.

The second CTE, `cte_two`, selects the `customer_id`, `session_id`, and the number of sessions that occurred before the first placed order for each customer. It does this by using the `COUNT(*) OVER()` function to count the number of rows for each `customer_id` before the first placed order.

The third CTE, `cte_three`, assigns a row number to each row in `table_two` based on the number of sessions that occurred before the first placed order for each customer, and then groups these rows by `customer_id`.

The fourth CTE, `cte_four`, selects the first row for each customer from `table_three`, which represents the "number of sessions that occurred before the first placed order" for that customer.

Finally, the `SELECT` statement uses the `PERCENTILE_CONT` function to calculate the median of the "before" column from `cte_four`, which represents the number of sessions that occurred before the first placed order for each customer.


## median_session_duration_minutes_before_order:

This SQL query computes the median time between the first and last events of a customer's session where the event type is `placed_order`. The query achieves this by using four Common Table Expressions (CTEs) and a final `SELECT` statement.

The first CTE (`cte_one`) selects the distinct `customer_id`, `session_id`, `timestamp`, and whether the event type is `placed_order`. 
It will merge events into one session by grouping customer_id, session_id, and timestamp and event type.

The second CTE (`cte_two`) selects the `customer_id`, `session_id`, `timestamp` of the first event, and the timestamp of the last event in each session where an order is placed. 
It uses the first CTE as a source and uses the window function `MIN` to find the first timestamp in each customer's session` where an `order is placed.

The third CTE (`cte_three`) selects the `customer_id`, `first_time_stamp`, `last_time_stamp`, and assigns a row number to each customer's session based on the last timestamp. The row number is used in the next CTE to select only the row with the smallest row number.

The fourth CTE (`cte_four`) selects the `customer_id` and calculates the time difference between the first and last event in each session in minutes. It only selects the row with the smallest row number from `cte_three`.

Finally, the `SELECT` statement uses the `PERCENTILE_CONT` function with an argument of 0.5 to calculate the median of the `difference_in_minutes` column in `cte_four`.

# Running the Application

Before running the application, make sure that the following tools are available in the command line path. (Please note that I have only tested this application on macOS)

- `Docker-Desktop` or (`docker` and `docker compose`) for running the containers for Postgres
- `python3` 

To run the application, run the provided script `run.sh`. This script spins up the Docker container for Postgres database, fetches the json data, runs the sessionize function and stores the results in the database. Then, it executes the two queries explained above. The results of the executed queries will be available at this address: http://localhost:8080/metrics/orders

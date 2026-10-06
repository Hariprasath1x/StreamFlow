# Power BI Dashboard

Since this project simulates a Data Engineering architecture, automating a Power BI report through code is out of scope (and practically impossible natively on macOS). However, the infrastructure is fully prepared for a business intelligence tool.

## Connecting Power BI to PostgreSQL

To build the dashboard on a supported Windows machine (or VM):

1. **Open Power BI Desktop**.
2. Click **Get Data** -> **PostgreSQL database**.
3. **Server**: `localhost` (or the IP of your Docker host).
4. **Database**: `streamflow`.
5. **Data Connectivity mode**: Import (or DirectQuery for real-time updates).
6. Enter the credentials (User: `postgres`, Password: `secretpassword`).
7. Select the `processed_orders` table and load.

## Intended Dashboard Design

The analytical schema supports the following visualizations:

1. **Total Revenue (Card):** Sum of `total_amount`.
2. **Total Orders (Card):** Count of `order_id`.
3. **Average Order Value (Card):** Average of `total_amount`.
4. **Revenue by Category (Pie Chart):** Sum of `total_amount` grouped by `category`.
5. **Revenue by City (Map/Bar Chart):** Sum of `total_amount` grouped by `city`.
6. **Orders Over Time (Line Chart):** Count of `order_id` grouped by `order_date` and `order_hour`.
7. **Top Products (Table):** List of `product` sorted by sum of `total_amount` descending.

These metrics provide a comprehensive view of e-commerce operations in near real-time as data streams through Kafka and PySpark into the warehouse.

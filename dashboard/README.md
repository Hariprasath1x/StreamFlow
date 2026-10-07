# STREAMFLOW — E-COMMERCE ANALYTICS

## 1. Dashboard Purpose
This analytics layer serves as the final downstream consumer of the StreamFlow real-time data pipeline. It visualizes the processed event logs residing in PostgreSQL to monitor e-commerce business KPIs and data pipeline quality. 

*Note: Power BI Desktop was not available in this environment, so the dashboard is specified below but could not be physically created as a .pbix file.*

## 2. PostgreSQL Connection Details
To reproduce this dashboard locally on a machine with Power BI Desktop:

- **Source:** PostgreSQL Database
- **Server:** `localhost`
- **Port:** `5432`
- **Database:** `streamflow`
- **Data Connectivity Mode:** Import
- **Credentials:** 
  - **User:** `postgres`
  - **Password:** *(Use the local password configured in your `.env` or `docker-compose.yml`)*

## 3. Source Tables & Views
The dashboard primarily consumes a dedicated SQL view to keep the analytics layer simple and decoupled from raw tables:
- **`powerbi_order_analytics`** (Derived from `processed_orders`)
- **`invalid_orders`** (Used exclusively for the Data Quality metric)

## 4. Dashboard KPIs & Required DAX Measures
Create the following simple DAX measures:

- **Total Orders** = `COUNT(powerbi_order_analytics[order_id])`
- **Total Revenue** = `SUM(powerbi_order_analytics[total_amount])`
- **Average Order Value** = `AVERAGE(powerbi_order_analytics[total_amount])`
- **Total Quantity** = `SUM(powerbi_order_analytics[quantity])`
- **Valid Records** = `COUNTROWS(powerbi_order_analytics)`
- **Invalid Records** = `COUNTROWS(invalid_orders)`
- **Data Quality %** = `DIVIDE([Valid Records], [Valid Records] + [Invalid Records], 0)`

## 5. Visuals Included
The professional single-page layout consists of:

**Section 1 — KPI Cards**
- Total Orders
- Total Revenue
- Average Order Value
- Total Quantity Sold

**Section 2 — Revenue Analysis**
- **Revenue by Category** (Donut / Pie Chart)
- **Revenue Trend by Order Date** (Line Chart)

**Section 3 — Operational Analysis**
- **Orders by City** (Bar Chart)
- **Orders by Hour** (Column Chart)

**Section 4 — Product Analysis**
- **Top 10 Products by Revenue** (Table or Matrix)

**Section 5 — Data Quality**
- Valid Records (Card)
- Invalid Records (Card)
- Data Quality % (Gauge or Card)

## 6. Steps to Reproduce
1. Start the StreamFlow Docker infrastructure.
2. Produce records using the Kafka python generator.
3. Allow PySpark to micro-batch the data into PostgreSQL.
4. Open Power BI Desktop, connect using the credentials above, and load the view.
5. Create the DAX measures and assemble the visual sections.

## 7. Screenshot Instructions
Once the dashboard is successfully built and populated with data, take a screenshot of the single-page layout and save it as `dashboard/preview.png` in this repository to showcase the final output.

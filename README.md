# 📘 README – Cricsheet Cricket Analysis

## 📌 Project Overview
This project analyzes cricket match data (from [Cricsheet](https://cricsheet.org/)) using:
- **Python + MySQL** for ingestion & storage  
- **SQL queries** for insights  
- **Power BI** for visualization  

---

## 🏗 Project Workflow
1. **Data Download**  
   - Python script uses Selenium/requests to fetch JSON match files from Cricsheet.  
   - Example: 10–12 sample matches (ODI, T20, Test).  

2. **Data Processing**  
   - JSON files parsed into structured Pandas DataFrames.  
   - Data inserted into MySQL (`cricket_analysis_db`).  

   **Tables:**
   - `matches` → metadata (match_id, date, teams, venue, result)  
   - `deliveries` → ball-by-ball data (match_id, batter, bowler, runs, dismissal, etc.)

3. **SQL Queries**  
   - 10 prepared queries for analysis (top run scorers, wicket takers, win rates, etc.).  
   - File: `cricsheet_analysis_queries.sql`  

4. **Exploratory Data Analysis (EDA)**  
   - Python (Matplotlib, Seaborn, Plotly) to check distributions and trends.  

5. **Power BI Dashboard**  
   - Connects directly to MySQL.  
   - Pre-built KPIs, charts, and matrix for cricket insights.  

---

## ⚙️ Setup Instructions

### 1. MySQL Installation
- Install **MySQL Server 8.0**  
- Create database:
  ```sql
  CREATE DATABASE cricket_analysis_db;
  ```

### 2. Python Setup
```bash
pip install mysql-connector-python pandas matplotlib seaborn plotly requests
```

Run ETL script:
```bash
python cricsheet_scraper.py --out ./cricsheet_jsons
```

This populates the `innings`,`matches`, `teams` and `deliveries` tables.

### 3. SQL Queries
Run `cricsheet_analysis_queries.sql` in MySQL Workbench or any client to explore insights.

### 4. Power BI Setup
1. Install **Power BI Desktop (x64)** (from Microsoft Download Center, not the Store).  
2. Install **MySQL Connector/NET 8.0 (x64)** from MySQL official site.  
3. In Power BI:  
   - **Get Data → MySQL Database**  
   - Server: `localhost` (or `hostname:3306`)  
   - Database: `cricket_analysis_db`  
   - Enter credentials  

---

## 📊 Power BI Dashboard Layout
### Visuals Included
- **KPI Cards**:  
  - Total Runs  
  - Total Wickets  
  - Sixes  
  - Fours  

- **Line Chart**: Runs per Match over Time  
- **Stacked Bar Chart**: Win % by Team  
- **Pie Chart**: Dismissal Modes  
- **Matrix**: Player vs Format (Runs, Strike Rate, Economy)  

### Key Measures (DAX)
```DAX
Total Runs = SUM(deliveries[runs_total])

Total Wickets = COUNTROWS(FILTER(deliveries, NOT ISBLANK(deliveries[wicket_kind])))

Total Sixes = COUNTROWS(FILTER(deliveries, deliveries[runs_batter] = 6))

Total Fours = COUNTROWS(FILTER(deliveries, deliveries[runs_batter] = 4))

Player Runs = SUM(deliveries[runs_batter])

Player Strike Rate =
DIVIDE(SUM(deliveries[runs_batter]) * 100, COUNTROWS(deliveries))

Player Economy =
DIVIDE(SUM(deliveries[runs_total]) * 6, COUNTROWS(deliveries))
```

---

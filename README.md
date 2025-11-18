

## 📋 Project Overview

This project demonstrates an end-to-end serverless data processing pipeline on AWS. The system automatically ingests raw order data, processes it using Lambda functions, catalogs it with AWS Glue, and provides SQL query capabilities through Amazon Athena. Finally, results are visualized on a live web dashboard hosted on an EC2 instance.

### Architecture Components:
- **Amazon S3**: Data storage with organized folder structure
- **AWS Lambda**: Serverless data processing and filtering
- **AWS Glue**: Automated data cataloging and schema detection
- **Amazon Athena**: SQL query engine for S3 data
- **Amazon EC2**: Web server hosting the analytics dashboard
- **IAM**: Security and access management between services

---

## 🏗️ Implementation Approach

### High-Level Workflow:
1. Raw order data (CSV) is uploaded to S3 `raw/` folder
2. S3 trigger automatically invokes Lambda function
3. Lambda processes and filters the data, saving results to `processed/` folder
4. Glue Crawler scans processed data and creates a queryable table schema
5. Athena executes SQL queries on the cataloged data
6. EC2-hosted Flask application displays query results in a web dashboard

---

## 1️⃣ Amazon S3 Bucket Structure 🪣

### Setup Steps:
1. Created S3 bucket: `aws-orders-pipeline-avinash` in `us-east-1` region
2. Established three-folder architecture:
   - **`raw/`**: Stores incoming raw CSV files (Orders.csv)
   - **`processed/`**: Contains Lambda-filtered data (filtered_Orders.csv)
   - **`enriched/`**: Holds Athena query results

### Purpose:
This structure separates data by processing stage, enabling clear data lineage tracking and preventing accidental overwrites. The organized hierarchy also simplifies IAM permission management and debugging.

**📸 Screenshot: S3 Bucket Structure**

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/4e69db9a-f580-4e0c-831a-8f1b65d7db05" />



*Screenshot shows the three folders (raw, processed, enriched) in the S3 bucket*

---

## 2️⃣ IAM Roles and Permissions 🔐

Created three IAM roles with principle of least privilege to enable secure service-to-service communication:

### Role 1: Lambda Execution Role
- **Name:** `Lambda-S3-Processing-Role`
- **Purpose:** Allows Lambda to read from S3 and write processed data
- **Attached Policies:**
  - `AWSLambdaBasicExecutionRole` - CloudWatch Logs access
  - `AmazonS3FullAccess` - Read/write S3 permissions

### Role 2: Glue Service Role
- **Name:** `Glue-S3-Crawler-Role`
- **Purpose:** Enables Glue Crawler to scan S3 data and create catalog entries
- **Attached Policies:**
  - `AmazonS3FullAccess` - Access to S3 processed folder
  - `AWSGlueConsoleFullAccess` - Glue console operations
  - `AWSGlueServiceRole` - Core Glue service permissions

### Role 3: EC2 Instance Profile
- **Name:** `EC2-Athena-Dashboard-Role`
- **Purpose:** Grants EC2 instance permission to execute Athena queries and read results
- **Attached Policies:**
  - `AmazonS3FullAccess` - Read query results from S3
  - `AmazonAthenaFullAccess` - Execute and manage Athena queries

**📸 Screenshot: IAM Roles Created**

<img width="1920" height="1080" alt="Screenshot 2025-11-10 183131" src="https://github.com/user-attachments/assets/d3aa6b8f-634b-419c-90dd-00c3d5cbeb48" />


*Screenshot displays all three IAM roles in the IAM console*

---

## 3️⃣ Lambda Function Creation ⚙️

### Function Configuration:
- **Function Name:** `FilterAndProcessOrders`
- **Runtime:** Python 3.9
- **Execution Role:** `Lambda-S3-Processing-Role`
- **Timeout:** 1 minute (increased from default 3 seconds)

### Lambda Function Logic:
The Lambda function implements business logic to filter order data:

**Filtering Criteria:**
- **Removes** orders with status `pending` or `cancelled` that are older than 30 days
- **Retains** all recent orders (within 30 days) regardless of status
- **Retains** all `shipped` and `confirmed` orders regardless of age

**Processing Flow:**
1. Triggered by S3 event when CSV uploaded to `raw/` folder
2. Reads raw CSV file from S3 using boto3
3. Parses CSV and applies date-based and status-based filters
4. Writes filtered results to `processed/` folder with `filtered_` prefix
5. Logs processing statistics (total records, filtered count, retained count)

**Why This Filtering?**
This removes stale pending/cancelled orders while preserving completed transactions and recent activity, ensuring downstream analytics focus on actionable and relevant data.

**📸 Screenshot: Lambda Function Created**

<img width="1919" height="878" alt="Screenshot 2025-11-17 224944" src="https://github.com/user-attachments/assets/7403a255-e5c7-4134-b968-a26a1f563a46" />

*Screenshot shows the Lambda function with Python code visible and "Changes deployed" confirmation*

---

## 4️⃣ S3 Trigger Configuration ⚡

### Trigger Setup:
- **Event Source:** S3 bucket `aws-orders-pipeline-avinash`
- **Event Type:** All object create events
- **Prefix:** `raw/` (ensures trigger only fires for files in raw folder)
- **Suffix:** `.csv` (only processes CSV files)

### Why This Configuration?
The prefix and suffix filters prevent infinite loops (Lambda won't trigger on its own output in `processed/`) and ensure only relevant file types are processed. This event-driven architecture eliminates the need for scheduled jobs or manual intervention.

### Testing the Pipeline:
1. Uploaded `Orders.csv` to `s3://aws-orders-pipeline-avinash/raw/`
2. Lambda automatically triggered within seconds
3. Verified `filtered_Orders.csv` appeared in `processed/` folder
4. Checked CloudWatch Logs for processing statistics

**📸 Screenshot: S3 Trigger Configuration**

<img width="1920" height="1080" alt="Screenshot 2025-11-10 180208" src="https://github.com/user-attachments/assets/9b0d37f4-f3d4-497e-9a6f-82cdfc8ad33e" />


*Screenshot shows the Function overview diagram with S3 connected to Lambda*

**📸 Screenshot: Processed CSV File in S3**

<img width="1920" height="1080" alt="Screenshot 2025-11-10 175731" src="https://github.com/user-attachments/assets/6d98e08d-24bc-4a10-b2c4-9c578e2d2284" />


*Screenshot displays filtered_Orders.csv in the processed/ folder with timestamp and file size*

---

## 5️⃣ AWS Glue Crawler Setup 🕸️

### Glue Database:
- **Database Name:** `orders_db`
- **Purpose:** Logical container for table metadata

### Crawler Configuration:
- **Crawler Name:** `orders_processed_crawler`
- **Data Source:** `s3://aws-orders-pipeline-avinash/processed/`
- **IAM Role:** `Glue-S3-Crawler-Role`
- **Target Database:** `orders_db`
- **Schedule:** On-demand (manual execution)

### Crawler Execution:
1. Ran crawler manually from Glue console
2. Crawler scanned processed CSV files in S3
3. Automatically inferred schema (column names and data types)
4. Created table named `processed` in `orders_db` database

### Result:
The Glue Data Catalog now contains a table with the following schema:
- `orderid` - string
- `customerid` - string  
- `orderdate` - string (date format)
- `status` - string
- `amount` - double (numeric)

**Why Glue Crawler?**
Manual schema definition is error-prone and time-consuming. Glue Crawler automates schema discovery, making data immediately queryable in Athena without writing DDL statements.

**📸 Screenshot: Glue Crawler CloudWatch Logs**


<img width="1919" height="810" alt="Screenshot 2025-11-17 224827" src="https://github.com/user-attachments/assets/23884dec-4152-42c2-b5da-4a6d9eb8ed95" />

*Screenshot shows crawler run history with "Succeeded" status and "Tables added: 1"*

---

## 6️⃣ Amazon Athena Queries 🔍

### Athena Configuration:
- **Data Source:** AwsDataCatalog
- **Database:** `orders_db`
- **Table:** `processed`
- **Query Result Location:** `s3://aws-orders-pipeline-avinash/enriched/`

### Queries Executed:

#### Query 1: Total Sales by Customer
**Business Question:** Which customers have spent the most?
```sql
SELECT 
    customerid,
    SUM(amount) AS total_spent,
    COUNT(orderid) AS total_orders
FROM processed
GROUP BY customerid
ORDER BY total_spent DESC;
```

**Insight:** Identifies high-value customers for targeted marketing and retention strategies.

---

#### Query 2: Monthly Order Volume and Revenue
**Business Question:** What are our monthly sales trends?
```sql
SELECT 
    DATE_FORMAT(DATE_PARSE(orderdate, '%Y-%m-%d'), '%Y-%m') AS month,
    COUNT(orderid) AS total_orders,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value
FROM processed
GROUP BY DATE_FORMAT(DATE_PARSE(orderdate, '%Y-%m-%d'), '%Y-%m')
ORDER BY month DESC;
```

**Insight:** Reveals seasonal patterns and growth trends to inform inventory and staffing decisions.

---

#### Query 3: Order Status Dashboard
**Business Question:** How are orders distributed by fulfillment status?
```sql
SELECT 
    status,
    COUNT(orderid) AS order_count,
    SUM(amount) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_order_value
FROM processed
GROUP BY status
ORDER BY order_count DESC;
```

**Insight:** Monitors operational efficiency and identifies potential fulfillment bottlenecks.

---

#### Query 4: Average Order Value (AOV) per Customer
**Business Question:** Which customers place the highest-value orders on average?
```sql
SELECT 
    customerid,
    COUNT(orderid) AS number_of_orders,
    SUM(amount) AS total_spent,
    ROUND(AVG(amount), 2) AS avg_order_value
FROM processed
GROUP BY customerid
HAVING COUNT(orderid) >= 2
ORDER BY avg_order_value DESC;
```

**Insight:** Segments customers by purchase behavior for personalized upselling opportunities.

---

#### Query 5: Top 10 Largest Orders in February 2025
**Business Question:** What were the highest-value transactions last month?
```sql
SELECT 
    orderid,
    customerid,
    orderdate,
    status,
    amount
FROM processed
WHERE orderdate >= '2025-02-01' 
    AND orderdate < '2025-03-01'
ORDER BY amount DESC
LIMIT 10;
```

**Insight:** Highlights major deals for sales team recognition and customer relationship management.

---

**📸 Screenshot: Athena Query Results in S3 Enriched Folder**


<img width="1920" height="1080" alt="Screenshot 2025-11-17 224634" src="https://github.com/user-attachments/assets/c0cfee3c-8cb5-4005-afb8-7b10ef3d776b" />

*Screenshot shows multiple CSV files in the enriched/ folder representing query results*

---

## 7️⃣ EC2 Web Dashboard Deployment 🖥️

### EC2 Instance Configuration:
- **Instance Name:** `Athena-Dashboard-Server`
- **AMI:** Amazon Linux 2023
- **Instance Type:** t2.micro (Free Tier eligible)
- **Key Pair:** `athena-dashboard-key.pem`
- **IAM Instance Profile:** `EC2-Athena-Dashboard-Role`

### Security Group Rules:
- **SSH (Port 22):** Restricted to my IP for secure access
- **HTTP (Port 5000):** Open to 0.0.0.0/0 for public web access

### Software Installation:
Connected via SSH and installed required dependencies:
```bash
sudo yum update -y
sudo yum install python3-pip -y
pip3 install Flask boto3
```

### Application Deployment:
Created `app.py` Flask application with configuration:
- **AWS Region:** us-east-1
- **Athena Database:** orders_db
- **S3 Output Location:** s3://aws-orders-pipeline-avinash/enriched/
- **Table Name:** processed (updated from default "filtered_orders")

### Application Architecture:
The Flask app:
1. Defines 5 SQL queries matching Athena queries
2. Uses boto3 to execute queries via Athena API
3. Polls for query completion status
4. Fetches results from S3 enriched folder
5. Parses CSV results and renders HTML tables
6. Serves responsive dashboard on port 5000

### Running the Dashboard:
```bash
python3 app.py
```
Accessed via: `http://[EC2-PUBLIC-IP]:5000`

**📸 Screenshot: Final Webpage Dashboard**


<img width="1920" height="1080" alt="Screenshot 2025-11-10 204153" src="https://github.com/user-attachments/assets/8aa1af5a-bd08-412f-9478-2d49b310b747" />

*Screenshot shows the browser displaying "📊 Athena Orders Dashboard" with all 5 query results rendered as HTML tables*

---

## 🎯 Key Learnings

### Technical Skills Gained:
1. **Serverless Architecture:** Understanding event-driven computing without managing servers
2. **Data Pipeline Design:** Implementing ETL (Extract, Transform, Load) patterns in the cloud
3. **IAM Security:** Configuring least-privilege access between AWS services
4. **SQL Analytics:** Writing complex aggregation queries for business intelligence
5. **Full-Stack Integration:** Connecting backend services to frontend visualization

### Challenges Overcome:
1. **Table Name Mismatch:** Glue auto-named the table "processed" instead of expected "filtered_orders" - required updating all queries in Flask app
2. **Lambda Timeout:** Default 3-second timeout was insufficient for processing - increased to 1 minute
3. **Security Group Configuration:** Initially forgot to open port 5000, preventing web access - added custom TCP rule
4. **IAM Role Attachment:** EC2 instance must have IAM role attached at launch; cannot modify after launch without stopping instance

### Best Practices Applied:
- ✅ Organized S3 folder structure for clear data lineage
- ✅ Used IAM roles instead of hardcoded credentials
- ✅ Implemented event-driven processing (no polling/scheduling)
- ✅ Leveraged serverless services to minimize operational overhead
- ✅ Documented code with comments and logging statements

---

## 💰 Cost Considerations

### Free Tier Usage:
- **S3:** First 5GB storage free (used ~5MB)
- **Lambda:** 1M requests + 400K GB-seconds compute free (used ~5 invocations)
- **Glue:** 1M objects stored + 1M requests free (used 1 crawler run)
- **Athena:** 10GB scanned data free per month (used ~1MB)
- **EC2:** 750 hours t2.micro free per month (used ~2 hours)

**Total Estimated Cost:** $0.00 (within Free Tier limits)

---

## 🛠️ Resource Cleanup

To avoid future charges, I stopped the EC2 instance after completing the assignment. For complete cleanup:
```bash
# Stop EC2 instance (preserves setup for future demos)
AWS Console → EC2 → Instances → Stop Instance

# Optional: Complete deletion after grading
- Terminate EC2 instance
- Empty and delete S3 bucket
- Delete Lambda function
- Delete Glue crawler and database
- Delete IAM roles
```

---

## 📚 Repository Structure
```
ITCS-6190-Assignment-3/
├── Orders.csv                    # Raw dataset
├── LambdaFunction.py            # Lambda processing code
├── EC2InstanceNANOapp.py        # Flask dashboard application
├── README.md                     # This file

```

---

## 🎓 Conclusion

This assignment provided hands-on experience building a production-ready serverless data pipeline on AWS. The project demonstrates how modern cloud architectures enable automated, scalable, and cost-effective data processing workflows. Key takeaways include understanding event-driven computing, implementing security best practices with IAM, and integrating multiple AWS services to solve real-world data analytics challenges.

The automated pipeline successfully:
- ✅ Processes raw order data with business logic filtering
- ✅ Catalogs data for SQL analytics without database administration
- ✅ Provides interactive web dashboard for stakeholder insights
- ✅ Operates entirely on serverless infrastructure (zero server management)



**AWS Services Used:** 6 (S3, Lambda, IAM, Glue, Athena, EC2)

---

**End of README**

import boto3
import time
from flask import Flask

AWS_REGION = "us-east-2"
ATHENA_DATABASE = "orders_db"
S3_OUTPUT_LOCATION = "s3://aws-orders-pipeline-avinash/enriched/"
TABLE = "orders_pipeline_avinash"   # <- change if Glue created a different name

app = Flask(__name__)
athena = boto3.client('athena', region_name=AWS_REGION)

QUERIES = [
    {
        "title": "1. Total Sales by Customer",
        "sql": f"""
            SELECT customer, SUM(CAST(amount AS DOUBLE)) AS total_spent
            FROM "{TABLE}"
            GROUP BY customer
            ORDER BY total_spent DESC;
        """
    },
    {
        "title": "2. Monthly Orders + Revenue",
        "sql": f"""
            SELECT DATE_TRUNC('month', CAST(orderdate AS DATE)) AS month,
                   COUNT(*) AS orders,
                   SUM(CAST(amount AS DOUBLE)) AS revenue
            FROM "{TABLE}"
            GROUP BY 1
            ORDER BY 1;
        """
    },
    {
        "title": "3. Orders by Status",
        "sql": f"""
            SELECT LOWER(status) AS status, COUNT(*) AS count
            FROM "{TABLE}"
            GROUP BY 1;
        """
    },
    {
        "title": "4. Average Order Value per Customer",
        "sql": f"""
            SELECT customer, AVG(CAST(amount AS DOUBLE)) AS aov
            FROM "{TABLE}"
            GROUP BY customer
            ORDER BY aov DESC;
        """
    },
    {
        "title": "5. Top 10 Orders Feb 2025",
        "sql": f"""
            SELECT orderdate, orderid, customer, CAST(amount AS DOUBLE) AS amount
            FROM "{TABLE}"
            WHERE CAST(orderdate AS DATE)
                  BETWEEN DATE '2025-02-01' AND DATE '2025-02-28'
            ORDER BY amount DESC
            LIMIT 10;
        """
    }
]

def run_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION}
    )
    qid = response['QueryExecutionId']

    while True:
        result = athena.get_query_execution(QueryExecutionId=qid)
        status = result['QueryExecution']['Status']['State']
        if status in ['FAILED', 'CANCELLED']:
            return None, f"Athena FAILED: {result['QueryExecution']['Status'].get('StateChangeReason', '')}"
        if status == 'SUCCEEDED':
            break
        time.sleep(1)

    results = athena.get_query_results(QueryExecutionId=qid)
    rows = results['ResultSet']['Rows']
    header = [col['VarCharValue'] for col in rows[0]['Data']]
    data = []
    for row in rows[1:]:
        data.append([cell.get('VarCharValue', '') for cell in row['Data']])
    return header, data

@app.route('/')
def index():
    html = "<h1>Athena Dashboard</h1>"
    for q in QUERIES:
        html += f"<h2>{q['title']}</h2>"
        header, data = run_query(q['sql'])
        if header is None:
            html += f"<p style='color:red;'>Error: {data}</p>"
            continue

        html += "<table border='1' cellpadding='5'>"
        html += "<tr>" + "".join(f"<th>{h}</th>" for h in header) + "</tr>"
        for row in data:
            html += "<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>"
        html += "</table><br>"

    return html

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

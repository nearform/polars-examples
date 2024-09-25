import polars as pl
from google.cloud import bigquery
import os    

credential_path = "..."
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

client = bigquery.Client()

# Perform a query.
# QUERY = (
#     'SELECT *, JSON_EXTRACT(payload, "$.action") as action '
#     'FROM `github-archive-319308.nearform_github.nearform_gharchive_data` '
#     "WHERE repo.name = 'nodejs/node'")

QUERY = (
    'SELECT actor.login, COUNT(*) as count FROM ( '
        "SELECT  created_at, actor, JSON_EXTRACT(payload, '$.action') as action, "
        'FROM `github-archive-319308.nearform_github.nearform_gharchive_data`  '      
        "WHERE type = 'PullRequestEvent'"
    ')'
    'WHERE action = \'"opened"\''
    'AND DATE(created_at) >= DATE_TRUNC(CURRENT_DATE(), YEAR)'
    'GROUP BY actor.login'
    'ORDER BY count DESC')

query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

df = pl.from_arrow(rows.to_arrow())

# Display the result
print(df)
from google.cloud import bigquery

class Big_query_executor():

    def __init__(self,project_name) -> None:
        self.client=bigquery.Client(project=project_name)

    def execute_query(self,query):
        """
        Execute a BigQuery SQL in order to change tables with the result of a query.

        Args:
            query (str): BigQuery query whose result will be used to create or replace the table.
        """
        query_job = self.client.query(query)
        query_job.result()

  
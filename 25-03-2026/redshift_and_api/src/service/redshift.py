import logging
import time

from src.config.settings import WORK_GROUP_NAME, DATABASE
from src.service.client import Client

def execute_query(query):
    try:
        client = Client.get_redshift_client()
        
        response = client.execute_statement(
            WorkgroupName=WORK_GROUP_NAME,
            Database=DATABASE,
            Sql=query
        )

        query_id = response['Id']
        logging.info(f"Query ID: {query_id}")

        while True:
            status_response = client.describe_statement(Id=query_id)
            status = status_response['Status']
            
            if status in ['FINISHED', 'FAILED', 'ABORTED']:
                break

            logging.info(f"Query running... Status: {status}")
            time.sleep(2)

        if status == 'FINISHED':
            if status_response.get('HasResultSet'):
                return client.get_statement_result(Id=query_id)
            else:
                logging.info("Query executed successfully")
                return status_response

        elif status == 'FAILED':
            error_message = status_response.get('Error', 'Unknown error')
            logging.error(f"Query FAILED with error: {error_message}")
            return None
        
        elif status == 'ABORTED':
            logging.error("Query was aborted")
            return None

    except Exception as e:
        logging.error(f"Exception occurred: {e}")
        return None
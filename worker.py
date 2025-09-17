import pika
import json
import base64
import configparser
import logging
import requests
import time
import os

from docusign_esign import ApiClient
from simple_salesforce import Salesforce

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
config = configparser.ConfigParser()
config.read('config.ini')

# DocuSign API Configuration
DS_CONFIG = config['DOCUSIGN']
DS_INTEGRATOR_KEY = DS_CONFIG['integrator_key']
DS_USER_ID = DS_CONFIG['user_id']
DS_ACCOUNT_ID = DS_CONFIG['account_id']
DS_PRIVATE_KEY_PATH = DS_CONFIG['private_key_file']

# Salesforce API Configuration
SF_CONFIG = config['SALESFORCE']
SF_USERNAME = SF_CONFIG['username']
SF_PASSWORD = SF_CONFIG['password']
SF_SECURITY_TOKEN = SF_CONFIG['security_token']

# RabbitMQ Configuration (from environment variable)
RABBITMQ_URL = os.environ.get('RABBITMQ_URL')
QUEUE_NAME = 'docusign_jobs'

if not RABBITMQ_URL:
    logging.critical("FATAL ERROR: RABBITMQ_URL environment variable not set for worker.")
    raise ValueError("No RABBITMQ_URL set for the Worker application")

# --- Helper Functions ---
def get_jwt_token():
    """Generates a JWT access token for DocuSign API calls."""
    api_client = ApiClient()
    try:
        with open(DS_PRIVATE_KEY_PATH, 'r') as private_key_file:
            private_key = private_key_file.read()
        
        token_response = api_client.request_jwt_user_token(
            client_id=DS_INTEGRATOR_KEY,
            user_id=DS_USER_ID,
            oauth_host_name=config['DOCUSIGN']['authorization_server'],
            private_key_bytes=private_key,
            expires_in=3600,
            scopes=["signature", "impersonation"]
        )
        return token_response.access_token
    except Exception as e:
        logging.error(f"Error getting DocuSign JWT token: {e}")
        raise

def get_docusign_api_client():
    """Authenticates with DocuSign using JWT and returns an API client."""
    try:
        api_client = ApiClient()
        api_client.host = f"https://{config['DOCUSIGN']['api_server']}/restapi"
        api_client.set_default_header("Authorization", "Bearer " + get_jwt_token())
        return api_client
    except Exception as e:
        logging.error(f"Error creating DocuSign API client: {e}")
        raise

def get_salesforce_client():
    """Authenticates with Salesforce and returns a client instance."""
    try:
        return Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_SECURITY_TOKEN)
    except Exception as e:
        logging.error(f"Error creating Salesforce client: {e}")
        raise

# --- Main Processing Logic ---
def process_webhook_job(webhook_data):
    """Contains all the logic from your original webhook handler."""
    if webhook_data.get('event') != 'envelope-completed':
        logging.info(f"Ignoring non-completed event: {webhook_data.get('event')}")
        return True

    try:
        envelope_id = webhook_data['data']['envelopeId']
        logging.info(f"Processing job for envelope ID: {envelope_id}")

        docusign_api_client = get_docusign_api_client()
        sf_client = get_salesforce_client()

        # Get Contract Number from DocuSign Custom Fields
        custom_fields_url = f"{docusign_api_client.host}/v2.1/accounts/{DS_ACCOUNT_ID}/envelopes/{envelope_id}/custom_fields"
        headers = {'Authorization': docusign_api_client.default_headers['Authorization']}
        response = requests.get(custom_fields_url, headers=headers)
        response.raise_for_status()
        custom_fields_data = response.json()
        
        contract_number = None
        if custom_fields_data.get('textCustomFields'):
            for field in custom_fields_data['textCustomFields']:
                if field.get('name') == 'contractNumber':
                    contract_number = field.get('value')
                    break
        
        if not contract_number:
            logging.error(f"FATAL: Custom field 'contractNumber' not found for envelope {envelope_id}.")
            return False

        logging.info(f"Found Contract Number: {contract_number}")

        # Query Salesforce for the Contract ID
        soql_query = f"SELECT Id FROM Contract WHERE ContractNumber = '{contract_number}'"
        query_result = sf_client.query(soql_query)

        if query_result['totalSize'] != 1:
            logging.error(f"FATAL: Found {query_result['totalSize']} contracts with number {contract_number}. Expected 1.")
            return False
        
        salesforce_contract_id = query_result['records'][0]['Id']
        logging.info(f"Found Salesforce Contract ID: {salesforce_contract_id}")

        # Download the document
        doc_download_url = f"{docusign_api_client.host}/v2.1/accounts/{DS_ACCOUNT_ID}/envelopes/{envelope_id}/documents/combined"
        response = requests.get(doc_download_url, headers=headers)
        response.raise_for_status()
        doc_content_bytes = response.content

        # Upload to Salesforce
        encoded_content = base64.b64encode(doc_content_bytes).decode('utf-8')
        attachment_payload = {
            'ParentId': salesforce_contract_id,
            'Name': f'Signed Contract - {contract_number}.pdf',
            'Body': encoded_content,
            'ContentType': 'application/pdf'
        }
        sf_client.Attachment.create(attachment_payload)
        logging.info(f"Successfully attached document to Salesforce Contract {salesforce_contract_id}.")
        return True

    except Exception as e:
        logging.error(f"An error occurred while processing the job: {e}")
        return False # Signal that the job failed

# --- RabbitMQ Listener Loop ---
def main():
    """Main worker loop to listen for jobs from the queue."""
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    
    logging.info('[*] Waiting for messages. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        logging.info(" [x] Received new job")
        webhook_data = json.loads(body)
        
        success = process_webhook_job(webhook_data)
        
        if success:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.info(" [x] Job acknowledged and completed.")
        else:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False) 
            logging.warning(" [!] Job failed and was not acknowledged.")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info('Interrupted by user.')
    except pika.exceptions.AMQPConnectionError as e:
        logging.critical(f"Cannot connect to RabbitMQ. Is the URL correct? Error: {e}")
        time.sleep(10) # Wait before exiting to prevent rapid restarts

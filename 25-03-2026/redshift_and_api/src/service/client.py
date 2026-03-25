import boto3

class Client:
    redshift_data_client = None

    @staticmethod
    def get_redshift_client():
        if Client.redshift_data_client:
            return Client.redshift_data_client
        
        Client.redshift_data_client = boto3.client('redshift-data')
        return Client.redshift_data_client
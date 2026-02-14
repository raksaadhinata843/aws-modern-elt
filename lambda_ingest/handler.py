import os
import json
import boto3
import requests
from datetime import datetime

s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get('aws-bucket-gweh')
API_KEY = os.environ.get('COMMODITIES_API_KEY')
BASE_URL = "https://commodities-api.com/api/latest"

def lambda_handler(event, context):
    params = {
        'access_key': API_KEY,
        'base': 'USD',
        'symbols': 'XAU,XAG,WHEAT,CORN,COFFEE' # Contoh komoditas
    }
    
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Validasi sederhana
        if not data.get('success'):
            raise ValueError(f"API Error: {data.get('error')}")
        
        # Metadata tambahan
        data['ingested_at'] = datetime.utcnow().isoformat()
        
        # S3 Path: raw/year=YYYY/month=MM/day=DD/data_uuid.json
        now = datetime.now()
        file_path = f"raw/year={now.year}/month={now.strftime('%m')}/day={now.strftime('%d')}/data_{now.strftime('%H%M%S')}.json"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_path,
            Body=json.dumps(data)
        )
        
        return {"status": "success", "path": file_path}
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"status": "error", "message": str(e)}

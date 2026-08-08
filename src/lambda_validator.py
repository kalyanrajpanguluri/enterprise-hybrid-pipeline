import json
import logging
import urllib.parse
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    AWS Lambda Validator Function:
    Inspects S3 Object Created events, validates file extensions, logs object metadata,
    and returns file metrics for downstream orchestration.
    """
    logger.info("Received Event: " + json.dumps(event))
    
    try:
        # Extract bucket and key from S3 Event
        records = event.get('Records', [])
        if not records:
            # Triggered directly via EventBridge detail
            detail = event.get('detail', {})
            bucket_name = detail.get('bucket', {}).get('name')
            object_key = detail.get('object', {}).get('key')
        else:
            bucket_name = records[0]['s3']['bucket']['name']
            object_key = urllib.parse.unquote_plus(records[0]['s3']['object']['key'], encoding='utf-8')

        logger.info(f"[VALIDATION] Validating input file: s3://{bucket_name}/{object_key}")

        # Basic extension check
        allowed_extensions = ('.csv', '.json', '.parquet')
        if not object_key.lower().endswith(allowed_extensions) and not object_key.endswith('/'):
            logger.warning(f"[VALIDATION WARNING] File '{object_key}' does not match allowed data extensions {allowed_extensions}.")
            return {
                'statusCode': 400,
                'body': json.dumps({'status': 'REJECTED', 'reason': 'Unsupported file extension'})
            }

        logger.info(f"[VALIDATION SUCCESS] File 's3://{bucket_name}/{object_key}' passed initial validation checks.")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'VALIDATED',
                'bucket': bucket_name,
                'key': object_key
            })
        }

    except Exception as e:
        logger.error(f"[VALIDATION ERROR] Error processing event: {str(e)}", exc_info=True)
        raise e

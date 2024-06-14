import yaml
import boto3
from utils.decorators import function_logger

class CONFIG:
	__config_location = './config.yaml'
	with open(__config_location) as f:
		__cfg = yaml.load(f, Loader=yaml.FullLoader)

	for k, v in __cfg.items():
		locals()[k] = v

@function_logger
def upload_to_s3(data, bucket, file_name):
	s3 = boto3.resource("s3")
	s3object = s3.Object(bucket, file_name)
	s3object.put(Body=data)
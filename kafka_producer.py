from time import sleep
import json
from confluent_kafka import Producer
import yaml
from utils.utils import CONFIG
import socket
import requests
from utils.decorators import function_logger

@function_logger
def get_users(url):
	response = requests.get(url)
	data = response.json()
	results = data["results"][0]

	return results

@function_logger
def start_streaming():
	conf = {
		'bootstrap.servers': CONFIG.kafka['socket'],
		'client.id': socket.gethostname()
	}

	producer = Producer(conf)
	url = CONFIG.users_api['url']

	n = 10
	while n:
		user_data = get_users(url)
		user_data = clean_data(user_data)
		print(f'streaming users data: {user_data}')
		producer.produce(CONFIG.kafka['topic'], key="key", value=json.dumps(user_data).encode('utf-8'))
		sleep(5)
		n -= 1

	producer.flush()

@function_logger
def clean_data(data):
	final_data = {}
	final_data['full_name'] = f"{data['name']['first']} {data['name']['last']}"
	final_data['country'] = data['location']['country']
	final_data['email'] = data['email']
	final_data['gender'] = data['gender']
	final_data['city'] = data['location']['city']

	return final_data


def main():
	start_streaming()
	


if __name__ == '__main__':
	main()
import logging
from functools import wraps

def function_logger(func):
	logging.basicConfig(
		level = logging.INFO,
		filename="main.log",
		format='%(asctime)s:%(funcName)s:%(decorated_function)s:%(levelname)s:%(message)s'
	)

	old_factory = logging.getLogRecordFactory()

	def record_factory(*args, **kwargs):
	    record = old_factory(*args, **kwargs)
	    record.decorated_function = func.__name__
	    return record

	logging.setLogRecordFactory(record_factory)

	logger = logging.getLogger(__name__)

	@wraps(func)
	def decorated_func(*args, **kwargs):
		log_str = f'calling {func.__name__}'
		
		if args:
			log_str += f' with arguments {args}'
		if kwargs:
			log_str += f', {kwargs}'
		
		result = func(*args, **kwargs)
		
		logger.info(log_str + f"\nreturn value: {result}")
		
		return result

	return decorated_func
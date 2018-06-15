import logging

class Config(object):
	
	#Logging config
	LOG_PATH = '.'
	LOG_NAME = 'hts_status.log'
	LOG_FORMAT = '%(asctime)s %(levelname)s %(message)s'
	
	#memcache config
	MEMCACHE_ADDRESS = '127.0.0.1'
	MEMCACHE_PORT = 11211
	
	#S3 Buckets
	S3_BUCKETS = ["s3://kvh-hts-terminal-status/terminal"] # ["s3://kvh-hts-terminal-status/terminal","s3://kvh-hts-terminal-status/sspc"]
	
	# Influx config
	INFLUX_URL = 'http://influx.ops.kvh.com:8086/write?'
	INFLUX_PRECISION = 's'
	

class ProductionConfig(Config):
	# Logging config
	LOG_LEVEL = logging.WARNING

	# Influx config
	UPLOAD_DATA = True
	INFLUX_DB = 'HTS_DEV' # Looks wrong but its right

class TestConfig(Config):
	# Logging config
	LOG_LEVEL = logging.DEBUG

	# Influx config
	UPLOAD_DATA = False
	INFLUX_DB = 'HTS'

class DevelopmentConfig(Config):
	# Logging config
	LOG_LEVEL = logging.DEBUG

	# Influx config
	UPLOAD_DATA = True
	INFLUX_DB = 'HTS'
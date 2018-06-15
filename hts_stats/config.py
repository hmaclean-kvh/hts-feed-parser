import logging

class Config(object):
    
    #Debug settingss
    FLASK_DEBUG = False
    DEBUG = False
    
    #Secret key
    SECRET_KEY = '\xe75:\x0f\xbd\x1e\xad\x91-\xdd\x04\x89\xcd\xebI\xc5\xd8\xfb\xe7J\xbc\x8e\xca3'
    
    #Flask settings
    FLASK_SERVER_NAME = 'localhost:8000'
    
    #Enable BASIC AUTH
    BASIC_AUTH_FORCE = True
    
    #Logging config
    LOGGING_FILENAME = '/home/deploy/logs/ossapi.log'
    LOGGING_MAX_BYTES = 25000000
    

    
class ProductionConfig(Config):
    

    
class DevelopmentConfig(Config):
    

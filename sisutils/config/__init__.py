from workers.mongo import Mongo

class MissingAppConfiguration(Exception):
  pass
  
class ConfigurationError(Exception):
  pass
  
class AdminConfigurationError(Exception):
  pass



class Config(object):

  def __init__(self,appKey):
    self.appKey = appKey
    self.config = {}  
    self.mongo = Mongo()
    
  def refresh(self):
    self.config = {}
    self.loadAll()
    
  def findConfiguration(self):
    appConfig = self.mongo.findAppConfig({'key':self.appKey})
    if appConfig is None:
      raise MissingAppConfiguration("There is no application configuration for " + self.appKey)
    
    self.loadAdminConfiguration(appConfig)
    return appConfig
  

  def loadAdminConfiguration(self, appConfig):
    try:
      adminConfiguration = appConfig['admin']
      if len(adminConfiguration) == 0:
        raise AdminConfigurationError("There is no admin configuration for this app! Make one!!")
      self.config['admin'] = adminConfiguration
    except KeyError,e:
      raise AdminConfigurationError("There is no admin configuration for this app! Make one!!")

  def getAggregatorConfigs(self):
    return self.config['aggregators']
    
  def getAggregatorConfig(self, id):
    for aggregator in self.config['aggregators']:
      if id == aggregator['_id']:
        return aggregator
    return None
    
  def getAggregatorConfigForType(self, type):
    for aggregator in self.config['aggregators']:
      if type == aggregator['type']:
        return aggregator
    return None
    
  def getParserConfigForType(self, type):
    for parser in self.config['parsers']:
      if type == parser['type']:
        return parser
    return None
      
          
  def getParserConfigs(self):
    return self.config['parsers']
    
  def getApplicationConfig(self):
    return self.config['application']
  
  '''
    Get all configurations for alerts
  '''
  def getAlertConfigs(self):
    return self.config['alerts']
    
  def getAnalysisConfigs(self):
    return self.config['analysis']
  
  
  def getAdminConfig(self):
    return self.config['admin']
  
  '''
    Get a certain configuration for an alert type
  '''
  def getAlertConfig(self, type):
    config = None
    try:
      config = self.config['alerts'][type]
    except KeyError, e:
      pass
    return config
    
  def getAnalysisConfig(self, type):
    config = None
    try:
      config = self.config['analysis'][type]
    except KeyError, e:
      pass
    return config
    
  '''
    Get the logging configuration
  '''
  def getLoggingConfig(self):
    config = None
    try:
      config = self.config['logging']
    except KeyError,e:
      pass
    return config
    
    
    
    
    
    

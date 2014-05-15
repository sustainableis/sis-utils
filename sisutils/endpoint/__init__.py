class ConfigurationError(Exception):
  pass

class Endpoint(object):
  
  def __init__(self, appConfig):
    try:
      self.insertEndpoint = appConfig['insertEndpoint']
    except KeyError, e:
      raise ConfigurationError('Missing configuration key \'insertEndpoint\'')

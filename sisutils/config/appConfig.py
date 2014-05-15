from sisutils.config import Config

class AppConfig(Config):
  
  def __init__(self,appKey):
    super(AppConfig, self).__init__(appKey)
    
  def loadAll(self):
    appConfig = self.findConfiguration()
    
    parserConfigKeys = appConfig['parsers']
    aggregatorConfigKeys = appConfig['aggregators']
    alertConfigs = appConfig['alerts']
    analysisConfigs = appConfig['analysis']
    loggingConfig = appConfig['logging']
    
    parserConfigurations = []
    for key in parserConfigKeys:
      parserConfigurations.append(self.mongo.findParserConfig({'_id':key}))
    aggregatorConfigurations = []
    for key in aggregatorConfigKeys:
      aggregatorConfigurations.append(self.mongo.findAggregateConfig({'_id':key}))
    alertConfigurations = {}
    for alert in alertConfigs:
      alertConfigurations[alert["type"]] = alert
    analysisConfigurations = {}
    for analysis in analysisConfigs:
      analysisConfigurations[analysis["type"]] = analysis
    
    
    self.config['application'] = appConfig
    self.config['parsers'] = parserConfigurations
    self.config['aggregators'] = aggregatorConfigurations
    self.config['alerts'] = alertConfigurations
    self.config['analysis'] = analysisConfigurations
    self.config['logging'] = loggingConfig
  


if __name__=='__main__':

  config = Config('developmentParser')
  config.loadAll()
  print config.getParserConfigs()

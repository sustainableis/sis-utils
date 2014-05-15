from config import Config
from config import MissingAppConfiguration
import sys

class CustomConfig(Config):

  def __init__(self,appKey):
    super(CustomConfig, self).__init__(appKey)
    

  def loadAll(self):
    appConfig = self.findConfiguration()

    parserConfigs = appConfig['parsers']
    alertConfigs = appConfig['alerts']
    analysisConfigs = appConfig['analysis']

    parserConfigurations = []
    aggregatorConfigurations = []
    alertConfigurations = {}
    analysisConfigurations = {}

    if not parserConfigs:
      print '!! No parsers in configuration'
    else:
      print '-- Loading %s parsers'%(len(parserConfigs))
      for parser in parserConfigs:
        parserConfigurations.append(parser)
        if 'aggregator' in parser:
          print '---- Loading %s for parser'%(parser['aggregator']['title'])
          aggregatorConfigurations.append(parser['aggregator'])
      
      
      
    if not alertConfigs:
      print '!! No alerts in configuration'  
    else:
      print '-- Loading %s alert modules'%(len(alertConfigs))
      for alert in alertConfigs:
        print '---- Loading %s module'%(alert['title'])
        alertConfigurations[alert['type']] = alert
    
    
    if not analysisConfigs:
      print '!! No analysis modules in configuration'
    else:
      print '-- Loading %s analysis modules'%(len(analysisConfigs))
      for analysis in analysisConfigs:
        print '---- Loading %s module'%(analysis['title'])
        analysisConfigurations[alert['type']] = analysis
    
    self.config['application'] = appConfig
    self.config['parsers'] = parserConfigurations
    self.config['aggregators'] = aggregatorConfigurations
    self.config['alerts'] = alertConfigurations
    self.config['analysis'] = analysisConfigurations





if __name__=='__main__':

  config = CustomConfig('reflowApp')
  config.loadAll()
  #print config.getParserConfigs()
  #print config.getAggregatorConfigs()
  #print config.getAnalysisConfigs()
  #print config.getAlertConfigs()
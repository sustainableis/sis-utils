from mq.topic import TopicConsumer

class TestExchange(object):
    
    def __init__(self, exchangeName, config):
      print exchangeName
      print appConfig['rabbit']
      self.topicConsumer = TopicConsumer(config = config,
                                          exchangeName = exchangeName,
                                          bindingKey = 'admin.developmentParser.parser.acquilite',
                                          consumeFunc = self.consumeMessages,
                                          noArgs = True)
      self.topicConsumer.startConsuming()
                                       
                                       
    def consumeMessages(self, ch, method, properties, body):
      print method.routing_key
      print body




if __name__=='__main__':
  exchangeName = 'sis-admin-exchange'
  from config.appConfig import AppConfig
  config = AppConfig('developmentParser')
  config.loadAll()
  appControl = TestExchange(exchangeName, config)
  
  

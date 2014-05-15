from mq import Rabbit
import rabbitpy

class TopicConsumer(Rabbit):

  def __init__(self,config,moduleType,exchangeName,bindingKeys,consumeFunc, do_dlx=True,noArgs=False):
    appConfig = config.getApplicationConfig()
    super(TopicConsumer,self).__init__(appConfig['rabbit'],moduleType)
    
    # convert dictionary unicode strings to utf-8 strings
    if do_dlx:
      topicArgs =  {str(key):str(appConfig['rabbit']['topicArguments'][key]) for key in appConfig['rabbit']['topicArguments']}
    else:
      topicArgs = {}
    if noArgs:
      topicArgs = {}
    self.exchangeName = exchangeName
    self.arguments = topicArgs
    self.bindingKeys = bindingKeys
    self.consumeFunc = consumeFunc
                      
  
  def initConsumer(self):
    self.initChannel()
    self.exchange = rabbitpy.TopicExchange(channel=self.channel,
                                      name = self.exchangeName,
                                      durable = True,
                                      auto_delete = False,
                                      arguments = self.arguments)
    self.exchange.declare()
    
    self.queue = rabbitpy.Queue(self.channel,
                           exclusive=True)
    self.queue.declare()
    # bind the queue to exchange with the appropriate binding keys
    for key in self.bindingKeys:
      self.queue.bind(self.exchange,key)
  
  
  '''
    Call this function to start infinitely consuming from
    the topic exchange. This method runs infinitely so it doesn't
    return unless there's an exception so be sure to put this in 
    a thread if you don't want to block execution
  '''
  def startConsuming(self):
    self.initConsumer()
    try:
      for message in self.queue.consume_messages():
        if message is not None:
          self.consumeFunc(message)
    except KeyboardInterrupt,e:
      self.stopConsuming()
    
  def stopConsuming(self):
    self.destroyChannel()
    
    
    
class TopicPublisher(Rabbit):

  def __init__(self,config, moduleType,exchangeName, do_dlx=True, confirm=False):
    appConfig = config.getApplicationConfig()
    super(TopicPublisher,self).__init__(appConfig['rabbit'],moduleType)
    self.exchangeName = exchangeName
    self.confirm = confirm
    # no topic arguments
    if do_dlx:
      topicArgs =  {str(key):str(appConfig['rabbit']['topicArguments'][key]) for key in appConfig['rabbit']['topicArguments']}
    else:
      topicArgs = {}
    self.arguments = topicArgs
  
  def emitMessage(self, routingKey, message):
    # Lazy loading the channel - first message sent will be slow
    if self.channel is None:
      self.initPublisher()
    rMessage = rabbitpy.Message(self.channel, message)
    rMessage.publish(self.exchange, routingKey)
    
                               
  def initPublisher(self):
    self.initChannel()
    self.exchange = rabbitpy.TopicExchange(channel=self.channel,
                                      name = self.exchangeName,
                                      durable = True,
                                      auto_delete = False,
                                      arguments = self.arguments)
    

  
  
  def stopPublishing(self):

    if self.channel is not None:
      self.destroyChannel()
    
    
    
    
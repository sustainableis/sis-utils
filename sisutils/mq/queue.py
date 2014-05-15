from mq import Rabbit
import rabbitpy

class QueueConsumer(Rabbit):

  def __init__(self,config,moduleType, queueName,consumeFunc):
    appConfig = config.getApplicationConfig()
    super(QueueConsumer,self).__init__(appConfig['rabbit'], moduleType)
    self.queueName = queueName
    self.consumeFunc = consumeFunc
    
  def initConsumer(self):
    self.initChannel()
    self.queue = rabbitpy.Queue(self.channel,self.queueName)
    self.queue.declare()
    
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
    
    
class QueuePublisher(Rabbit):

  def __init__(self,config,moduleType,queueName):
    appConfig = config.getApplicationConfig()
    super(QueuePublisher,self).__init__(appConfig['rabbit'], moduleType)
    self.queueName = queueName

  def initPublisher(self):
    self.initChannel()
    self.queue = rabbitpy.Queue(self.channel,self.queueName)
    self.queue.declare()
  
  '''
    Emit a message to a basic RabbitMQ Queue
  '''
  def emitMessage(self, message):
    # Lazy loading the channel - first message sent will be slow
    if self.channel is None:
      self.initPublisher()
    rMessage = rabbitpy.Message(self.channel, message)
    rMessage.publish('',routing_key=self.queueName)
    
  def stopPublishing(self):
    if self.channel is not None:
      self.destroyChannel()
    
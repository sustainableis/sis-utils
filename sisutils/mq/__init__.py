import rabbitpy

my_globals = {'connection_count':0}

connections = {}

def getConnection(url,moduleType):
  print '@@@@ Getting a connection'
  print moduleType
  try:
    connection = connections[moduleType]['connection']
    connections[moduleType]['count'] += 1
    return connection
  except KeyError,e:
    print '!!!! Instantiating a new connection'
    print url
    connections[moduleType] = {}
    connections[moduleType]['connection'] = rabbitpy.Connection(url)
    connections[moduleType]['count'] = 1
    my_globals['connection_count'] += 1
    return connections[moduleType]['connection']



class Rabbit(object):
  
  def __init__(self, config, moduleType):
    
    host = str(config['host'])
    port = str(config['port'])
    user = str(config['user'])
    passwd = str(config['pass'])
    vHost = str(config['vHost'])
    self.url = 'amqp://'+user+':'+passwd+'@'+host+':'+port+vHost
    self.moduleType = moduleType
    self.channel = None
    
  def initChannel(self):
    self.connection = getConnection(self.url, self.moduleType)
    print '!!! --- Instantiating channel'
    print self.connection
    self.channel = self.connection.channel()
  
  def destroyChannel(self):
    print '******* DESTROY'
    self.channel.close()
    self.decrementConnectionCount()
  
  
  def decrementConnectionCount(self):
    connections[self.moduleType]['count'] -= 1
    print 'Number of remaining connections: %s'%(connections[self.moduleType]['count'])
    if connections[self.moduleType]['count'] == 0:
      print '#$#$#$#$#$# CLOSING CONNECTION'
      connections[self.moduleType]['connection'].close()
      del connections[self.moduleType]
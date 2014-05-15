from pymongo import MongoClient
import os

class Mongo(object):

  def __init__(self):
    uri = os.environ['MONGOHQ_URL']
    dbName = os.environ['MONGOHQ_DB_NAME']
    self.client = MongoClient(uri)
    self.db = self.client[dbName]
    
  '''
    If you want to do things yourself
  '''
  def getDB(self):
    return self.db

  def findAppConfig(self, where):
    return self.db.appConfig.find_one(where)

  def findParserConfig(self, where):
    return self.db.parserConfig.find_one(where)
  
  def findAggregateConfig(self, where):
    return self.db.aggregateConfig.find_one(where)
    
  def findFeedConfig(self, where):
    return self.db.feedConfig.find_one(where)
  
  def fileIsParsed(self, filename):
    if self.db.messages.find_one({'file_name':filename}) is None:
      return False
    else:
      return True
      
  def getAlreadyParsed(self):
    return self.db.messages.distinct("file_name")
  
  


#self.client = MongoClient('mongodb://node-worker:node34worker@troup.mongohq.com:10002')
if __name__=='__main__':

  mongo = Mongo()
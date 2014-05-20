from sisutils.endpoint import Endpoint
import psycopg2
from datetime import datetime
from psycopg2.extras import DictCursor
import traceback


class NoConnectionException(Exception):
  pass

class SelectorError(Exception):
  pass


class PSQL(Endpoint):
  
  def __init__(self, appConfig, endpointConfig):
    super(PSQL, self).__init__(appConfig)
    self.database = endpointConfig['database']
    self.user = endpointConfig['user']
    self.password = endpointConfig['pass']
    self.host = endpointConfig['host']
    self.conn = None
    
    
  def connect(self):
    print self.host
    try:
      self.conn = psycopg2.connect(database=self.database,
                                 user=self.user,
                                 password=self.password,
                                 host=self.host)
                
      print "Connected to PSQL Endpoint"              
    except Exception, e:
      print e
  
  def close(self):
    if self.conn:
      self.conn.close()
  
  '''
    Execute a select on a table and return all the rows
    Parameters:
      tableName:  name of the table (String)
      fields:     list of fields you want back (List)
      conditions: list of conditions to satisfy (List of strings)
      wheres:     list of where statements (List of strings)
  '''
  def select(self, tableName, fields=None, wheres=None, dictResults=False):
    if self.conn:
      try:
        # parse the select string here
        if fields is None:
          fieldString = '*'
        else:
          fieldString = ','.join(fields)
        if wheres is None:
          whereString = ''
        else:
          whereString = ' WHERE ' + ' and '.join(wheres)
        
        query = 'SELECT %s FROM %s %s'%(fieldString, tableName, whereString)
        print query
        if dictResults:
          cursor = self.conn.cursor(cursor_factory=DictCursor)
        else:
          cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
      except Exception, e:
        print e
      finally:
        try:
          if cursor:
            cursor.close()
        except NameError:
          pass
      return []
    else:
      raise NoConnectionException('You need to connect to the database!')



  '''
    Execute a complicated query that selects some rows
    Trusting that there is nobody using this code that will
    try to do something stupid...might need to secure this
    later at some point
  '''
  def complicatedSelectExecution(self, query, dictResults=False):
    if self.conn:
      try:
        if dictResults:
          cursor = self.conn.cursor(cursor_factory=DictCursor)
        else:
          cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
      except Exception, e:
        print e
        
      finally:
        try:
          if cursor:
            cursor.close()
        except NameError:
          pass
    else:
      raise NoConnectionException('You need to connect to the database!')
  
  
  def complicatedExecution(self, query):
    if self.conn:
      try:
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
      except Exception, e:
        print e
        
      finally:
        try:
          if cursor:
            cursor.close()
        except NameError:
          pass
    else:
      raise NoConnectionException('You need to connect to the database!')
  

  '''
    Insert many records into a table in the database
    Parameters:
      table: tableName (String)
      dataList: (List of Dicts) key -> column name, value -> column value
  '''
  def insertMany(self, tableName, dataList):
    success = False
    if not self.insertEndpoint:
      print 'Inserting is not allowed under the current configuration'
      return None
    if not isinstance(dataList, list):
      raise TypeError('insertMany arg[2] take type or subtype of a list!')
    if self.conn:
      try:
        columnKeys = []
        columnData = []
        if len(dataList) > 0:
          columnKeys = dataList[0].keys()
        else:
          return None
        for item in dataList:
          itemData = []
          for key in columnKeys:
            if isinstance(item[key], datetime):
              item[key] = "'"+str(item[key])+"'"
            else:
              item[key] = str(item[key])
            itemData.append(item[key])
          columnData.append(itemData)
        # have all the data in lists now
        keyString = ','.join(columnKeys)
        valueStrings = []
        for data in columnData:
          valueStrings.append('(' + ','.join(data) + ')')
        valueStrings = ','.join(valueStrings)
        
        query = 'INSERT INTO %s (%s) VALUES %s'%(tableName, keyString, valueStrings)
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        success = True
      except Exception, e:
        print e
      finally:
        try:
          if cursor:
            cursor.close()
        except NameError:
          pass
      return success
    else:
      raise NoConnectionException('You need to connect to the database!')
  
  '''
    Insert a record into a table in the database
    Parameters:
      table:  tableName (String)
      dataDict: key -> column name, value -> column value
  '''
  def insertOne(self, table, dataDict):
    inserted_id = None
    if not self.insertEndpoint:
      print 'Inserting is not allowed under the current configuration'
      return None
    if self.conn:
      try:
        dataKeys = dataDict.keys()
        dataVals = []
        for key in dataKeys:
          dataVals.append(dataDict[key])
        query = "INSERT INTO %s (%s) VALUES (%s) RETURNING id"%(table, ','.join(dataKeys), ','.join(dataVals))
        cursor = self.conn.cursor()
        cursor.execute(query)
        inserted_id = cursor.fetchone()[0]
        self.conn.commit()
      except Exception, e:
        print e
      finally:
        try:
          if cursor:
            cursor.close()
        except NameError:
          pass
      return inserted_id
    else:
      raise NoConnectionException('You need to connect to the database!')
  
  '''
    Update a record in a table in the database
    Parameters:
      table:  tableName (String)
      selector: the 'where' part of the query
      dataDict: (List of Dicts) key -> column name, value -> column value
  '''
  def updateMany(self, table, selector, dataList):
    success = False
    if not self.insertEndpoint:
      print 'Inserting is not allowed under the current configuration'
      return False
    if not isinstance(dataList, list):
      raise TypeError('insertMany arg[2] take type or subtype of a list!')
    if self.conn:
      try:
        if len(dataList) > 0:
          columnKeys = dataList[0].keys()
        else:
          return None
        executionList = []
        for row in dataList:
          setList = []
          valueList = []
          where = None
          for k,v in row.items():
            if k == selector:
              where = 'WHERE %s=%s'%(k,v)
              continue
            if isinstance(v, str):
              setList.append(k + '= %s')
            elif isinstance(v, int):
              setList.append(k + '= %s')
            elif isinstance(v,unicode):
              v = v.encode('ascii','ignore')
              setList.append(k + '= %s')
            else:
              raise ValueError("Values for update must be of type String or Integer")
            valueList.append(v)
          if where is None:
            raise SelectorError("There was no selector found!")
          executionList.append({'query': 'UPDATE '+ table + ' SET ' + ','.join(setList) + ' ' + where, 'val_tuple': tuple(valueList) })
        
        for execution in executionList:
          cursor = self.conn.cursor()
          cursor.execute(execution['query'],execution['val_tuple'])
        self.conn.commit()
        success = True
          
      except Exception, e:
        print traceback.print_exc()
        print e
        raise
      finally:
        try:
          if cursor:
            cursor.close()
        except NameError:
          pass
      return success
    else:
      raise NoConnectionException('You need to connect to the database!')
  
  
  
  
  
  
  
  
  
  
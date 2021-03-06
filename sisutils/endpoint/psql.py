from sisutils.endpoint import Endpoint
import psycopg2
from datetime import datetime
from datetime import date
from psycopg2.extras import DictCursor
import traceback

class NoConnectionException(Exception):
  pass

class SelectorError(Exception):
  pass


class PSQL(Endpoint):
  
  def __init__(self, appConfig, endpointConfig, debug=False):
    super(PSQL, self).__init__(appConfig)
    self.database = endpointConfig['database']
    self.user = endpointConfig['user']
    self.password = endpointConfig['pass']
    self.host = endpointConfig['host']
    if 'port' in endpointConfig:
        self.port = endpointConfig['port']
    else:
        self.port = None
    self.debug = debug
    self.conn = None
    
    
  def connect(self):
    print self.host
    try:
      self.conn = psycopg2.connect(database=self.database,
                                 user=self.user,
                                 password=self.password,
                                 host=self.host,
                                 port=self.port)
                
      print "Connected to PSQL Endpoint"              
    except Exception, e:
      print e
      raise
 
  def close(self):
    if self.conn:
      self.conn.close()
  
  def getCursor(self):
    if self.conn:
      return self.conn.cursor(cursor_factory=DictCursor)
    else:
      raise NoConnectionException('You need to connect to the database before you get a cursor!')
  
  def closeCursor(self, cursor):
    if cursor is not None:
      cursor.close()
  
  def rollbackCursor(self, cursor):
    self.conn.rollback()
      
  def commitCursor(self, cursor):
    self.conn.commit()
      
  def closeCursor(self, cursor):
    if cursor is not None:
      cursor.close()
      
  def cleanValue(self, value):
    if value is None:
      return 'NULL'
    if isinstance(value, int):
      return str(value)
    elif isinstance(value, datetime):
      return value.strftime("'%Y-%m-%d %H:%M:%S'")
    elif isinstance(value, date):
      return value.strftime("'%Y-%m-%d %H:%M:%S'")
    elif isinstance(value,unicode):
      return "'%s'"%(value.encode('ascii','ignore'))
    elif isinstance(value, str):
      return "'%s'"%(value)
    elif isinstance(value, float):
      return str(value)
    else:
      return value
    
  
  '''
    Execute a select on a table and return all the rows
    Parameters:
      tableName:  name of the table (String)
      fields:     list of fields you want back (List)
      conditions: list of conditions to satisfy (List of strings)
      wheres:     list of where statements (List of strings)
  '''
  def select(self, tableName, fields=None, wheres=None, dictResults=True, cursor=None):
    if self.conn:
      rows = []
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
        if self.debug:
            print query
        
        if cursor is None:
          if dictResults:
            localCursor = self.conn.cursor(cursor_factory=DictCursor)
          else:
            localCursor = self.conn.cursor()
        else:
          localCursor = cursor
        localCursor.execute(query)

        rows = localCursor.fetchall()
                
      except Exception, e:
        print e
        traceback.print_exc()
        if localCursor is not None:
            self.rollbackCursor(localCursor)
      	raise
      finally:
        try:
          if cursor is None:
            localCursor.close()
        except NameError:
          pass
      return rows

    else:
      raise NoConnectionException('You need to connect to the database!')



  '''
    Execute a complicated query that selects some rows
    Trusting that there is nobody using this code that will
    try to do something stupid...might need to secure this
    later at some point
  '''
  def complicatedSelectExecution(self, query, dictResults=True, cursor=None):
    if self.conn:
      rows = []
      try:
        if cursor is None:
          if dictResults:
            localCursor = self.conn.cursor(cursor_factory=DictCursor)
          else:
            localCursor = self.conn.cursor()
        else:
          localCursor = cursor
        localCursor.execute(query)

        rows = localCursor.fetchall()
                
      except Exception, e:
        print e
        traceback.print_exc()
        if localCursor is not None:
            self.rollbackCursor(localCursor)
      	raise
      finally:
        try:
          if cursor is None:
            localCursor.close()
        except NameError:
          pass
      return rows
    else:
      raise NoConnectionException('You need to connect to the database!')
  
  
  def complicatedExecution(self, query, cursor=None, commit=True):
    if self.conn:
      try:
        if cursor is None:
          localCursor = self.conn.cursor()
        else:
          localCursor = cursor
        localCursor.execute(query)
        if commit:
            self.conn.commit()
            
      except Exception, e:
        print e
        traceback.print_exc()
        if localCursor is not None:
            self.rollbackCursor(localCursor)
      	raise
      finally:
        try:
          if cursor is None:
            localCursor.close()
        except NameError:
          pass
    else:
      raise NoConnectionException('You need to connect to the database!')
  
  '''
    Upsert many records into a table in the database
    Parameters:
      table: tableName (String)
      dataList: (List of Dicts) key -> column name, value -> column value
      upsertColumns: (List of Strings) -> conflict columns
      upsertValues: (List of Strings) -> on conflict, set these columns
  '''
  def upsertMany(self, tableName, dataList, upsertColumns, upsertValues):
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
        upsertIndexes = {}
        if len(dataList) > 0:
          columnKeys = dataList[0].keys()
        else:
          return None
        for item in dataList:
          itemData = []
          for idx, key in enumerate(columnKeys):
            itemData.append(self.cleanValue(item[key]))
            if key in upsertColumns:
                upsertIndexes[key] = idx
            if key in upsertValues:
                upsertIndexes[key] = idx
          columnData.append(itemData)
        # have all the data in lists now
        keyString = ','.join(columnKeys)
        valueStrings = []
        for data in columnData:
          valueStrings.append('(' + ','.join(data) + ')')
        valueStrings = ','.join(valueStrings)
        
        upsertSetData = []
        for val in upsertValues:
            ## amount=exlcuded.amount
            upsertSetData.append('{}=excluded.{}'.format(val, val))
        
        query = 'INSERT INTO %s (%s) VALUES %s on conflict (%s) DO UPDATE SET %s'%(tableName, keyString, valueStrings, ','.join(upsertColumns), ','.join(upsertSetData))
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        success = True
      except Exception, e:
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

  '''
    Insert many records into a table in the database
    Parameters:
      table: tableName (String)
      dataList: (List of Dicts) key -> column name, value -> column value
  '''
  def insertMany(self, tableName, dataList, cursor=None, commit=True):
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
            itemData.append(self.cleanValue(item[key]))
          columnData.append(itemData)
        # have all the data in lists now
        keyString = ','.join(columnKeys)
        valueStrings = []
        for data in columnData:
          valueStrings.append('(' + ','.join(data) + ')')
        valueStrings = ','.join(valueStrings)
        
        query = 'INSERT INTO %s (%s) VALUES %s'%(tableName, keyString, valueStrings)
        if cursor is None:
          localCursor = self.conn.cursor()
        else:
          localCursor = cursor
        localCursor.execute(query)
        if commit:
            self.conn.commit()
        success = True
      except Exception, e:
        print e
        traceback.print_exc()
        if localCursor is not None:
            self.rollbackCursor(localCursor)
      	raise
      finally:
        try:
          if cursor is None:
            localCursor.close()
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
  def insertOne(self, table, dataDict, cursor=None, uniqueKeys=[], commit=True,returning="id"):
    inserted_id = None
    if not self.insertEndpoint:
      print 'Inserting is not allowed under the current configuration'
      return None
    if self.conn:
      try:
        localCursor = None
        dataKeys = []
        dataVals = []
        for key,value in dataDict.items():
          dataKeys.append(key)
          dataVals.append(self.cleanValue(value))
        if len(uniqueKeys) > 0:
          uniqueWhere = ""
          for idx,key in enumerate(uniqueKeys):
            uniqueWhere += "=".join([key,self.cleanValue(dataDict[uniqueKeys[idx]])])
            if idx != len(uniqueKeys)-1:
              uniqueWhere += ' and '
          query = "INSERT INTO %s (%s) SELECT %s where not exists(select %s from %s where %s) returning %s"%(table, ','.join(dataKeys), ','.join(dataVals), ','.join(dataKeys), table, uniqueWhere, returning)
        else:
          query = "INSERT INTO %s (%s) VALUES (%s) RETURNING id"%(table, ','.join(dataKeys), ','.join(dataVals))
        
        if self.debug:
            print query
        if cursor is None:
          localCursor = self.conn.cursor()
        else:
          localCursor = cursor
        localCursor.execute(query)
        inserted_id = localCursor.fetchone()
        if inserted_id:
          inserted_id = inserted_id[0]
        if commit:
          self.conn.commit()
      except Exception, e:
        print e
        traceback.print_exc()
        if localCursor is not None:
            self.rollbackCursor(localCursor)
      	raise
      finally:
        try:
          if cursor is None:
            localCursor.close()
        except NameError:
          pass
      return inserted_id
    else:
      raise NoConnectionException('You need to connect to the database!')
  
  '''
    Simple upsert - do an update first (unaffected if the row doesn't exist) and then
    do a unique insert.
    
    # TODO - possible race condition between the update and the insert. Someone
    could insert a high between the update and the insert and then there would
    be a duplicate key error. 
   
  '''
  def upsert(self, table, selectors, dataDict, uniqueKeys=[],returning="id"):
    self.updateMany(table, selectors, [dataDict])
    return self.insertOne(table, dataDict, uniqueKeys=uniqueKeys, returning=returning)
  
  
  '''
    Update a record in a table in the database
    Parameters:
      table:  tableName (String)
      selector: the 'where' part of the query
      dataDict: (List of Dicts) key -> column name, value -> column value
  '''
  def updateMany(self, table, selectors, dataList, cursor=None, commit=True):
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
          wheres = []
          for k,v in row.items():
            if k in selectors:
              wheres.append('%s=%s'%(k,self.cleanValue(v)))
              continue
            valueList.append(self.cleanValue(v))
            setList.append(k + '= %s')
          where = 'WHERE ' + ' and '.join(wheres)
          executionList.append({'query': 'UPDATE '+ table + ' SET ' + ','.join(setList) + ' ' + where, 'val_tuple': tuple(valueList) })
        for execution in executionList:
          if cursor is None:
            localCursor = self.conn.cursor()
          else:
            localCursor = cursor
          if self.debug:
            print execution
          localCursor.execute(execution['query'],execution['val_tuple'])
        if commit:
          self.conn.commit()
        success = True
          
      except Exception, e:
        print traceback.print_exc()
        print e
        if localCursor is not None:
            self.rollbackCursor(localCursor)
        raise
      finally:
        try:
          if cursor is None:
            localCursor.close()
        except NameError:
          pass
      return success
    else:
      raise NoConnectionException('You need to connect to the database!')

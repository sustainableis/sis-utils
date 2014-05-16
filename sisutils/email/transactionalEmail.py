import mandrill

class TransactionalEmail(object):

  def __init__(self):
    self.apiKey = 'aDpGrxUCdNwZMhR5zjeRCA'
    self.fromEmail = 'dev@sustainableis.com'
    self.fromName = 'SIS Developers'
    self.mClient = mandrill.Mandrill(self.apiKey)
  
  def sendMessage(self,subject,receiverList,message):
    toList = []
    for receiver in receiverList:
      toList.append({'email':receiver['email'],
                     'name': receiver['name'],
                     'type': 'to'})
                     
    message = {'from_email':self.fromEmail,
               'from_name':self.fromName,
               'to': toList,
               'subject':subject,
               'text':message}
               
    result = self.mClient.messages.send(message = message, async=False, ip_pool='Main Pool')
    
    return result
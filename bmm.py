import requests

class Bmm:
    
    def __init__(self, backendURL, generatorID) -> None:

        if backendURL.endswith('/'):
            self.backendURL = backendURL[:-1]
        else:
            self.backendURL = backendURL

        self.generatorID = generatorID

    def getEvents(self):

        response = requests.get(f"{self.backendURL}/api/events/bygenerator/{self.generatorID}")
        response = response.json()
    
        return response
    
    def notifyEvent(self, eventUUID, content):

        notificationData = {
            'uuid': self.generatorID,
            'eventUuid': eventUUID,
            'content': content
        }

        response = requests.post(f"{self.backendURL}/api/events/notify/{eventUUID}", data = notificationData)
    
        return response
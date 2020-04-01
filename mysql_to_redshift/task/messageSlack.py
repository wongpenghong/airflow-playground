import requests,json,os
from datetime import date

PATH_AIRFLOW = os.environ['AIRFLOW_HOME']
PATH = "{path}/dags/test_dags".format(path=PATH_AIRFLOW)

# Get Credentials Config

with open("{path}/config/conf.json".format(path=PATH), "r") as read_file:
    CONFIG = json.load(read_file)['config']

def sendMessage():
    # slack access bot token
    
    data = {
        # Token ID
        'token': CONFIG['tokenSlack'],
        # Channel ID
        'channel': CONFIG['channel'],    # User ID. 
        # show sender as user or tester api
        'as_user': True,
        # text that you want to send
        'text': 'Hey there are some error, you can check in your airflow !!!!'
    }

    # Hit the url with POST method
    requests.post(url='https://slack.com/api/chat.postMessage',data=data)
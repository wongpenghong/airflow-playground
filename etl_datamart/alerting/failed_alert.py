import os
from slack import WebClient

def execute(token,dags_name,channel):
    client = WebClient(token=token)

    client.chat_postMessage(
    channel=channel,
    text="hi! my name is Nami your Data navigator, hey let's check your {} dags, there is some failed process !".format(dags_name)
    )
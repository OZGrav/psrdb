from datetime import datetime


def get_current_time():
    now = datetime.today()
    return now.strftime("%Y-%m-%dT%H:%M:%S")


def get_time(unixtime):
    epoch = datetime.fromtimestamp(unixtime)
    return epoch.strftime("%Y-%m-%dT%H:%M:%S")

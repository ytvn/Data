import schedule
import time
from datetime import datetime
import pandas as pd
import requests
import json
import csv
import tzlocal
import os

os.environ['TZ'] = 'Asia/Ho_Chi_Minh'
time.tzset()
try:
    from types import SimpleNamespace as Namespace
except ImportError:
    from argparse import Namespace


def ConvertToDateTime(timestamp):
    # local_timezone = tzlocal.get_localzone()
    dt_object = datetime.fromtimestamp(timestamp)
    dt = str(dt_object).split(" ")
    d = str(dt[0]).split("-")
    t = str(dt[1]).split(":")
    second = t[2].split("+")
    return int(d[2]), int(d[1]), int(d[0]), int(t[0]) * 3600 + int(t[1]) * 60 + int(second[0])

def Forecast120():
    url = "https://weatherbit-v1-mashape.p.rapidapi.com/forecast/hourly"

    querystring = {"lat": "10.869918", "lon": "106.803808", "hours": "120"}

    headers = {
        'x-rapidapi-key': "d2ac96cc7emsh5ac55bb4f5fa65cp1fe38cjsnc97c14b1021c",
        'x-rapidapi-host': "weatherbit-v1-mashape.p.rapidapi.com"
    }
    columnsForecast = ['UTC', 'Local', 'Radiation', 'AirTemp', 'RelativeHumidity', 'SurfacePressure',
                       'WindDirection10m', 'WindSpeed10m', 'Day', 'Month',
                       'Year', 'Timestamp']
    response = requests.request("GET", url, headers=headers, params=querystring)
    data = response.json()['data']
    weathers = []
    for d in data:
        d = json.dumps(d)
        w = json.loads(d, object_hook=lambda d: Namespace(**d))
        dt_object = ConvertToDateTime(w.ts)
        weather = [w.timestamp_utc, w.timestamp_local, w.solar_rad, w.temp, w.rh, w.pres, w.wind_dir, w.wind_spd]
        for i in dt_object:
            weather.append(i)
        weathers.append(weather)

    df = pd.DataFrame(weathers, columns=columnsForecast)
    df.to_csv("Forecast1.csv")
    return df

    print(response.text)



def MergeForecast():
    print("Calling API\n")
    Forecast120()
    forecast1 = pd.read_csv('Forecast.csv',index_col=0)
    forecast2 = pd.read_csv('Forecast1.csv',index_col=0)
    forecast = pd.concat([forecast1,forecast2])
    forecast = forecast.drop_duplicates(subset='UTC', keep="last").reset_index(drop=True)
    print("Merge datasets!!!\n")
    forecast.to_csv('Forecast.csv')
    print(forecast)


schedule.every().days.at("23:00").do(MergeForecast)

while True:
    schedule.run_pending()
    time.sleep(1)

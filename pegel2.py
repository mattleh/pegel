#!/usr/bin/env python3
# %% import
import logging
import requests
import datetime
from dateutil import parser
import json
import time
import paho.mqtt.client as MQTT
import os
from sys import stdout
import atexit
import gc
import multiprocessing as mp

# %%
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
logFormatter = logging.Formatter\
("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(stdout) #set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

def get_urldata(url):
    stationdata = []
    stationresp = requests.get(url)
    stationdata.append(json.loads(stationresp.content))
    data = {}
    for set in stationdata:
      nr = set[0].get("station_no")
      data[nr] = { 'unique_id' : nr }
      for entry in set:
          id = entry.get("station_no")
          shortname = entry.get("ts_shortname").split(".")[1]
          if len(entry.get("data")) > 0:
            data[nr][shortname] = entry.get("data")[-1][1]
          else:
            data[nr][shortname] = ''
    return data

def merge_nested_dicts(d1, d2):
    for key, value in d2.items():
        if key in d1 and isinstance(d1[key], dict) and isinstance(value, dict):
            merge_nested_dicts(d1[key], value)
        else:
            d1[key] = value


# %% get pegel data
def get_pegel():
  # daten abholen
    response = requests.get('http://data.ooe.gv.at/files/hydro/HDOOE_Export_OG.zrxp')

    data = {}
    nr = None
    # über alle Zeilen iterieren. Die Response ist so aufgebaut, dass immer zuerst
    # der Header kommt und anschließend die zugehörigen Datenzeilen
    for line in response.iter_lines():
      # Headerzeilen parsen

      if line.decode('latin1').startswith('#') and '|' in line.decode('latin1'):
        # erstes Zeichen wegschnippseln und nach | trennen und iterieren
        line = line[1:]
        elements = line.split(b'|')
        for element in elements:
          # manche Datenelemente sind UTF-8 kodiert, andere LATIN1, und andere einfach nur Müll ;-(
          try:
            element = element.decode('utf-8', 'strict')
          except UnicodeDecodeError:
            element = element.decode('latin1', 'strict')
          # ID parsen
          if element.startswith('SANR'):
            nr = element.encode('utf-8')[4:].decode('utf-8')
            data[nr] = { 'unique_id' : nr }
          # Gewässer parsen
          if element.startswith('SWATER'):
            data[nr]['water'] = element.encode('utf-8')[6:].decode('utf-8')
          # Stationsname parsen
          if element.startswith('SNAME'):
            data[nr]['location'] = element.encode('utf-8')[5:].decode('utf-8')
          # Unit parsen
          if element.startswith('CUNIT'):
            data[nr]['unit'] = element.encode('utf-8')[5:].decode('utf-8')
          # Timezone parsen
          if element.startswith('TZ'):
            data[nr]['tz'] = element.encode('utf-8')[2:].decode('utf-8')
      else:
        new_entry = True
        if nr == None or len(line) == 0:
          continue
        line = line.decode('latin1')
        elements = line.split(' ')
        # erstes Element: Timecode parsen und in Unix-Timestamp verwandeln
        #timestamp = int(datetime.datetime.strptime(elements[0], "%Y%m%d%H%M%S").timestamp())
        timestamp = parser.parse(f'{elements[0]} {data[nr].get("tz")[3:]}')
        # zweites Element: Höhe in cm, allerdings gibt es ungültige Messwerte mit negativen Werten
        value = int(elements[1])
        # Timestamp und Value ersetzen, wenn es einen aktuelleren, gültigen Wert in der aktuellen Zeile gibt
        if ('timestamp' not in data[nr] or timestamp > data[nr]['timestamp']) and value >= 0:
          data[nr]['timestamp'] = timestamp
          data[nr]['value'] = value

    # Korrektur für kaputte Daten die doppelt transkodiert wurden -> Müll
    data['9450']['location'] = 'Unterweißenbach'
    data['5230']['location'] = 'Weißenbach am Attersee'
    data['8445']['location'] = 'Roßleithen'

      # for url in urls:
      #   x= get_urldata(url)
      #   stationdata = []
      #   stationresp = urlopen(url)
      #   stationdata.append(json.loads(stationresp.read()))

      #   for set in stationdata:
      #     for entry in set:
      #         id = entry.get("station_no")
      #         shortname = entry.get("ts_shortname").split(".")[1]
      #         if len(entry.get("data")) > 0:
      #           data[nr][shortname] = entry.get("data")[-1][1]
      #         else:
      #           data[nr][shortname] = ''  
    return data

data = get_pegel()    
if __name__ == '__main__':
  print('connected')
  data2lastsync = datetime.datetime.now()
  data2 = {}
  urls = []
  for item in data.items():
    nr = item[0]
    # get and add additional data from web
    urls.append(f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{nr}/S/alm.json")
    urls.append(f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{nr}/S/events.json")
    urls.append(f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{nr}/S/ltv.json")

  while True:
    start = time.time()
    data = get_pegel()   

    if not bool(data2) or (data2lastsync - datetime.datetime.now()).days >= 1: 
      print("Update Additional Data")
      with mp.Pool() as pool:
        for result in pool.map(get_urldata, urls):
          merge_nested_dicts(data2, result)

    end = time.time()
    merge_nested_dicts(data, data2)
    print(data)
    print(f"Took {end - start:.2f}s collecting data")
    time.sleep(10)
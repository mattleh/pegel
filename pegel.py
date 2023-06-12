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
from urllib.request import urlopen
import gc

# %%
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
logFormatter = logging.Formatter\
("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(stdout) #set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

mqtt_server = os.environ.get('MQTT_SERVER')
mqtt_port = int(os.environ.get('MQTT_PORT', '1883'))
mqtt_user = os.environ.get('MQTT_USER')
mqtt_password = os.environ.get('MQTT_PASSWORD')
sleep = int(os.environ.get('SLEEP',  '15'))

# connect to MQTT Server and publish all items
mqtt = MQTT.Client("pegel-bridge")
flag_connected = 0

def on_connect(mqtt, userdata, flags, rc):
   global flag_connected
   flag_connected = 1

def on_disconnect(mqtt, userdata, rc):
   global flag_connected
   flag_connected = 0

mqtt.on_connect = on_connect
mqtt.on_disconnect = on_disconnect
mqtt.enable_logger(logger)

if mqtt_user and mqtt_password:
    mqtt.username_pw_set(mqtt_user, mqtt_password)
mqtt.connect(mqtt_server, mqtt_port)  
mqtt.loop_start()

def exit_handler():
    mqtt.disconnect()
    mqtt.loop_stop()

atexit.register(exit_handler)

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
    
    for item in data.items():
      nr = item[0]
      # get and add additional data from web
      urls = {f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{nr}/S/alm.json",
      f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{nr}/S/events.json",
      f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{nr}/S/ltv.json"
      }
      for url in urls:
        stationdata = []
        stationresp = urlopen(url)
        stationdata.append(json.loads(stationresp.read()))

        for set in stationdata:
          for entry in set:
              id = entry.get("station_no")
              shortname = entry.get("ts_shortname").split(".")[1]
              if len(entry.get("data")) > 0:
                data[nr][shortname] = entry.get("data")[-1][1]
              else:
                data[nr][shortname] = ''  
    
    return data

def publish_mqtt(item):
    # publish config to mqtt HA
    mqtt.publish(
          f"homeassistant/sensor/pegel_bridge/{item[0]}/config", 
          json.dumps(
              {
                  "name": item[1].get("location") +' '+ item[1].get("water"),
                  "state_topic": f"homeassistant/sensor/pegel_{item[0]}/state",
                  "json_attributes_topic": f"homeassistant/sensor/pegel_{item[0]}/attr",
                  "unit_of_measurement": item[1].get('unit'),
                  "value_template": "{{ value_json.level }}",
                  "device": {
                      "identifiers": ["pegel_bridge"],
                      "manufacturer": "ML/LH",
                      "model": "PEGEL OOE OPENDATA",
                      "name": "PEGEL OOE",
                  },
                  "unique_id": f"pegel_{item[0]}",
              }
          ),
      )
    # publish value to mqtt HA
    mqtt.publish(
          f"homeassistant/sensor/pegel_{item[0]}/state",
          json.dumps({"level": item[1].get('value')}),
      )
    # publish additional data to mqtt HA
    mqtt.publish(
          f"homeassistant/sensor/pegel_{item[0]}/attr",
          json.dumps(
              {
                  "Water": item[1].get('water'),
                  "Location": item[1].get('location'),
                  "timestamp": f"{str(nr[1].get('timestamp').isoformat()) if nr[1].get('timestamp') is not None else 'null'}",
                  "Voralarm": item[1].get('Voralarm'),
                  "Alarm1": item[1].get('Alarm1'),
                  "Alarm2": item[1].get('Alarm2'),
                  "Alarm3": item[1].get('Alarm3'),
                  "Last_Event": item[1].get('Event'),
                  "HW1": item[1].get('HW1'),
                  "HW2": item[1].get('HW2'),
                  "HW5": item[1].get('HW5'),
                  "HW10": item[1].get('HW10'),
                  "HW30": item[1].get('HW30'),
                  "HW100": item[1].get('HW100'),
                  "Niederwasser": item[1].get('NW'),
                  "Mittelwasser": item[1].get('MW'),
              }
          ),
      )
#%%
while True:
  #%%
  while flag_connected == 0:
     print('connecting')
     time.sleep(30)
  # %%
  data = get_pegel()

  # %%
  for nr in data.items():
      print(nr)
      publish_mqtt(nr)
  print('Pegel Sendt')

  gc.collect()
  # ein wenig schlafen
  time.sleep(60*sleep)
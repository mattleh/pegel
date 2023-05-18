#!/usr/bin/env python3

import logging
import requests
import datetime
import json
import time
import paho.mqtt.client as MQTT
import os
from sys import stdout
import atexit
from urllib.request import urlopen

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

flag_connected = 0

def on_connect(client, userdata, flags, rc):
   global flag_connected
   flag_connected = 1

def on_disconnect(client, userdata, rc):
   global flag_connected
   flag_connected = 0

# connect to MQTT Server and publish all items
mqtt = MQTT.Client("pegel-bridge")
mqtt.on_connect = on_connect
mqtt.on_disconnect = on_disconnect
mqtt.enable_logger(logger)

def exit_handler():
    mqtt.disconnect()

atexit.register(exit_handler)

while True:

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

    # Datenzeilen parsen jeweils Zeitstempel und Wert dazu mit Leerzeichen getrennt
    if line.decode('latin1').startswith('20'):
      if nr == None:
        continue
      line = line.decode('latin1')
      elements = line.split(' ')
      # erstes Element: Timecode parsen und in Unix-Timestamp verwandeln
      timestamp = int(datetime.datetime.strptime(elements[0], "%Y%m%d%H%M%S").timestamp())
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
  
  if flag_connected == 0:
      print("connecting mqtt")  
      if mqtt_user and mqtt_password:
          mqtt.username_pw_set(mqtt_user, mqtt_password)
      mqtt.connect(mqtt_server, mqtt_port)

  stationdata = []

  for nr, measurement in data.items():
    urls = {f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{nr}/S/alm.json",
        f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{nr}/S/events.json",
        f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{nr}/S/ltv.json"
    }
  
    for url in urls:
        stationresp = urlopen(url)
        stationdata.append(json.loads(stationresp.read()))

        for set in stationdata:
          for entry in set:
              id = entry.get("station_no")
              shortname = entry.get("ts_shortname").split(".")[1]
              try:
                data[id][shortname] = entry.get("data")[-1][1]
              except:
                print(entry.get("data"))

    name = measurement.get("location") + "-" + measurement.get("water")
    unit = measurement.get("unit")
    level = measurement.get("value")
    water = measurement.get("water")
    location = measurement.get("location")
    timest = measurement.get("timestamp")

    if data[nr].get("Voralarm"):
      voralarm = data[nr].get("Voralarm")
    else:
      voralarm = ""

    if data[nr].get("Alarm1"):
      alarm1 = data[nr].get("Alarm1")
    else:
      alarm1 = ""

    if data[nr].get("Alarm2"):
      alarm2 = data[nr].get("Alarm2")
    else:
      alarm2 = ""

    if data[nr].get("Alarm3"):
      alarm3 = data[nr].get("Alarm3")
    else:
      alarm3 = ""

    if data[nr].get("Event"):
      event = data[nr].get("Event")
    else:
      event = ""

    if data[nr].get("HW1"):
      hw1 = data[nr].get("HW1")
    else:
      hw1 = ""

    if data[nr].get("HW2"):
      hw2 = data[nr].get("HW2")
    else:
      hw2 = ""

    if data[nr].get("HW5"):
      hw5 = data[nr].get("HW5")
    else:
      hw5 = ""

    if data[nr].get("HW10"):
      hw10 = data[nr].get("HW10")
    else:
      hw10 = ""

    if data[nr].get("HW30"):
      hw30 = data[nr].get("HW30")
    else:
      hw30 = ""

    if data[nr].get("HW100"):
      hw100 = data[nr].get("HW100")
    else:
      hw100 = ""      

    if data[nr].get("NW"):
      nw = data[nr].get("NW")
    else:
      nw = ""

    if data[nr].get("MW"):
      mw = data[nr].get("MW")
    else:
      mw = ""

    autodiscover= mqtt.publish(
        f"homeassistant/sensor/pegel_bridge/{nr}/config", 
        json.dumps(
            {
                "name": name,
                "state_topic": f"homeassistant/sensor/pegel_{nr}/state",
                "json_attributes_topic": f"homeassistant/sensor/pegel_{nr}/attr",
                "unit_of_measurement": unit,
                "value_template": "{{ value_json.level }}",
                "device": {
                    "identifiers": ["pegel_bridge"],
                    "manufacturer": "ML/LH",
                    "model": "PEGEL OOE OPENDATA",
                    "name": "PEGEL OOE",
                },
                "unique_id": f"pegel_{nr}",
            }
        ),
    )
    result_state = mqtt.publish(
        f"homeassistant/sensor/pegel_{nr}/state",
        json.dumps({"level": level}),
    )
    result_attrs = mqtt.publish(
        f"homeassistant/sensor/pegel_{nr}/attr",
        json.dumps(
            {
                "Water": water,
                "Location": location,
                "timestamp": timest,
                "Voralarm": voralarm,
                "Alarm1": alarm1,
                "Alarm2": alarm2,
                "Alarm3": alarm3,
                "Last_Event": event,
                "HW1": hw1,
                "HW2": hw2,
                "HW5": hw5,
                "HW10": hw10,
                "HW30": hw30,
                "HW100": hw100,
                "Niederwasser": nw,
                "Mittelwasser": mw,
            }
        ),
    )
    
  print('Pegel Sendt')
  
  # ein wenig schlafen
  time.sleep(60*sleep)
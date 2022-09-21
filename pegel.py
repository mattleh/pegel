#!/usr/bin/env python3

import logging
import requests
import datetime
import json
import time
import paho.mqtt.client as MQTT
import os

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)

mqtt_server = os.environ.get('MQTT_SERVER')
mqtt_port = int(os.environ.get('MQTT_PORT', 1883))
mqtt_user = os.environ['MQTT_USER']
mqtt_password = os.environ['MQTT_PASSWORD']
sleep = int(os.environ.get('SLEEP',  15))

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
          data[nr] = { 'id' : nr }
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

  # connect to MQTT Server and publish all items
  mqtt = MQTT.Client()
  mqtt.enable_logger(logger)
  
  if mqtt_user and mqtt_password:
    mqtt.username_pw_set(mqtt_user, mqtt_password)
  mqtt.connect(mqtt_server, mqtt_port)
  for nr, measurement in data.items():
    mqtt.publish("jarvis/water_level/"+nr, json.dumps(measurement, ensure_ascii=False))
  mqtt.disconnect()

  # ein wenig schlafen
  time.sleep(60*sleep)

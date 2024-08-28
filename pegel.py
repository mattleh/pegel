#!/usr/bin/env python
# coding: utf-8

# In[1]:


import logging
import requests
import datetime
from dateutil import parser, tz
import json
import time
from paho.mqtt import client as mqtt_client
import os
import atexit
from urllib.request import urlopen
import aiohttp
import gc
import asyncio


# In[2]:


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
            # data[nr] = { 'unique_id' : nr }
            data[nr] = {}
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
        from_zone = tz.gettz(data[nr]['tz'])
        # erstes Element: Timecode parsen und in Unix-Timestamp verwandeln
        #timestamp = int(datetime.datetime.strptime(elements[0], "%Y%m%d%H%M%S").timestamp())
        #timestamp = parser.parse(f'{elements[0]} {data[nr].get("tz")[3:]}')
        timestamp = parser.parse(elements[0])
        timestamp = timestamp.replace(tzinfo=from_zone)
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

    return data


# In[3]:

async def fetch_page(session, url):
    # make GET request using session
    async with session.get(url) as response:
        # return HTML content
        return await response.json()

async def get_data(urls):
    connector = aiohttp.TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(connector=connector) as session:
 
        # Initialize tasks list
        tasks = []

        for url in urls:
            tasks.append(fetch_page(session, url))
 
        # group and Execute tasks concurrently
        htmls = await asyncio.gather(*tasks)
    return htmls


# In[4]:


broker = os.environ.get('MQTT_SERVER')
port = int(os.environ.get('MQTT_PORT', '1883'))
topic = "homeassistant/sensor"
#broker = 'mqtt.lan'
#port = 1883
#topic = 'test'

client_id = 'pegel-bridge'
username = os.environ.get('MQTT_USER')
password = os.environ.get('MQTT_PASSWORD')
sleep = int(os.environ.get('SLEEP',  '15'))


def connect_mqtt():
    def on_connect(client, userdata, flags, reason_code, properties):
        if flags.session_present:
            pass# ...
        if reason_code == 0:
            print("Connected to MQTT Broker!")
        if reason_code > 0:
            print(f'Failed to connect, return code {reason_code}\n')
    
    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2, client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(mqtt, item):
    # publish config to mqtt HA
    p = mqtt.publish(
          f"{topic}/pegel_bridge/{item[0]}/config", 
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
                  "device_class": f"{str(item[1].get('timestamp').isoformat()) if item[1].get('timestamp') is not None else 'null'}",
                  "unique_id": f"pegel_{item[0]}",
              }
          ),
      )
    p.wait_for_publish()
    # publish value to mqtt HA
    p = mqtt.publish(
          f"{topic}/pegel_{item[0]}/state",
          json.dumps({"level": item[1].get('value')}),
      )
    p.wait_for_publish()
    # publish additional data to mqtt HA
    p = mqtt.publish(
          f"{topic}/pegel_{item[0]}/attr",
          json.dumps(
              {
                  "Water": item[1].get('water'),
                  "Location": item[1].get('location'),
                  "timestamp": f"{str(item[1].get('timestamp').isoformat()) if item[1].get('timestamp') is not None else 'null'}",
                  "Voralarm": f"{str(item[1].get('Voralarm')['data'][0][-1]) if item[1].get('Voralarm')['data'] else ''}",
                  "Alarm1": f"{str(item[1].get('Alarm1')['data'][0][-1]) if item[1].get('Alarm1')['data'] else ''}",
                  "Alarm2": f"{str(item[1].get('Alarm2')['data'][0][-1]) if item[1].get('Alarm2')['data'] else ''}",
                  "Alarm3": f"{str(item[1].get('Alarm3')['data'][0][-1]) if item[1].get('Alarm3')['data'] else ''}",
                  "Last_Event": f"{str(item[1].get('Event').get('data')[-1][-1]) if item[1].get('Event').get('data') else ''}",
                  "HW1": f"{str(item[1].get('HW1')['data'][0][-1]) if item[1].get('HW1')['data'] else ''}",
                  "HW2": f"{str(item[1].get('HW2')['data'][0][-1]) if item[1].get('HW2')['data'] else ''}",
                  "HW5": f"{str(item[1].get('HW5')['data'][0][-1]) if item[1].get('HW5')['data'] else ''}",
                  "HW10": f"{str(item[1].get('HW10')['data'][0][-1]) if item[1].get('HW10')['data'] else ''}",
                  "HW30": f"{str(item[1].get('HW30')['data'][0][-1]) if item[1].get('HW30')['data'] else ''}",
                  "HW100": f"{str(item[1].get('HW100')['data'][0][-1]) if item[1].get('HW100')['data'] else ''}",
                  "Niederwasser": f"{str(item[1].get('NW').get('data')[-1][-1]) if item[1].get('NW').get('data') else ''}",
                  "Mittelwasser": f"{str(item[1].get('MW').get('data')[-1][-1]) if item[1].get('MW').get('data') else ''}"
              }
          ),
      )
    p.wait_for_publish()


def run():
    client = connect_mqtt()
    client.loop_start()
    for i in data.items():
        publish(client, i)
    client.loop_stop()


# In[5]:


data = get_pegel()


# In[6]:


urls = []
for item, value in data.items():
  nr = item
  # get and add additional data from web
  urls.append(f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{nr}/S/alm.json")
  urls.append(f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{nr}/S/events.json")
  urls.append(f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{nr}/S/ltv.json")


# In[ ]:


lastsync = datetime.datetime.now() - datetime.timedelta(days=2)
jsons = []
while True:
    start = time.time()
    data = get_pegel()
    if not jsons or (lastsync - datetime.datetime.now()).days >= 1: 
        print("Update Additional Data")
        jsons = asyncio.run(get_data(urls))
        lastsync = datetime.datetime.now()

    end = time.time()
    #print(data)
    print(f"Took {end - start:.2f}s collecting data")

    for i in jsons:
        for j in i:
            data[j["station_no"]][j["ts_shortname"].split(".")[-1]] = j
     
    run()
    print('Pegel Sendt')

    # ein wenig schlafen
    time.sleep(60*sleep)

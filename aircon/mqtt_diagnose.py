"""Diagnose-Entitaeten fuer Home Assistant per MQTT-Discovery.

Je Anlage:
  * Last contact   - Zeitstempel der letzten Anfrage der Anlage an das Add-on (hoechstens 1x/min)
  * Last change    - Zeitstempel der letzten Wertaenderung der Anlage
  * Problem        - an, wenn error_code ungleich 0 ist (device_class problem, Name von HA uebersetzt)
  * Command backlog- an, wenn die Anlage anstehende Befehle nicht abholt (stiller Haenger)
  * je Datenpunkt, den das Add-on sonst nicht auswertet und der einen echten Wert hat (nicht
    65535), ein Diagnose-Sensor. Nur die nuetzlichen sind von Anfang an aktiv, die uebrigen
    werden deaktiviert angelegt und koennen in HA einzeln eingeschaltet werden.

Alle Werte werden 'retained' veroeffentlicht: die Anlage schickt viele Datenpunkte nur beim
Verbinden, Home Assistant soll sie auch nach einem eigenen Neustart noch kennen.
"""
from datetime import datetime, timezone
import json
import logging
import threading
import time

_KONTAKT_ABSTAND = 60.0  # Last contact hoechstens einmal pro Minute
_AENDERUNG_ABSTAND = 10.0
_MAX_STATE = 255

# Datenpunkte, die von Anfang an aktiv sind
AKTIV = {'error_code', 'op_status', 'wifi_led_enable'}

# Englische Anzeigenamen; alles andere wird aus dem Namen des Datenpunkts gebildet
NAMEN = {
    'error_code': 'Error code',
    'op_status': 'Operation status',
    'wifi_led_enable': 'Wi-Fi LED',
    'model_name': 'Model',
    'mcu_fw_version': 'Controller firmware',
    'oem_host_version': 'Wi-Fi module firmware',
    'device_name': 'Device name',
    'device_capabilities': 'Device capabilities',
    'af_vertical_num_dir': 'Vertical airflow positions',
    'af_horizontal_num_dir': 'Horizontal airflow positions',
    'min_heat': 'Minimum heat',
    'human_det': 'Human detection',
    'human_det_auto_off': 'Human detection auto off',
    'human_det_auto_on_off': 'Human detection auto on/off',
    'human_det_auto_save': 'Human detection auto save',
    'coil_dry_mode': 'Coil dry mode',
    'external_thermostat_off': 'External thermostat off',
    'heat_sterilizatio': 'Heat sterilization',
    'comm_version': 'Communication version',
    'ac_info1': 'AC info 1',
    'monitor1': 'Monitor 1',
    'monitor2': 'Monitor 2',
    'ota_status': 'OTA status',
    'ota_status2': 'OTA status 2',
    'ota_completed': 'OTA completed',
}


def platzhalter(wert) -> bool:
  """65535 bzw. Listen nur aus 65535/leer = vom Modell nicht unterstuetzt."""
  if wert is None:
    return True
  if isinstance(wert, (int, float)):
    return int(wert) == 65535
  teile = [t for t in str(wert).replace(':', ',').split(',')]
  return all(t.strip() in ('', '65535') for t in teile)


def anzeigename(name: str) -> str:
  return NAMEN.get(name) or name.replace('_', ' ').strip().capitalize()


def _jetzt_iso() -> str:
  return datetime.now(timezone.utc).isoformat(timespec='seconds')


class MqttDiagnose:

  def __init__(self, client, topics: dict, praefix: str):
    self._client = client
    self._topics = topics
    self._praefix = praefix
    self._lock = threading.Lock()
    self._angelegt = set()
    self._zuletzt = {}  # (mac, art) -> Zeit der letzten Veroeffentlichung

  # ------------------------------------------------------------- Hilfen
  def _geraet(self, device) -> dict:
    return {'identifiers': [f'hisense_ac_{device.mac_address}']}

  def _state_topic(self, device, name: str) -> str:
    return self._topics['pub'].format(device.mac_address, name)

  def _anlegen(self, device, plattform: str, name: str, extra: dict) -> None:
    schluessel = (device.mac_address, name)
    if schluessel in self._angelegt:
      return
    config = {
        'unique_id': f'{device.mac_address}_{name}',
        'state_topic': self._state_topic(device, name),
        'availability_topic': self._topics['lwt'],
        'entity_category': 'diagnostic',
        'device': self._geraet(device),
    }
    config.update(extra)
    config = {k: v for k, v in config.items() if v is not None or k == 'name'}
    self._client.publish(f'{self._praefix}/{plattform}/{device.mac_address}/{name}/config',
                         payload=json.dumps(config), retain=True)
    self._angelegt.add(schluessel)

  def _senden(self, device, name: str, wert) -> None:
    self._client.publish(self._state_topic(device, name), payload=str(wert), retain=True)

  def _gedrosselt(self, device, art: str, abstand: float) -> bool:
    jetzt = time.time()
    s = (device.mac_address, art)
    if jetzt - self._zuletzt.get(s, 0) < abstand:
      return True
    self._zuletzt[s] = jetzt
    return False

  # ------------------------------------------------------------- feste Entitaeten
  def anlage_anlegen(self, device) -> None:
    with self._lock:
      self._anlegen(device, 'sensor', 'last_contact',
                    {'name': 'Last contact', 'device_class': 'timestamp'})
      self._anlegen(device, 'sensor', 'last_change',
                    {'name': 'Last change', 'device_class': 'timestamp'})
      self._anlegen(device, 'binary_sensor', 'problem',
                    {'name': None, 'device_class': 'problem', 'payload_on': 'ON',
                     'payload_off': 'OFF'})
      self._anlegen(device, 'binary_sensor', 'command_backlog',
                    {'name': 'Command backlog', 'device_class': 'problem', 'payload_on': 'ON',
                     'payload_off': 'OFF', 'icon': 'mdi:tray-full'})
      self._senden(device, 'command_backlog', 'OFF')
      # Innentemperatur als eigener Sensor (bisher nur Attribut der Klima-Entitaet)
      quelle = device.topics.get('display_temperature') or device.topics.get('env_temp')
      if quelle:
        self._anlegen(device, 'sensor', 'indoor_temperature', {
            'name': 'Indoor temperature',
            'state_topic': self._state_topic(device, quelle),
            'entity_category': None,
            'device_class': 'temperature',
            'state_class': 'measurement',
            'unit_of_measurement': '°F' if device.is_fahrenheit else '°C',
            'suggested_display_precision': 1,
        })

  def kontakt(self, device) -> None:
    with self._lock:
      if not self._gedrosselt(device, 'kontakt', _KONTAKT_ABSTAND):
        self._senden(device, 'last_contact', _jetzt_iso())

  def aenderung(self, device) -> None:
    with self._lock:
      if not self._gedrosselt(device, 'aenderung', _AENDERUNG_ABSTAND):
        self._senden(device, 'last_change', _jetzt_iso())

  def haenger(self, device, an: bool) -> None:
    with self._lock:
      self._senden(device, 'command_backlog', 'ON' if an else 'OFF')

  # ------------------------------------------------------------- unbekannte Datenpunkte
  def datenpunkt(self, device, name: str, wert) -> None:
    if platzhalter(wert):
      return
    with self._lock:
      extra = {'name': anzeigename(name), 'enabled_by_default': name in AKTIV}
      text = str(wert)
      if len(text) > _MAX_STATE:
        attr_topic = self._topics['pub'].format(device.mac_address, name) + '/attributes'
        extra['json_attributes_topic'] = attr_topic
        self._client.publish(attr_topic, payload=json.dumps({'value': text}), retain=True)
        text = text[:_MAX_STATE - 1] + '…'
      if name == 'error_code':
        extra['icon'] = 'mdi:alert-circle-outline'
      self._anlegen(device, 'sensor', name, extra)
      self._senden(device, name, text)
      if name == 'error_code':
        try:
          self._senden(device, 'problem', 'ON' if int(wert) != 0 else 'OFF')
        except (TypeError, ValueError):
          pass

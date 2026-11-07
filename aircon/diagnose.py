"""Diagnose fuer die lokale Verbindung zu den Klimaanlagen.

Sammelt je Anlage:
  * unbekannte Datenpunkte (Name, erster/letzter Wert, Anzahl, letzte Aenderungen),
  * den Verbindungsverlauf (letzter Kontakt der Anlage, Keep-Alive-Fehler mit Fehlerart,
    Dauer von Ausfaellen, Schluesseltausch).

Ergebnis: knappe Log-Zeilen bei Zustandswechseln (statt einer Fehlerzeile je Versuch) und
JSON-Dateien unter $CONFIG_DIR/diagnose/. Es werden NIE Schluessel, Tokens oder Passwoerter
gespeichert oder protokolliert - nur Namen, Werte von Datenpunkten, Zeiten und Fehlerarten.
"""
import json
import logging
import os
import threading
import time

_MAX_AENDERUNGEN = 20
_SCHREIB_ABSTAND = 30.0  # Sekunden zwischen zwei Schreibvorgaengen je Datei
_HAENGER_NACH = 60.0  # Befehle so lange nicht abgeholt -> stiller Haenger


def _zeit(t):
  return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t)) if t else None


def fehlertext(fehler: BaseException) -> str:
  """Kurzer, sicherer Text zu einem Fehler (str() kann bei aiohttp-Fehlern selbst scheitern)."""
  ursache = getattr(fehler, 'os_error', None)
  try:
    text = str(ursache) if ursache is not None else str(fehler)
  except Exception:
    text = repr(getattr(fehler, 'args', ''))
  return text[:200]


class _Anlage:

  def __init__(self, name: str, ip: str):
    self.name = name
    self.ip = ip
    self.unbekannt = {}
    self.letzter_kontakt = None  # letzte Anfrage DER ANLAGE an uns
    self.letzter_kontakt_art = None
    self.letztes_keepalive_ok = None
    self.ausfall_seit = None
    self.fehlversuche = 0
    self.fehlerarten = {}
    self.letzter_fehler = None
    self.ausfaelle = 0
    self.schluesseltausch = 0
    self.letzter_schluesseltausch = None
    self.erholt_um = None
    self.wartet_seit = None  # Befehle stehen an, seit dann kein Abholen mehr
    self.wartend = 0
    self.haenger_gemeldet = False
    self.haenger = 0


class Diagnose:

  def __init__(self, verzeichnis: str = None):
    basis = verzeichnis or os.environ.get('CONFIG_DIR') or '.'
    self._verzeichnis = os.path.join(basis, 'diagnose')
    self._anlagen = {}
    self._lock = threading.Lock()
    self._geschrieben = {}
    self.ausgabe = None  # optional: MqttDiagnose fuer Entitaeten in Home Assistant

  def _aus(self, methode: str, *args) -> None:
    if self.ausgabe is None:
      return
    try:
      getattr(self.ausgabe, methode)(*args)
    except Exception as ex:
      logging.debug('Diagnose-Entitaet nicht aktualisiert (%s): %r', methode, ex)

  def _anlage(self, device) -> _Anlage:
    a = self._anlagen.get(device.ip_address)
    if a is None:
      a = self._anlagen[device.ip_address] = _Anlage(device.name, device.ip_address)
    return a

  # ---------------------------------------------------------------- unbekannte Datenpunkte
  def unbekannt(self, device, name: str, wert) -> None:
    jetzt = time.time()
    with self._lock:
      a = self._anlage(device)
      e = a.unbekannt.get(name)
      if e is None:
        a.unbekannt[name] = {
            'erstmals': _zeit(jetzt), 'zuletzt': _zeit(jetzt), 'anzahl': 1,
            'erster_wert': wert, 'letzter_wert': wert, 'aenderungen': []
        }
        logging.info('Unbekannter Datenpunkt bei %s: %s = %r', a.name, name, wert)
        self._schreiben(a, 'unbekannt', sofort=True)
        self._aus('datenpunkt', device, name, wert)
        return
      e['zuletzt'] = _zeit(jetzt)
      e['anzahl'] += 1
      if wert != e['letzter_wert']:
        logging.info('Unbekannter Datenpunkt bei %s geaendert: %s = %r -> %r', a.name, name,
                     e['letzter_wert'], wert)
        e['aenderungen'] = (e['aenderungen'] + [[_zeit(jetzt), e['letzter_wert'], wert]
                                                ])[-_MAX_AENDERUNGEN:]
        e['letzter_wert'] = wert
        self._schreiben(a, 'unbekannt', sofort=True)
        self._aus('datenpunkt', device, name, wert)
        self._aus('aenderung', device)
      else:
        self._schreiben(a, 'unbekannt')

  def aenderung(self, device) -> None:
    """Ein bekannter Wert der Anlage hat sich geaendert."""
    self._aus('aenderung', device)

  # ---------------------------------------------------------------- Verbindung
  def kontakt(self, device, art: str) -> None:
    """Die Anlage hat uns angefragt (commands, datapoint, key_exchange)."""
    jetzt = time.time()
    with self._lock:
      a = self._anlage(device)
      a.letzter_kontakt, a.letzter_kontakt_art = jetzt, art
      self._aus('kontakt', device)
      if art == 'commands' and a.wartet_seit:
        a.wartet_seit = jetzt  # Fortschritt: die Anlage holt ab
      if art == 'key_exchange':
        a.schluesseltausch += 1
        a.letzter_schluesseltausch = jetzt
        seit = f', {jetzt - a.erholt_um:.1f} s nach Wiedererreichbarkeit' if a.erholt_um and jetzt - a.erholt_um < 600 else ''
        logging.info('Schluesseltausch mit %s (%s), Nr. %d%s', a.name, a.ip, a.schluesseltausch, seit)
        self._schreiben(a, 'status', sofort=True)
      else:
        self._schreiben(a, 'status')

  def keepalive_ok(self, device) -> None:
    jetzt = time.time()
    with self._lock:
      a = self._anlage(device)
      a.letztes_keepalive_ok = jetzt
      if a.ausfall_seit:
        dauer = jetzt - a.ausfall_seit
        kontakt = f'{jetzt - a.letzter_kontakt:.0f} s' if a.letzter_kontakt else 'nie'
        arten = ', '.join(f'{k} {v}x' for k, v in a.fehlerarten.items())
        logging.warning('%s (%s) wieder erreichbar nach %.1f min, %d Fehlversuche (%s), '
                        'letzte Anfrage der Anlage vor %s', a.name, a.ip, dauer / 60,
                        a.fehlversuche, arten, kontakt)
        a.ausfall_seit, a.fehlversuche, a.fehlerarten = None, 0, {}
        a.erholt_um = jetzt
        self._schreiben(a, 'status', sofort=True)
      else:
        self._schreiben(a, 'status')

  def keepalive_fehler(self, device, fehler: BaseException) -> None:
    jetzt = time.time()
    art = type(fehler).__name__
    text = fehlertext(fehler)
    with self._lock:
      a = self._anlage(device)
      a.fehlversuche += 1
      a.fehlerarten[art] = a.fehlerarten.get(art, 0) + 1
      a.letzter_fehler = {'zeit': _zeit(jetzt), 'art': art, 'text': text}
      if not a.ausfall_seit:
        a.ausfall_seit = jetzt
        a.ausfaelle += 1
        kontakt = f'{jetzt - a.letzter_kontakt:.0f} s' if a.letzter_kontakt else 'nie'
        ok = f'{jetzt - a.letztes_keepalive_ok:.0f} s' if a.letztes_keepalive_ok else 'nie'
        logging.warning('%s (%s) nicht erreichbar: %s: %s | letzte Anfrage der Anlage vor %s, '
                        'letztes erfolgreiches Keep-Alive vor %s', a.name, a.ip, art, text,
                        kontakt, ok)
        self._schreiben(a, 'status', sofort=True)
      else:
        logging.debug('%s (%s) weiterhin nicht erreichbar (%d. Versuch): %s', a.name, a.ip,
                      a.fehlversuche, art)
        self._schreiben(a, 'status')

  def warteschlange(self, device, anzahl: int) -> None:
    """Erkennt stille Haenger: Befehle stehen an, die Anlage holt sie aber nicht ab."""
    jetzt = time.time()
    with self._lock:
      a = self._anlage(device)
      a.wartend = anzahl
      if anzahl <= 0:
        if a.haenger_gemeldet:
          logging.warning('%s (%s) holt Befehle wieder ab (Haenger %.1f min)', a.name, a.ip,
                          (jetzt - a.haenger_gemeldet) / 60)
          a.haenger_gemeldet = False
          self._schreiben(a, 'status', sofort=True)
          self._aus('haenger', device, False)
        a.wartet_seit = None
        return
      if a.wartet_seit is None:
        a.wartet_seit = jetzt
      elif not a.haenger_gemeldet and jetzt - a.wartet_seit > _HAENGER_NACH:
        kontakt = f'{jetzt - a.letzter_kontakt:.0f} s' if a.letzter_kontakt else 'nie'
        ok = f'{jetzt - a.letztes_keepalive_ok:.0f} s' if a.letztes_keepalive_ok else 'nie'
        logging.warning('%s (%s) holt seit %.0f s keine Befehle ab, %d warten | letzte Anfrage '
                        'der Anlage vor %s (%s), letztes erfolgreiches Keep-Alive vor %s', a.name,
                        a.ip, jetzt - a.wartet_seit, anzahl, kontakt, a.letzter_kontakt_art, ok)
        a.haenger_gemeldet = jetzt
        a.haenger += 1
        self._schreiben(a, 'status', sofort=True)
        self._aus('haenger', device, True)

  # ---------------------------------------------------------------- Dateien
  def _status(self, a: _Anlage) -> dict:
    return {
        'name': a.name, 'ip': a.ip, 'stand': _zeit(time.time()),
        'letzter_kontakt': _zeit(a.letzter_kontakt), 'letzter_kontakt_art': a.letzter_kontakt_art,
        'letztes_keepalive_ok': _zeit(a.letztes_keepalive_ok),
        'ausfall_seit': _zeit(a.ausfall_seit), 'fehlversuche': a.fehlversuche,
        'fehlerarten': a.fehlerarten, 'letzter_fehler': a.letzter_fehler,
        'ausfaelle_seit_start': a.ausfaelle, 'schluesseltausch_seit_start': a.schluesseltausch,
        'letzter_schluesseltausch': _zeit(a.letzter_schluesseltausch),
        'befehle_wartend': a.wartend, 'wartet_seit': _zeit(a.wartet_seit),
        'haenger_seit': _zeit(a.haenger_gemeldet or None), 'haenger_seit_start': a.haenger
    }

  def _schreiben(self, a: _Anlage, art: str, sofort: bool = False) -> None:
    schluessel = (a.ip, art)
    jetzt = time.time()
    if not sofort and jetzt - self._geschrieben.get(schluessel, 0) < _SCHREIB_ABSTAND:
      return
    self._geschrieben[schluessel] = jetzt
    inhalt = self._status(a) if art == 'status' else {'name': a.name, 'ip': a.ip,
                                                       'datenpunkte': a.unbekannt}
    try:
      os.makedirs(self._verzeichnis, exist_ok=True)
      datei = os.path.join(self._verzeichnis, f'{art}_{a.ip.replace(".", "_")}.json')
      tmp = datei + '.tmp'
      with open(tmp, 'w') as f:
        json.dump(inhalt, f, indent=1, ensure_ascii=False, default=str)
      os.replace(tmp, datei)
    except OSError as ex:
      logging.debug('Diagnose-Datei nicht geschrieben: %r', ex)


DIAGNOSE = Diagnose()

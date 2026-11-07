import aiohttp
import asyncio
import concurrent
from dataclasses import dataclass
from http import HTTPStatus
import json
import logging
import socket
import sys
from tenacity import retry, retry_if_exception_type, wait_exponential, stop_after_attempt
import time
import threading

from .aircon import Device
from .diagnose import DIAGNOSE, fehlertext

if sys.version_info < (3, 8):
  TimeoutError = concurrent.futures.TimeoutError
else:
  TimeoutError = asyncio.exceptions.TimeoutError


@dataclass
class _NotifyConfiguration:
  device: Device
  headers: dict
  last_timestamp: int


def _run_after_failure(retry_state):
  config = retry_state.kwargs['config']
  config.device.available = False
  return 0


class Notifier:
  _KEEP_ALIVE_INTERVAL = 10.0
  _TIME_TO_HANDLE_REQUESTS = 100e-3

  def __init__(self, port: int, local_ip: str):
    self._configurations = []
    self._condition = asyncio.Condition()

    self._running = False

    local_ip = local_ip or self._get_local_ip()
    self._json = {'local_reg': {'ip': local_ip, 'notify': 0, 'port': port, 'uri': '/local_lan'}}

  def _get_local_ip(self):
    sock = None
    try:
      sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
      sock.connect(('10.255.255.255', 1))
      return sock.getsockname()[0]
    finally:
      if sock:
        sock.close()

  def register_device(self, device: Device):
    if device not in (conf.device for conf in self._configurations):
      headers = {
          'Accept': 'application/json',
          'Connection': 'keep-alive',
          'Content-Type': 'application/json',
          'Host': device.ip_address,
          'Accept-Encoding': 'gzip'
      }
      self._configurations.append(_NotifyConfiguration(device, headers, 0))

  async def _notify(self):
    async with self._condition:
      self._condition.notify_all()

  def notify(self):
    loop = asyncio.get_event_loop()
    asyncio.run_coroutine_threadsafe(self._notify(), loop)

  async def start(self, session: aiohttp.ClientSession):
    """Startet je Anlage eine eigene Keep-Alive-Schleife.

    Frueher liefen die Keep-Alives aller Anlagen in einer gemeinsamen Runde (gather): hing eine
    Anlage in ihren Wiederholungen (bis ca. 1 min), bekamen die anderen in der Zeit weder
    Keep-Alive noch Befehls-Benachrichtigung - sie mussten neu verschluesseln bzw. holten
    Befehle verspaetet ab. Jetzt bremst eine ausgefallene Anlage die anderen nicht mehr.
    """
    self._running = True
    await asyncio.gather(*(self._device_loop(session, config) for config in self._configurations))

  async def _device_loop(self, session: aiohttp.ClientSession, config: _NotifyConfiguration):
    while self._running:
      try:
        queue_size = await self._perform_request(session=session, config=config)
      except Exception:
        logging.exception('[KeepAlive] Unerwarteter Fehler fuer %s', config.device.ip_address)
        queue_size = 0
        await asyncio.sleep(self._KEEP_ALIVE_INTERVAL)
      DIAGNOSE.warteschlange(config.device, config.device.commands_queue.qsize())
      if queue_size <= 1:
        logging.debug('[KeepAlive] %s: Waiting for notification or timeout', config.device.ip_address)
        async with self._condition:
          try:
            await asyncio.wait_for(self._condition.wait(), timeout=self._KEEP_ALIVE_INTERVAL)
          except TimeoutError:
            pass
      else:
        # give some time to clean up the queues
        await asyncio.sleep(self._TIME_TO_HANDLE_REQUESTS)

  async def stop(self):
    self._running = False
    await self._notify()

  @retry(retry=retry_if_exception_type(ConnectionError),
         retry_error_callback=_run_after_failure,
         wait=wait_exponential(exp_base=1.6, max=10),
         stop=stop_after_attempt(6))
  async def _perform_request(self, session: aiohttp.ClientSession,
                             config: _NotifyConfiguration) -> int:
    now = time.time()
    queue_size = config.device.commands_queue.qsize()
    if (queue_size == 0 or
        not config.device.available) and now - config.last_timestamp < self._KEEP_ALIVE_INTERVAL:
      return 0
    method = 'PUT' if config.device.available else 'POST'
    self._json['local_reg']['notify'] = int(config.device.commands_queue.qsize() > 0)
    url = f'http://{config.device.ip_address}/local_reg.json'
    logging.debug(f'[KeepAlive] Sending {method} {url} {json.dumps(self._json)}')
    try:
      async with session.request(method, url, json=self._json, headers=config.headers) as resp:
        if resp.status != HTTPStatus.ACCEPTED.value:
          resp_data = await resp.text()
          fehler = ConnectionError(f'Sending local_reg failed: {resp.status}, {resp_data}')
          DIAGNOSE.keepalive_fehler(config.device, fehler)
          logging.debug(f'[KeepAlive] Sending local_reg failed: {resp.status}, {resp_data}')
          raise fehler
    except (aiohttp.client_exceptions.ClientConnectorError,
            aiohttp.client_exceptions.ClientConnectionError, TimeoutError) as e:
      # Diagnose meldet den Beginn des Ausfalls einmal (mit Fehlerart), Wiederholungen nur debug.
      DIAGNOSE.keepalive_fehler(config.device, e)
      logging.debug(f'Failed to connect to {config.device.ip_address}, maybe it is offline? {fehlertext(e)}')
      raise ConnectionError(
          f'Failed to connect to {config.device.ip_address}, maybe it is offline?')
    config.last_timestamp = now
    DIAGNOSE.keepalive_ok(config.device)
    config.device.available = True
    return queue_size

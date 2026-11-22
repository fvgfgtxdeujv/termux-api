"""
ble_bridge - Python client for the Termux:API BLE bridge.

Talks to the BleSocketServer running inside a patched Termux:API app over a
loopback TCP socket (127.0.0.1:7362) using newline-delimited JSON-RPC.
Android SELinux blocks cross-app abstract Unix sockets, hence TCP loopback.

This is a local module (not a pip package) so it can be dropped straight into
Termux. Usage:

    from ble_bridge import BleClient, BleScanner

    async with BleScanner() as scanner:
        devices = await scanner.discover(timeout=5.0)

    async with BleClient("AA:BB:CC:DD:EE:FF") as client:
        services = await client.get_services()
        value = await client.read_gatt_char("00002a00-0000-1000-8000-00805f9b34fb")
"""

from __future__ import annotations

import asyncio
import binascii
import itertools
import json
import socket
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

# Loopback TCP instead of abstract Unix socket: Android SELinux blocks
# cross-app abstract sockets, but any app with INTERNET can reach localhost.
TCP_HOST = "127.0.0.1"
TCP_PORT = 7362


class BleBridgeError(Exception):
    """Error returned by the Java bridge or a transport failure."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass
class Advertisement:
    address: str
    name: Optional[str]
    rssi: int
    data: str = ""

    @property
    def bytes(self) -> bytes:
        return bytes.fromhex(self.data) if self.data else b""


@dataclass
class GattCharacteristic:
    uuid: str
    properties: int
    descriptors: list[str]


@dataclass
class GattService:
    uuid: str
    characteristics: list[GattCharacteristic]


class _Transport:
    """Synchronous socket wrapper. All blocking I/O runs in an executor."""

    def __init__(self):
        self._sock: Optional[socket.socket] = None
        self._file = None
        self._lock = asyncio.Lock()

    async def connect(self):
        await asyncio.to_thread(self._connect_sync)

    def _connect_sync(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(30)
        s.connect((TCP_HOST, TCP_PORT))
        self._sock = s
        self._file = s.makefile("rwb", buffering=0)

    def _send_sync(self, payload: dict):
        assert self._file is not None
        data = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
        self._file.write(data)

    def send(self, payload: dict):
        return self._send_sync(payload)

    def readline(self) -> Optional[dict]:
        assert self._file is not None
        line = self._file.readline()
        if not line:
            return None
        return json.loads(line.decode("utf-8"))

    async def close(self):
        await asyncio.to_thread(self._close_sync)

    def _close_sync(self):
        if self._file:
            try:
                self._file.close()
            except Exception:
                pass
            self._file = None
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None


class BleBridge:
    """
    Low-level JSON-RPC client. Usually you want BleScanner / BleClient instead.

    Example:
        bridge = BleBridge()
        await bridge.connect()
        await bridge.call("get_state")
        await bridge.close()
    """

    TOKEN_PATH = "~/.config/ble_bridge/token"

    def __init__(self, notification_handler: Optional[Callable[[dict], None]] = None,
                 event_handler: Optional[Callable[[dict], None]] = None,
                 token: Optional[str] = None):
        self._transport = _Transport()
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._reader_task: Optional[asyncio.Task] = None
        self._notification_handler = notification_handler
        self._event_handler = event_handler
        self._token = token if token is not None else self._load_token()
        # address -> per-device notify router (installed by BleClient)
        self._notify_routes: dict[str, Callable[[dict], None]] = {}
        # address -> per-device state-event router (installed by BleClient)
        self._state_routes: dict[str, Callable[[dict], None]] = {}
        self._connected = False

    @classmethod
    def _load_token(cls) -> Optional[str]:
        import os
        path = os.path.expanduser(cls.TOKEN_PATH)
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
        except FileNotFoundError:
            return None

    @classmethod
    def save_token(cls, token: str):
        """Persist the pairing token so future clients authenticate automatically."""
        import os
        path = os.path.expanduser(cls.TOKEN_PATH)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(token.strip())
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass

    async def connect(self):
        if self._connected:
            return
        await self._transport.connect()
        self._connected = True
        self._reader_task = asyncio.create_task(self._reader_loop())
        # Authenticate. Same-UID installs accept any token (including empty);
        # otherwise a valid pairing token is required. Must run after the
        # reader loop starts so we can await the response.
        try:
            await self.call("auth", params={"token": self._token or ""}, rpc_timeout=5.0)
        except BleBridgeError as e:
            if e.code == "unauthorized":
                raise BleBridgeError("unauthorized",
                                     "BLE bridge requires a pairing token. Open the Termux:API app, "
                                     "copy the 'BLE bridge pairing token', then run:\n"
                                     "    BleBridge.save_token('<token>')\n"
                                     "and retry.")
            raise

    async def close(self):
        self._connected = False
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):
                pass
            self._reader_task = None
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(BleBridgeError("disconnected", "Bridge closed"))
        self._pending.clear()
        await self._transport.close()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def _reader_loop(self):
        loop = asyncio.get_running_loop()
        while self._connected:
            try:
                msg = await asyncio.to_thread(self._transport.readline)
            except Exception as e:
                msg = {"event": "error", "params": {"code": "transport", "message": str(e)}}
            if msg is None:
                break
            if "id" in msg and ("ok" in msg):
                fut = self._pending.pop(msg["id"], None)
                if fut and not fut.done():
                    if msg["ok"]:
                        fut.set_result(msg.get("result", {}))
                    else:
                        err = msg.get("error", {})
                        fut.set_exception(BleBridgeError(
                            err.get("code", "error"), err.get("message", "Unknown error")))
            else:
                event = msg.get("event", "")
                params = msg.get("params", {})
                if event == "notify":
                    addr = str(params.get("address", "")).lower()
                    # Prefer the per-device route; fall back to global handler.
                    route = self._notify_routes.get(addr)
                    if route:
                        try:
                            route(params)
                        except Exception:
                            pass
                    elif self._notification_handler:
                        try:
                            self._notification_handler(params)
                        except Exception:
                            pass
                # Route connection-lifecycle events by address to the owning client.
                if event in ("connection_state", "reconnecting", "reconnected",
                             "reconnect_failed", "heartbeat", "mtu_changed"):
                    addr = str(params.get("address", "")).lower()
                    route = self._state_routes.get(addr)
                    if route:
                        try:
                            route(msg)
                        except Exception:
                            pass
                if self._event_handler:
                    try:
                        self._event_handler(msg)
                    except Exception:
                        pass

    async def call(self, cmd: str, params: Optional[dict] = None,
                   rpc_timeout: float = 15.0) -> dict:
        """Send a JSON-RPC command. `params` is the remote params object;
        `rpc_timeout` is the local wait timeout (kept separate so a command
        parameter named 'timeout' cannot shadow it)."""
        if not self._connected:
            raise BleBridgeError("not_connected", "Call connect() first")
        req_id = next(self._ids)
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending[req_id] = fut
        payload: dict[str, Any] = {"id": req_id, "cmd": cmd}
        if params:
            payload["params"] = params
        try:
            await asyncio.to_thread(self._transport.send, payload)
            return await asyncio.wait_for(fut, timeout=rpc_timeout)
        finally:
            self._pending.pop(req_id, None)

    # ----- convenience API -----

    async def get_state(self) -> dict:
        return await self.call("get_state")

    async def list_connections(self) -> list[dict]:
        """List all BLE connections held by the bridge."""
        result = await self.call("list_connections")
        return result.get("connections", [])

    async def get_reconnect_config(self) -> dict:
        """Get global auto-reconnect / heartbeat configuration."""
        return await self.call("get_reconnect_config")

    async def set_reconnect_config(
        self,
        auto_reconnect: bool,
        max_attempts: Optional[int] = None,
        delay_ms: Optional[int] = None,
        heartbeat_interval_ms: Optional[int] = None,
        heartbeat_fail_threshold: Optional[int] = None,
    ) -> dict:
        """
        Configure auto-reconnect and heartbeat globally.

        Args:
            auto_reconnect: enable automatic reconnect on unexpected drops.
            max_attempts: max reconnect attempts (0 = never).
            delay_ms: base reconnect delay (exponential backoff applied).
            heartbeat_interval_ms: RSSI heartbeat period; 0 disables heartbeat.
            heartbeat_fail_threshold: consecutive heartbeat failures before
                                      treating the link as dead.
        Only provided (non-None) tuning values are changed.
        """
        params: dict[str, Any] = {"autoReconnect": auto_reconnect}
        if max_attempts is not None:
            params["maxAttempts"] = max_attempts
        if delay_ms is not None:
            params["delayMs"] = delay_ms
        if heartbeat_interval_ms is not None:
            params["heartbeatIntervalMs"] = heartbeat_interval_ms
        if heartbeat_fail_threshold is not None:
            params["heartbeatFailThreshold"] = heartbeat_fail_threshold
        return await self.call("set_reconnect_config", params=params)

    async def set_auto_reconnect(self, address: str, enabled: bool) -> dict:
        """Enable/disable auto-reconnect for one connection."""
        return await self.call("set_auto_reconnect",
                               params={"address": address, "enabled": enabled})

    async def request_permissions(self) -> bool:
        # This command replies asynchronously with the same id.
        return (await self.call("request_permissions", rpc_timeout=120.0)).get("granted", False)

    async def scan_start(self, timeout_ms: int = 10000, service_uuids: Optional[list[str]] = None):
        return await self.call("scan.start",
                               params={"timeout": timeout_ms, "service_uuids": service_uuids or []},
                               rpc_timeout=timeout_ms / 1000 + 2)

    async def scan_stop(self):
        return await self.call("scan.stop")

    async def connect_device(self, address: str, timeout_ms: int = 10000):
        return await self.call("connect",
                               params={"address": address, "timeout": timeout_ms},
                               rpc_timeout=timeout_ms / 1000 + 2)

    async def disconnect(self, address: Optional[str] = None):
        params = {"address": address} if address else None
        return await self.call("disconnect", params=params)

    async def disconnect_all(self):
        return await self.call("disconnect_all")

    async def discover(self, address: Optional[str] = None,
                       timeout_ms: int = 10000) -> list[GattService]:
        params: dict[str, Any] = {"timeout": timeout_ms}
        if address:
            params["address"] = address
        result = await self.call("discover", params=params,
                                 rpc_timeout=timeout_ms / 1000 + 2)
        services = []
        for svc in result.get("services", []):
            chars = [GattCharacteristic(
                uuid=c["uuid"],
                properties=c.get("properties", 0),
                descriptors=c.get("descriptors", []),
            ) for c in svc.get("characteristics", [])]
            services.append(GattService(uuid=svc["uuid"], characteristics=chars))
        return services

    async def read(self, uuid: str, address: Optional[str] = None,
                   timeout_ms: int = 10000) -> bytes:
        params: dict[str, Any] = {"uuid": uuid, "timeout": timeout_ms}
        if address:
            params["address"] = address
        result = await self.call("read", params=params,
                                 rpc_timeout=timeout_ms / 1000 + 2)
        return bytes.fromhex(result.get("value", ""))

    async def write(self, uuid: str, value: bytes, response: bool = False,
                    address: Optional[str] = None, timeout_ms: int = 10000):
        params: dict[str, Any] = {"uuid": uuid, "value": value.hex(),
                                  "response": response, "timeout": timeout_ms}
        if address:
            params["address"] = address
        return await self.call("write", params=params,
                               rpc_timeout=timeout_ms / 1000 + 2)

    async def subscribe(self, uuid: str, address: Optional[str] = None,
                        timeout_ms: int = 10000):
        params: dict[str, Any] = {"uuid": uuid, "timeout": timeout_ms}
        if address:
            params["address"] = address
        return await self.call("subscribe", params=params,
                               rpc_timeout=timeout_ms / 1000 + 2)

    async def unsubscribe(self, uuid: str, address: Optional[str] = None,
                          timeout_ms: int = 10000):
        params: dict[str, Any] = {"uuid": uuid, "timeout": timeout_ms}
        if address:
            params["address"] = address
        return await self.call("unsubscribe", params=params,
                               rpc_timeout=timeout_ms / 1000 + 2)

    async def request_mtu(self, mtu: int = 517, address: Optional[str] = None,
                          timeout_ms: int = 10000):
        params: dict[str, Any] = {"mtu": mtu, "timeout": timeout_ms}
        if address:
            params["address"] = address
        return await self.call("request_mtu", params=params,
                               rpc_timeout=timeout_ms / 1000 + 2)


class BleScanner:
    """bleak-like scanner interface."""

    def __init__(self, bridge: Optional[BleBridge] = None):
        self._own_bridge = bridge is None
        self._bridge = bridge or BleBridge()
        self._ads: list[Advertisement] = []
        self._done = asyncio.Event()

    async def __aenter__(self):
        await self._bridge.connect()
        self._bridge._event_handler = self._on_event
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._bridge._event_handler = None
        if self._own_bridge:
            await self._bridge.close()

    def _on_event(self, msg: dict):
        event = msg.get("event")
        if event == "advertisement":
            p = msg.get("params", {})
            self._ads.append(Advertisement(
                address=p.get("address", ""),
                name=p.get("name"),
                rssi=p.get("rssi", 0),
                data=p.get("data", ""),
            ))

    async def discover(self, timeout: float = 5.0,
                       service_uuids: Optional[list[str]] = None) -> list[Advertisement]:
        self._ads.clear()
        await self._bridge.scan_start(timeout_ms=int(timeout * 1000),
                                      service_uuids=service_uuids)
        await asyncio.sleep(timeout)
        await self._bridge.scan_stop()
        # de-duplicate by address, keeping the latest advertisement
        seen: dict[str, Advertisement] = {}
        for ad in self._ads:
            seen[ad.address] = ad
        return list(seen.values())


class BleClient:
    """
    bleak-like GATT client bound to one device address, with async context manager.

    Multiple BleClient instances can share a single BleBridge to talk to several
    devices concurrently:

        bridge = BleBridge()
        await bridge.connect()
        async with BleClient(ADDR_A, bridge=bridge) as a, \\
                   BleClient(ADDR_B, bridge=bridge) as b:
            ...
        await bridge.close()
    """

    def __init__(self, address: str, bridge: Optional[BleBridge] = None,
                 notification_callback: Optional[Callable[[str, str, bytes], None]] = None,
                 state_callback: Optional[Callable[[str, dict], None]] = None):
        self.address = address
        self._own_bridge = bridge is None
        self._bridge = bridge or BleBridge()
        # callback signature: (address, uuid, value)
        self._notify_cb = notification_callback
        # state callback signature: (event_name, full_event_message)
        self._state_cb = state_callback
        self._notify_slots: dict[str, Callable[[bytes], None]] = {}

    async def __aenter__(self):
        if self._own_bridge:
            await self._bridge.connect()
        # Route notifications and lifecycle events by address to this client.
        addr = self.address.lower()
        self._bridge._notify_routes[addr] = self._route_notify
        self._bridge._state_routes[addr] = self._route_state
        state = await self._bridge.get_state()
        if not state.get("permissionGranted", False):
            granted = await self._bridge.request_permissions()
            if not granted:
                raise BleBridgeError("permission_denied", "BLE permissions not granted")
        await self._bridge.connect_device(self.address)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        addr = self.address.lower()
        self._bridge._notify_routes.pop(addr, None)
        self._bridge._state_routes.pop(addr, None)
        try:
            await self._bridge.disconnect(self.address)
        finally:
            if self._own_bridge:
                await self._bridge.close()

    def _route_state(self, msg: dict):
        if self._state_cb:
            try:
                self._state_cb(msg.get("event", ""), msg)
            except Exception:
                pass

    def _route_notify(self, params: dict):
        uuid = params.get("uuid", "")
        value = bytes.fromhex(params.get("value", ""))
        if uuid in self._notify_slots:
            try:
                self._notify_slots[uuid](value)
            except Exception:
                pass
        if self._notify_cb:
            try:
                self._notify_cb(self.address, uuid, value)
            except Exception:
                pass

    async def get_services(self) -> list[GattService]:
        return await self._bridge.discover(self.address)

    async def read_gatt_char(self, uuid: str) -> bytes:
        return await self._bridge.read(uuid, address=self.address)

    async def write_gatt_char(self, uuid: str, value: bytes, response: bool = False):
        return await self._bridge.write(uuid, value, response=response, address=self.address)

    async def start_notify(self, uuid: str):
        return await self._bridge.subscribe(uuid, address=self.address)

    async def stop_notify(self, uuid: str):
        return await self._bridge.unsubscribe(uuid, address=self.address)

    async def request_mtu(self, mtu: int = 517):
        return await self._bridge.request_mtu(mtu, address=self.address)

    async def set_auto_reconnect(self, enabled: bool):
        """Enable/disable auto-reconnect for this connection."""
        return await self._bridge.set_auto_reconnect(self.address, enabled)

    @property
    def is_connected(self) -> bool:
        return True  # connection lifecycle is managed by the context manager

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Optional


class RendezvousError(RuntimeError):
    """erro no protocolo de comunicação como servidor rdv"""

class RendezvousConnection:

    MAX_LINE_BYTES = 32768  # limite definido no protocolo

    def __init__(self, host: str, port: int,
                 connect_timeout: float = 3.0, io_timeout: float = 5.0,
                 logger: Optional[logging.Logger] = None):
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.io_timeout = io_timeout
        self.logger = logger or logging.getLogger(__name__)

    
    async def register(self, namespace: str, name: str, port: int,
                        ttl: int = 7200) -> Dict[str, Any]:
        payload = {
            "type": "REGISTER",
            "namespace": namespace,
            "name": name,
            "port": int(port),
            "ttl": int(ttl),
        }
        self.logger.info(f"[RDV] REGISTER {name}@{namespace}:{port} (ttl={ttl})")
        return await self._request_ok(payload, "REGISTER")

    async def refresh(self, namespace: str, name: str, port: int, ttl: int) -> Dict[str, Any]:
        return await self.register(namespace=namespace, name=name, port=port, ttl=ttl)

    async def discover(self, namespace: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"type": "DISCOVER"}
        if namespace:
            payload["namespace"] = namespace

        self.logger.debug(f"[RDV] DISCOVER namespace={namespace}")
        resp = await self._request_ok(payload, "DISCOVER")

        peers = resp.get("peers", [])
        if not isinstance(peers, list):
            raise RendezvousError(f"DISCOVER retornou 'peers' que não é uma lista: {peers!r}")
        resp["peers"] = [p for p in peers if isinstance(p, dict)]

        self.logger.debug(f"[RDV] DISCOVER -> {len(resp['peers'])} peer(s)")
        return resp

    async def unregister(self, namespace: str, name: Optional[str] = None,
                          port: Optional[int] = None) -> Dict[str, Any]:
        """Remove nosso registro do Rendezvous (chamado ao encerrar a aplicação)."""
        payload: Dict[str, Any] = {"type": "UNREGISTER", "namespace": namespace}
        if name is not None:
            payload["name"] = name
        if port is not None:
            payload["port"] = int(port)

        self.logger.info(f"[RDV] UNREGISTER {name}@{namespace}:{port}")
        return await self._request_ok(payload, "UNREGISTER")


    async def _request_ok(self, payload: Dict[str, Any], op_name: str) -> Dict[str, Any]:
        resp = await self._request(payload)
        if resp.get("status") != "OK":
            raise RendezvousError(f"{op_name} falhou: {resp.get('message', resp)}")
        return resp

    async def _request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        line = json.dumps(payload, separators=(",", ":")) + "\n"

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.connect_timeout,
            )
        except Exception as e:
            raise ConnectionError(f"Não foi possível conectar ao Rendezvous {self.host}:{self.port}: {e}") from e

        try:
            writer.write(line.encode("utf-8"))
            await asyncio.wait_for(writer.drain(), timeout=self.io_timeout)

            raw = await asyncio.wait_for(reader.readline(), timeout=self.io_timeout)
            if not raw:
                raise RendezvousError("Rendezvous fechou a conexão sem responder nada (EOF)")

            text = raw.decode("utf-8", errors="replace").strip()
            try:
                resp = json.loads(text)
            except json.JSONDecodeError as e:
                raise RendezvousError(f"Resposta do Rendezvous não é JSON válido: {text!r}") from e

            if not isinstance(resp, dict):
                raise RendezvousError(f"Resposta do Rendezvous deveria ser um objeto JSON: {resp!r}")

            return resp
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            
import json
import asyncio

class RendevousConnection:
    def __init__(
        self,
        host: str,
        port: int,
        logger: Optional[logging.Logger] = None,
        *,
        encoding: str = "utf-8",
    ):
    
        self.host = host
        self.port = property
        self.logger = logger
        self._reader = None,
        self._writer = None,

        self._refresh = {}

        self.ttl = 7200,

    async def _connect(self):
        self self._reader, self._writer = await asyncio.open_connection(self.host, self.port)

        return True

    async def close(self):
        self._writer.close()
        await self._writer.wait_closed()

        return True

    async def _register(self, group: Optional[str] = None, name: Optional[str] = None):
        hello_msg = {
            "type": "REGISTER",
            "namespace": group,
            "name": name,
            "port": port,
            ttl: 500
        };

        await self._request(json.dumps(bye_msg))
    
    async def _refresh(self, group: Optional[str] = None, name: Optional[str] = None):
        refresh_msg = {
            "type": "REFRESH",
            "namespace": group,
            "name": name,
            "port": port,
            "ttl": ttl
        }

    resp = await self._request(json.dumps(refresh_msg))

    if resp is not None:
        async def loop():
            while True:
                await asyncio.sleep(ttl * 0.8)
                await self._refresh(group, name, port, ttl)  # _refresh, não refresh

        self._refresh_tasks[group] = asyncio.create_task(loop())


    async def _discover(self, group: Optional[str] = None):

        disc_msg = {
            "type": "DISCOVER",
            "namespace": group
        };

        users = await self._request(json.dumps(disc_msg))
        if users is not None:
            users = json.loads(raw.decode().strip())
            return users["peers"]

        return None

    async def unregister(group: Optional[str] = None):

        bye_msg = {
            "type": "UNREGISTER",
            "namespace": group,
            "name": name,
            "port": port
        };

        await self._request(json.dumps(bye_msg))

    async def _request(to_send):

        self._writer.write(to_send)
        await self._writer.drain()

        await self._ensure_ok()


    async def _ensure_ok():
        response = await self._reader.readline()

        if response["status"] != "OK":
            #tratar erro
            return None

        return response

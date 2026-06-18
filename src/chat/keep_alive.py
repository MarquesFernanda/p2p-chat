import time
import asyncio
import uuid
import logging
from datetime import datetime, timezone

class KeepAlive:

    def __init__(self, peer_server, interval_s: int, logger: None):
        self.peer_server = peer_server
        self.interval_s = interval_s 
        self.logger = logger or logging.getLogger(__name__)

        self._task = None 
        self._running = False
        self.pending_pings: dict[str, float] = {} 
        self._ping_events = {}
        self.mean_rtt = {}

    async def start(self):
        self._running = True
        self.logger.info(f"[KeepAlive] Starting Loop (interval={self.interval_s}s)")

        self._task = asyncio.create_task(self._pingpong_loop())

    async def stop(self):
        self._running = False #garante que haja um shutdown do loop mesmo que o task.cancel() demore um pouco para realmente ser cancelada
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError: # necessário para ignorar erro que surge em .cancel()
                pass
        self.logger.info(f"[KeepAlive] Quitted (interval={self.interval_s}s)")

    async def _pingpong_loop(self):
        while self._running:
            try:
                await self.send_ping()

            except Exception as error:
                self.logger.error(f"[KeepAlive] Error in loop: {error}")

            await asyncio.sleep(self.interval_s)

    
    async def send_ping(self):
        for peer_id, connection in list(self.peer_server.items()): #TROCAR QUANDO IMPLEMENTADO
            msg_id = str(uuid.uuid4())
            timestamp = (                    #define formato do tempo inserido em timestamp
                datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )

            msg = {
                "type": "PING",
                "msg_id": msg_id, #"uuid"
                "timestamp": timestamp, #usar time()
                "ttl": 1 #número de saltos de roteador
            }

            try:
                start_time = time.monotonic()
                self.pending_pings[msg_id] = start_time
                await self.peer_server._send_json(connection.writer, msg) #TROCAR QUANDO IMPLEMENTADO

                self.logger.info(f"[KeepAlive] Ping sent to peer {peer_id}")

                asyncio.create_task(self.manage_pong(msg_id, peer_id))

            except Exception as error:
                self.logger.warning(f"[KeepAlive] Error in sending PING to peer {peer_id}. Error: {error}")  

    async def wake_pong(self, msg_id: str): #deve ser implementado para quando for recebido um pong, de forma a acordar o manage_pong
        event = self._ping_events.get(msg_id)
        if event:
            event.set() # Acorda o manage_pong que está esperando [1]
            self.logger.debug(f"[KeepAlive] Event set for msg_id {msg_id}")
   
    async def manage_pong(self, msg_id, peer_id):
    #função responsável por manage quais pongs fram recebidos em um determinado período de tempo
        event = asyncio.Event()
        self._ping_events[msg_id] = event
        start_time = self.pending_pings.get(msg_id)

        try:
            await asyncio.wait_for(event.wait(), timeout=10.0) #fica "de olho" no evento esperando o sinal por até x segundos
        
            rtt = (time.monotonic() - start_time) * 1000 #RTT DEVE SER ATUALIZADO NA TABELA GLOBAL PEERTABLE
            self.logger.debug(f"[KeepAlive] PONG received from {peer_id} | RTT: {rtt:.1f}ms")

            if (peer_id not in self.mean_rtt) or (self.mean_rtt[peer_id] == 'STALE'):
                self.mean_rtt[peer_id] = rtt
            else:
                avgrtt = (self.mean_rtt[peer_id] + rtt)/2
                self.mean_rtt[peer_id] = avgrtt
        
        except asyncio.TimeoutError:
            self.mean_rtt[peer_id] = 'STALE'
            self.logger.warning(f"[KeepAlive] PING timeout for {peer_id} (ID: {msg_id})")

        finally:
        # Limpa o registro para não ocupar memória
            self._ping_events.pop(msg_id, None)
            self.pending_pings.pop(msg_id, None)

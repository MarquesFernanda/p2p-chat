import asyncio
import logging
import uuid


class MessageRouter:
    def __init__(self, state, peer_server, logger: logging.Logger, ttl: int=1):
        self.state = state
        self.peer_server = peer_server
        self.logger = logger
        self.ttl = ttl
        self.ack_events: dict[str, asyncio.Event] = {}

    async def start(self):
        self.logger.info(f"[Router] iniciando com TTL={self.ttl}")
        await asyncio.sleep(1)

    async def stop(self):
        self.logger.info(f"[Router] parando")
        await asyncio.sleep(1)

    async def trigger_reconcile(self):
        app = getattr(self, "chat_app", None)
        if app and hasattr(app, "reconcile_after_discover"):
            await app.reconcile_after_discover()
            self.logger.info("[Router] Reconexão com peer")
        else:
            self.logger.warning("[Router] Referencial do chat não disponível")

    async def send(self, peer_id: str = None, namespace: str = None, message: str = None, mode: str = None, require_ack: bool = True):

        msg_id = str(uuid.uuid4())

        if peer_id is not None:
            dst = peer_id
            opt = 0
        elif namespace is not None:
            dst = namespace
            opt = 1
        else:
            dst = "*"
            opt = 2

        msg = {
            "type": mode,
            "msg_id": msg_id,
            "src": self.state.peer_id(),
            "dst": dst,
            "payload": message,
            "require_ack": require_ack,
            "ttl": self.ttl
        }

        match opt:
            case 0:
                conn = self.peer_server.connnections.get(peer_id)

                if not conn:
                    self.logger.warning(f"[Router] Sem conexão com o peer {peer_id}")
                    return

                await self.peer_server.send_json(conn.writer, msg)
                self.logger.info(f"[Router] Envio {peer_id}: {message}")

            case 1, 2:
                sent = 0
                failed = 0

                for peer_id, conn in list(self.peer_server.connnections.items()):
                    writer = conn.writer

                    if writer.is_closing():
                        self.logger.warning(f"[Router] Pulando conexão fechada: {peer_id}")
                        failed += 1

                    if opt == 1:
                        if conn.namespace == namespace:
                            sent += 1
                            await self.peer_server.send_json(writer, msg)

                        self.logger.info(f"[Router] Publicado na sala #{namespace}, enviados: {sent}, falhos: {failed}\nMensagem: {message}")


        if require_ack:
            try:
                await asyncio.wait_for(self.ack_response(msg_id, peer_id), timeout= 5.0)
                self.logger.debug(f"[Router] ACK recebido para {msg_id}")
            except asyncio.TimeoutError:
                self.logger.debug(f"[Router] Timeout de ACK para {msg_id}")

    async def ack_response(self, msg_id: str, peer_id: str):
        event = asyncio.Event()
        self.ack_events[msg_id] = event

        try:
            await event.wait()
        finally:
            self.ack_events.pop(msg_id, None)
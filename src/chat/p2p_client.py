
import asyncio

import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import List

from message_router import MessageRouter
from cli import CLI
from keep_alive import KeepAlive
from peer_connection import PeerServer
from peer_table import PeerRecord, PeerTable
from rendezvous_connection import RendezvousConnection
from state import State


@dataclass
class ConfiguracoesJson:
    app_name: str
    rdv_host: str
    rdv_port: int
    name: str
    namespace: str
    listen_host: str
    listen_port: int
    discover_interval: int
    keepalive_interval: int
    rdv_ttl: int
    fixed_msg_ttl: int
    log_level: str
    max_reconnect_attempts: int 
    autonomous_mode: bool = False
    features: List[str] = field(default_factory=list)


class p2pChatApp:

    def __init__(self, config: ConfiguracoesJson, logger: logging.Logger):
        self.config = config
        self.logger = logger

        self.state = State(logger=self.logger)
        self.state.set_identity(name=config.name, namespace=config.namespace)

        self.peer_table = PeerTable(
            my_name=self.config.name,
            my_namespace=self.config.namespace,
            max_attempts=self.config.max_reconnect_attemps,
            logger=self.logger
        )

        self.peer_server = PeerServer(
            host=self.config.listen_host,
            port=self.config.listen_port,
            state=self.state,
            logger=self.logger,
            peer_table=self.peer_table,
            features=self.config.features,
            autonomous_mode=self.config.autonomous_mode
        )

        self.keep_alive = KeepAlive(
            peer_server=self.peer_server,
            interval_s=self.config.keepalive_interval,
            logger=self.logger,
            peer_table=self.peer_table
        )

        #atribuição tardia necessária
        self.peer_server.keep_alive = self.keep_alive

        self.router = MessageRouter(
            state=self.state,
            peer_server=self.peer_server,
            logger=self.logger,
            ttl=1
        )

        #atribuição tardia necessária
        self.peer_server.router = self.router

        self.rendezvous = RendezvousConnection(
            host=self.config.rdv_host,
            port=self.config.rdv_port,
            connect_timeout=3.0,
            io_timeout=5.0,
            logger=self.logger
        )

        self.cli = CLI(
            peer_table=self.peer_table,
            router=self.router,
            logger=self.logger
        )

        # Atribuições tardias necessárias
        self.peer_server.router = self.router
        self.peer_server.keep_alive = self.keep_alive
        self.router.chat_app = self

        # controle de tasks asyncio
        self._tasks: list[asyncio.Task] = []
        self._running = False

    async def start(self) -> None:

        self._running = True
        self.logger.info(
            f"[P2P] Iniciando como {self.state.peer_id()} "
            f"em {self.config.listen_host}:{self.config.listen_port}"
        )

        await self.peer_server.start()
        await self._register_rdv()
        await self._discover_and_update()
        await self._reconnect_peers()
        await self.keep_alive.start()
        await self.router.start()

        self.logger.info("Aplicação iniciada com sucesso.")

    
    async def run(self) -> int:

        await self.start()

        discover_task = asyncio.create_task(
            self._discover_loop(), name="discover_loop"
        )
        reconnect_task = asyncio.create_task(
            self._reconnect_loop(), name="reconnect_loop"
        )
        rdv_refresh_task = asyncio.create_task(
            self._rdv_refresh_loop(), name="rdv_refresh_loop"
        )

        self._tasks = [discover_task, reconnect_task, rdv_refresh_task]

        try:
            await self.cli.run()
        finally:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

            await self.shutdown()

        return 0

    async def shutdown(self) -> None:

        self.logger.info("Iniciando shutdown...")
        self._running = False

        await self.keep_alive.stop()
        await self.router.stop()
        await self.peer_server.stop(timeout=2.0)
        await self._unregister_rdv()

        self.logger.info("shutdown concluído.")

    async def _register_rdv(self) -> None:
        try:
            resp = await self.rendezvous.register(
                namespace=self.config.namespace,
                name=self.config.name,
                port=self.config.listen_port,
                ttl=self.config.rdv_ttl,
            )
            self.logger.info(
                f"registrado no Rendezvous como "
                f"{self.config.name}@{self.config.namespace} "
                f"(IP externo: {resp.get('ip')}, TTL: {resp.get('ttl')}s)"
            )
        except Exception as e:
            self.logger.error(f"Falha ao registrar no rendezvous: {e}")

    async def _unregister_rdv(self) -> None:
        try:
            await self.rendezvous.unregister(
                namespace=self.config.namespace,
                name=self.config.name,
                port=self.config.listen_port,
            )
            self.logger.info("Registro removido do rendezvous.")
        except Exception as e:
            self.logger.warning(f"Falha no unregister: {e}")

    async def _discover_and_update(self) -> None:

        try:
            resp = await self.rendezvous.discover(namespace=self.config.namespace)
            peers = resp.get("peers", [])

            self.state.update_bulk(peers)

            vistos_agora: set[str] = set()

            for peer_data in peers:
                name = peer_data.get("name")
                namespace = peer_data.get("namespace")
                ip = peer_data.get("ip")
                port = peer_data.get("port")
                ttl = peer_data.get("ttl")

                if not all([name, namespace, ip, port]):
                    continue

                peer_id = PeerTable.build_peer_id(name, namespace)
                vistos_agora.add(peer_id)

                self.peer_table.upsert_from_rdv(
                    name=name,
                    namespace=namespace,
                    ip=ip,
                    port=port,
                    rdv_ttl_sec=ttl,
                )

            self.peer_table.mark_stale_if_missing(vistos_agora)

            self.logger.debug(
                f"DISCOVER: {len(peers)} peers no namespace '{self.config.namespace}'"
            )

        except Exception as e:
            self.logger.warning(f"Erro no discover: {e}")

    async def _reconnect_peers(self) -> None:

        candidatos = self.peer_table.peers_needing_reconnect()

        if not candidatos:
            return

        self.logger.debug(f"Tentando reconectar {len(candidatos)} peers")

        tarefas = [self._connect_one_peer(rec) for rec in candidatos]
        await asyncio.gather(*tarefas, return_exceptions=True)

    async def _connect_one_peer(self, rec: PeerRecord) -> None:

        peer_id = rec.peer_id

        self.peer_table.register_reconnect_attempt(peer_id)

        success = await self.peer_server.request_connection(
            remote_peer_id=peer_id,
            ip=rec.ip,
            port=rec.port,
            timeout=5.0,
        )

        if not success:
            self.peer_table.mark_reconnect_failed(
                peer_id, error="Falha no handshake TCP"
            )
            self.logger.debug(
                f"Conexão com {peer_id} falhou. "
                f"Tentativas: {rec.attempts}/{self.peer_table.max_attempts}"
            )
        else:
            self.logger.info(f"Conectado a {peer_id}")

    async def _discover_loop(self) -> None:
        self.logger.info(
            f"Discover loop iniciado (intervalo={self.config.discover_interval}s)"
        )
        while self._running:
            try:
                await asyncio.sleep(self.config.discover_interval)

                if not self._running:
                    break

                await self._discover_and_update()
                await self._reconnect_peers()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Erro no discover loop: {e}")

        self.logger.debug("Discover loop encerrado.")

    async def _reconnect_loop(self) -> None:

        while self._running:
            try:
                await asyncio.sleep(10)

                if not self._running:
                    break

                await self._reconnect_peers()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Erro no reconnect loop: {e}")

        self.logger.debug("Reconnect loop encerrado.")

    async def _rdv_refresh_loop(self) -> None:
        refresh_interval = max(60, self.config.rdv_ttl // 2)
        self.logger.info(
            f"RDV refresh loop iniciado (intervalo={refresh_interval}s)"
        )
        while self._running:
            try:
                await asyncio.sleep(refresh_interval)

                if not self._running:
                    break

                await self._register_rdv()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Erro no rdv refresh loop: {e}")

        self.logger.debug("RDV refresh loop encerrado.")

    async def reconcile_after_discover(self) -> None:
        self.logger.info("Reconciliação forçada pelo Router")
        await self._discover_and_update()
        await self._reconnect_peers()
        
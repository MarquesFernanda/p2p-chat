import time
import asyncio
import uuid
import logging
from datetime import datetime, timezone
from dataclasses import dataclass

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
    features: list[str] = []
    autonomous_mode: bool
    max_reconnect_attemps: int

class p2pChatApp:

    def __init__(self, config: ConfiguracoesJson, logger: logging.Logger):
        self.config = config
        self.logger = logger

        #Instanciações de objetos
        self.state = State(logger=self.logger)
        self.state.set_identity(name= config.name, namespace= config.namespace)

        self.peer_table = PeerTable(
            my_name= self.config.name,
            my_namespace= self.config.namespace,
            max_attempts= self.config.max_reconnect_attemps, 
            logger= self.logger
        )

        self.peer_server = PeerServer(
            host= self.config.listen_host,
            port= self.config.listen_port, 
            state= self.state, 
            logger= self.logger, 
            peer_table= self.peer_table, 
            features= self.config.features, 
            autonomous_mode= self.config.autonomous_mode
        )

        self.keep_alive = KeepAlive(
            peer_server= self.peer_server,
            interval_s= self.config.keepalive_interval, 
            logger= self.logger, 
            peer_table= self.peer_table
        )

        #atribuição tardia necessária
        self.peer_server.keep_alive = self.keep_alive

        self.router = MessageRouter(
            state= self.state, 
            peer_server= self.peer_server, 
            logger= self.logger, 
            ttl= 1
        )

        #atribuição tardia necessária
        self.peer_server.router = self.router

        self.rendezvous = RendezvousConnection(
            host= self.config.rdv_host,
            port= self.config.rdv_port,
            connect_timeout= 3.0,
            io_timeout= 5.0,
            logger= self.logger
        )

        self.cli = CLI(
            peer_server= self.peer_server,
            peer_table= self.peer_table, 
            router= self.router, 
            logger= self.logger
        )


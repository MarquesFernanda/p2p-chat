from __future__ import annotations
 
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class ConnectionState(Enum):
    DISCONNECTED = "DISCONNECTED" #estado inicial
    DIALING = "DIALING" #tentando conectar agora
    CONNECTED = "CONNECTED" #conexão tcp realizada com sucesso
    FAILED = "FAILED" #tentativa de reconexão falhou (max_attempts)
    COOLDOWN = "COOLDOWN" #espera (backoff exponencial)
    STALE = "STALE" #nao ta respondendo o ping/sumiu do rdv


@dataclass
#todas as informações de um peer
class PeerRecord:
    peer_id: str          #name@namespace
    ip: str
    port: int
    namespace: str

    last_seen_rdv: float  # ultima vez q o peer apareceu no rdv
    rdv_ttl_sec: Optional[int] = None  # tempo de vida do peer no rdv

    connection_state: ConnectionState = ConnectionState.DISCONNECTED
    direction: Optional[str] = None  # inbound, outbound
    since_state: float = field(default_factory=time.monotonic)  #quando entrou nesse estado
 
    attempts: int = 0
    last_attempt_at: Optional[float] = None #ultima tentativa de conexao
    backoff_until: Optional[float] = None #qnd posso tentar conectar de novo 
    last_error: Optional[str] = None #erro que a onteceu na ultima tentativa
    rtt_ms: Optional[float] = None  # do keepalive


    def __repr__(self) -> str:
        rtt = f"{self.rtt_ms:.1f}ms" if self.rtt_ms is not None else "?"
        return f"<Peer {self.peer_id} {self.ip}:{self.port} state={self.connection_state.name} rtt={rtt}>"
 


#agenda com todos os peers
class PeerTable:

    def __init__(self, my_name: str, my_namespace: str, max_attempts: int, logger: logging.Logger):
        self.my_name = my_name
        self.my_namespace = my_namespace
        self.my_peer_id = self.build_peer_id(my_name, my_namespace)
        self._by_id: Dict[str, PeerRecord] = {}  
        self.logger = logger or logging.getLogger(__name__)
        self.max_attempts = max_attempts         #config.json (max_reconnect_attempts)

    #define o peer_id
    @staticmethod
    def build_peer_id(name: str, namespace: str) -> str:
        return f"{name}@{namespace}"
    


    def upsert_from_rdv(self, name: str, namespace: str, ip: str, port: int,
                         rdv_ttl_sec: Optional[int] = None) -> None:
        #quem ta montando o peer_id é a peer_table, chamar metodo build_peer_id
        peer_id = self.build_peer_id(name, namespace)

        if peer_id == self.my_peer_id:
            return
 
        now = time.monotonic()
        rec = self._by_id.get(peer_id)
 
        if rec is None:
            self._by_id[peer_id] = PeerRecord(
                peer_id=peer_id,
                ip=ip,
                port=port,
                namespace=namespace,
                last_seen_rdv=now,
                rdv_ttl_sec=rdv_ttl_sec,
            )
            return
 
        changed_address = (rec.ip != ip) or (rec.port != port)
 
        rec.ip = ip
        rec.port = port
        rec.namespace = namespace
        rec.last_seen_rdv = now
        rec.rdv_ttl_sec = rdv_ttl_sec

 
        if  rec.connection_state == ConnectionState.STALE:
            rec.connection_state = ConnectionState.DISCONNECTED
            rec.since_state = now
            rec.backoff_until = None
 
            if changed_address:
                rec.attempts = 0
                rec.last_error = None
                self.logger.debug(f"[PeerTable] {peer_id} reapareceu com novo IP/porta -> attempts resetados.")
            else:
                self.logger.debug(f"[PeerTable] {peer_id} reapareceu com mesmo IP/porta -> histórico mantido.")


        #correçao: estava em failed e reapareceu
        elif rec.connection_state == ConnectionState.FAILED:
            rec.connection_state = ConnectionState.DISCONNECTED
            rec.since_state = now
            rec.backoff_until = None
            rec.attempts = 0
            rec.last_error = None
            self.logger.info(f"[PeerTable] {peer_id} reapareceu no Rendezvous (estava FAILED) -> tentativas resetadas.")


    def mark_stale_if_missing(self, seen_now: set[str]) -> None:
        now = time.monotonic()
        for peer_id, rec in self._by_id.items():
            if peer_id not in seen_now:
                if rec.connection_state != ConnectionState.CONNECTED:
                    rec.connection_state = ConnectionState.STALE
                    rec.since_state = now
    
    def mark_stale(self, peer_id: str) -> None:
        rec = self._by_id.get(peer_id)
        if rec is None:
            return
        if rec.connection_state == ConnectionState.CONNECTED:
            rec.connection_state = ConnectionState.STALE
            rec.since_state = time.monotonic()
            rec.direction = None
            self.logger.warning(f"[PeerTable] {peer_id} -> STALE (timeout de PING)")
    

    def on_connection_established(self, peer_id: str, direction: str) -> None:
        rec = self._by_id.get(peer_id)
        if not rec:
            return
 
        now = time.monotonic()
 
        rec.connection_state = ConnectionState.CONNECTED
        rec.direction = direction
        rec.since_state = now
 
        rec.backoff_until = None
        rec.last_error = None
        rec.attempts = 0
    

    def update_rtt(self, peer_id: str, rtt_ms: float) -> None:
        rec = self._by_id.get(peer_id)
        if rec:
            rec.rtt_ms = rtt_ms
    


#consultas

    def all(self) -> List[PeerRecord]:
        return list(self._by_id.values())
 
    def get(self, peer_id: str) -> Optional[PeerRecord]:
        return self._by_id.get(peer_id)
 
    def get_by_name(self, name: str, namespace: str) -> Optional[PeerRecord]:
        return self._by_id.get(self.build_peer_id(name, namespace))
 
    def active(self) -> List[PeerRecord]:
        return [rec for rec in self._by_id.values() if rec.connection_state == ConnectionState.CONNECTED]
 
    def in_namespace(self, namespace: str) -> List[PeerRecord]:
        return [rec for rec in self._by_id.values() if rec.namespace == namespace]
 
    def remove(self, peer_id: str) -> None:
        self._by_id.pop(peer_id, None)
 
    def contains(self, peer_id: str) -> bool: #deixei público para usar em peer_connection
        return peer_id in self._by_id
 
    def __len__(self) -> int:
        return len(self._by_id)
    
    

    def on_connection_lost(self, peer_id: str, reason: Optional[str] = None) -> None:
        rec = self._by_id.get(peer_id)
        if rec:
            rec.connection_state = ConnectionState.DISCONNECTED
            rec.direction = None
            rec.since_state = time.monotonic()
            rec.last_error = reason
            self.logger.info(f"[PeerTable] {peer_id} -> DISCONNECTED (motivo: {reason})")

    def should_reconnect(self, peer_id: str) -> bool:

        rec = self._by_id.get(peer_id)
        if rec is None:
            return False
 
        if peer_id == self.my_peer_id:
            return False
 
        if rec.connection_state in (
            ConnectionState.CONNECTED,
            ConnectionState.DIALING,
            ConnectionState.STALE,
        ):
            return False
 
        if rec.connection_state == ConnectionState.COOLDOWN:
            if rec.backoff_until and time.monotonic() < rec.backoff_until:
                return False
 
        if rec.attempts >= self.max_attempts:
            return False
 
        return bool(rec.ip and rec.port)
    

    def peers_needing_reconnect(self) -> List[PeerRecord]:
        return [rec for rec in self._by_id.values() if self.should_reconnect(rec.peer_id)]
    


    def register_reconnect_attempt(self, peer_id: str) -> None:
        rec = self._by_id.get(peer_id)
        if rec is None:
            return
 
        rec.attempts += 1
        rec.connection_state = ConnectionState.DIALING
        rec.direction = "outbound"
        rec.since_state = time.monotonic()
        rec.last_attempt_at = time.monotonic()
 
        self.logger.info(
            f"[PeerTable] Tentativa {rec.attempts}/{self.max_attempts} de reconexão com {peer_id}"
        )
    


    def mark_reconnect_failed(self, peer_id: str, error: Optional[str] = None) -> None:
        rec = self._by_id.get(peer_id)
        if rec is None:
            return
 
        rec.last_error = error
 
        if rec.attempts >= self.max_attempts:
            rec.connection_state = ConnectionState.FAILED
            rec.since_state = time.monotonic()
            self.logger.warning(
                f"[PeerTable] {peer_id} -> FAILED (excedeu {self.max_attempts} "
                f"tentativas; último erro: {error})"
            )
            return
 
        base_delay = min(60, 2 ** min(rec.attempts, 5))
        jitter = random.uniform(0, 1.0)
        rec.backoff_until = time.monotonic() + base_delay + jitter
        rec.connection_state = ConnectionState.COOLDOWN
        rec.since_state = time.monotonic()
 
        self.logger.info(
            f"[PeerTable] {peer_id}: tentativa {rec.attempts}/{self.max_attempts} "
            f"falhou ({error}) -> cooldown de ~{base_delay:.1f}s"
        )
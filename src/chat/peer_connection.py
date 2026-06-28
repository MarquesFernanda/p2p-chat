from dataclasses import dataclass, field
import asyncio
import json
from typing import Dict, Optional, Set
from datetime import datetime, timezone
import uuid


from peer_table import PeerRecord, ConnectionState

MAX_MSG_SIZE = 32768 #tamanho máximo de msg em bytes

@dataclass
class ConnectionInfo: #stores connection data

    peer_id: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    direction: str
    features: Set[str] = field(default_factory=set)
    last_ping_ts: Optional[float] = None
    rtt_ms: Optional[float] = None

class PeerServer:

    def __init__(self, host, port, state, logger: None, peer_table, features, autonomous_mode: bool):
        self.host = host # endereço de ip do meu próprio computador, listen
        self.port = port
        self.state = state
        self.logger = logger
        self.peer_table = peer_table
        self.features = features
        self.autonomous_mode = autonomous_mode or False

        self.router = None #inicia assim e será conectado por self.peer_server.router = self.router 
        self.keep_alive = None 

        self._server = None # Guardará a instância do servidor asyncio
        self._running = False
        self.connections: Dict[str, ConnectionInfo] = {}
        self.my_id = self.state.peer_id()
        self._dialing: Set[str] = set()
        self._set_for_tasks: Set[asyncio.Task] = set() #útil para função stop()


    async def start(self):

        self.logger.info(f"[PeerServer] Escutando em: {self.host}:{self.port}")
        self._server = await asyncio.start_server(self.accept_new_peer, self.host, self.port)
        self._running = True


    async def stop(self, timeout: float):

        self._running = False
        #close server
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self.logger.info("[PeerServer] server fechado.")

        #cancela todos _handle_peer_msgs
        if hasattr(self, "_set_for_tasks"):
            for task in list(self._set_for_tasks):
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self.logger.info("[PeerServer] Tasks de mensagens canceladas.")

        #send bye to all connected peers
        for remote_peer_id, connection in list(self.connections.items()):
            try:
                msg_id = str(uuid.uuid4())
                bye_msg = {
                    "type": "BYE",
                    "msg_id": msg_id,
                    "src": self.my_id,
                    "dst": remote_peer_id,
                    "reason": "Encerrando aplicação",
                    "ttl": 1
                }
                await self.send_json(connection.writer, bye_msg)
            
                #se for outbound, tenta ler o BYE_OK com um timeout curto
                if connection.direction == "outbound":
                    try:
                        await asyncio.wait_for(connection.reader.readline(), timeout)
                    except asyncio.TimeoutError:
                        pass

            except Exception as e:
                self.logger.warning(f"Erro ao enviar BYE para {remote_peer_id}: {e}")

            #fechar todas conexões in connections
            try:
                connection.writer.close()
                await connection.writer.wait_closed()
            except Exception:
                pass

        self.connections.clear()
        self.logger.info("[PeerServer] Parado com sucesso.")


    async def accept_new_peer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter): #handles inbound connections 
        handshake_success = False
        remote_peer_id = None
        
        try:
            raw_data = await reader.readline() #Lê a linha bruta (em bytes)

            if not raw_data: #retorna caso conexão fechada abruptamente
                self.logger.warning(f"[PeerServer] Conexão fechada rapidamente demais para receber HELLO")
                writer.close()
                await writer.wait_closed()
                return 
            
            elif len(raw_data) > MAX_MSG_SIZE:
                self.logger.warning(f"[PeerServer] Mensagem grande demais para ser HELLO")
                writer.close()
                await writer.wait_closed()
                return 
            
            data_hello = json.loads(raw_data.decode("utf-8").strip()) #transforma mensagem em bits em dicionário json

            if data_hello.get('type') == 'HELLO':
                remote_peer_id = data_hello.get('peer_id')
                features = data_hello.get('features')

                self.logger.info(f"[PeerServer] HELLO recebido de {remote_peer_id}")

                hello_ok = {
                    "type": "HELLO_OK",
                    "peer_id": self.my_id, #deve ser enviado o meu peer_id, pegado de state
                    "version": "1.0",
                    "features": self.features or [],
                    "ttl": 1
                }

                await self.send_json(writer, hello_ok)

                if not self.peer_table.contains(remote_peer_id):  #verifica se peer está registrado na peertable, se não, o registra via upsert_rdv
                    self.logger.info(f"[PeerServer] {remote_peer_id} ainda não conhecido. Registrando via handshake...")
                    ip, port = writer.get_extra_info("peername")
                    name = remote_peer_id.split('@')[0]
                    namespace = remote_peer_id.split('@')[1]

                    self.peer_table.upsert_from_rdv(
                        name=name,
                        ip=ip,
                        port=port,
                        namespace=namespace
                        )

                self._register_connections(
                    remote_peer_id= remote_peer_id,
                    reader= reader,
                    writer= writer,
                    direction= 'inbound',
                    features=features
                    )

                self.peer_table.on_connection_established(remote_peer_id, "inbound") 

                task = asyncio.create_task(self._handle_peer_messages(remote_peer_id, reader, writer))
                self._set_for_tasks.add(task)
                task.add_done_callback(lambda task: self._set_for_tasks.discard(task))

                handshake_success = True
                self.logger.info(f"[PeerServer] Handshake concluído com {remote_peer_id}")


            else:
                self.logger.warning("[PeerServer] Handshake inválido: esperando por HELLO")
                writer.close()
                await writer.wait_closed()

        except Exception as e:
            self.logger.error(f"[PeerServer] Erro no processamento do handshake: {e}")
            writer.close() 
            await writer.wait_closed()  

        finally:
            if not handshake_success:
                self.logger.info(f"[PeerServer] Limpando tentativa de conexão falha")
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass


    async def request_connection(self, remote_peer_id: str, ip: str, port: int, timeout: float) -> bool:
        if (remote_peer_id in self.connections) or (remote_peer_id in self._dialing): #evita fazer duplas conexões com mesmo peer
            return True
        
        self._dialing.add(remote_peer_id)

        try:

            self.logger.info(f"[PeerServer] Tentando conectar a {remote_peer_id} em {ip}:{port}...")

            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, port), 
                timeout=timeout
            )

            hello_msg = {
                "type": "HELLO",
                "peer_id": self.my_id,
                "version": "1.0",
                "features": self.features or [],
                "ttl": 1
            } 

            await self.send_json(writer, hello_msg)
            raw_data = await asyncio.wait_for(reader.readline(), timeout) 

            if not raw_data: 
                self.logger.warning(f"[PeerServer] Conexão fechada rapidamente demais para receber HELLO_OK")
                return False
            
            elif len(raw_data) > MAX_MSG_SIZE:
                self.logger.warning(f"[PeerServer] Mensagem grande demais para ser HELLO_OK")
                return False

            msg = json.loads(raw_data.decode("utf-8").strip())

            if msg.get('type') == 'HELLO_OK':
                features = set(msg.get("features", []))
                self._register_connections(
                    remote_peer_id= remote_peer_id,
                    reader= reader,
                    writer= writer,
                    direction= 'outbound',
                    features=features
                    )
                
                self.peer_table.on_connection_established(remote_peer_id, "outbound") 
                
                task = asyncio.create_task(self._handle_peer_messages(remote_peer_id, reader, writer))
                self._set_for_tasks.add(task)
                task.add_done_callback(lambda task: self._set_for_tasks.discard(task))     

            else:
                raise RuntimeError(f"Handshake falhou: esperado HELLO_OK, recebido {msg.get('type')}")
            
            self.logger.info(f"[PeerServer] Conexão outbound realizada com {remote_peer_id}")
            return True

        except Exception as e:
            self.logger.warning(f"[PeerServer] Falha ao solicitar conexão com {remote_peer_id}: {e}")
            return False
        finally:
            self._dialing.remove(remote_peer_id)



    async def _handle_peer_messages(self, remote_peer_id : str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while self._running:
                raw_data = await reader.readline()

                if not raw_data: #retorna caso conexão fechada abruptamente
                    self.logger.warning(f"[PeerServer] Conexão fechada no meio de mensagem enviada")
                    break
                            
                elif len(raw_data) > MAX_MSG_SIZE:
                    self.logger.warning(f"[PeerServer] Mensagem grande demais")
                    break
                
                msg = json.loads(raw_data.decode("utf-8").strip()) 

                if msg.get('type') == 'PING':
                    msg_id = msg.get('msg_id')
                    timestamp = (              
                        datetime.now(timezone.utc)
                        .replace(microsecond=0)
                        .isoformat()
                        .replace("+00:00", "Z")
                    )

                    msg_pong = {
                        "type": "PONG",
                        "msg_id": msg_id,
                        "timestamp": timestamp,
                        "ttl": 1
                        }
                    
                    await self.send_json(writer, msg_pong)
                    self.logger.debug(f"[PeerServer] Enviando PONG para {remote_peer_id}")  
                            
                elif msg.get('type') == 'PONG':
                    msg_id = msg.get('msg_id')
                    connection = self.connections.get(remote_peer_id)
                    last_ping_ts = connection.last_ping_ts

                    keep_alive = getattr(self, "keep_alive", None) #segurança em atribuição tardia

                    if connection and last_ping_ts and keep_alive:
                        await self.keep_alive.wake_pong(msg_id) #executa todo o cálculo de rtt em keepalive

                elif msg.get('type') == 'SEND':
                    payload = msg.get('payload')
                    self.logger.info(f"[MSG] {remote_peer_id}: {payload}")

                    if msg.get("require_ack"):
                        timestamp = (              
                            datetime.now(timezone.utc)
                            .replace(microsecond=0)
                            .isoformat()
                            .replace("+00:00", "Z")
                            )   
                        
                        ack_msg = {
                            "type": "ACK",
                            "msg_id": msg.get("msg_id"), # DEVE ser o mesmo id da mensagem recebida [1]
                            "timestamp": timestamp,
                            "ttl": 1
                        }
                        await self.send_json(writer, ack_msg)
                        self.logger.debug(f"[PeerServer] ACK enviado para {remote_peer_id}")

                    if self.autonomous_mode:
                        auto_reply_msg = {
                           "type": "SEND",
                            "msg_id": str(uuid.uuid4()),
                            "src": self.my_id,
                            "dst": remote_peer_id,
                            "payload": f"{self.my_id} agradece pela mensagem: {payload}",
                            "require_ack": False,
                            "ttl": 1
                        }
                        await self.send_json(writer, auto_reply_msg)
                        self.logger.debug(f"[PeerServer] Resposta automática enviada para {remote_peer_id}")

                elif msg.get('type') == 'ACK':
                    msg_id = msg.get('msg_id')
                    router = getattr(self, "router", None)
                    
                    if router and msg_id in router.ack_events:
                        # 2. Sinaliza (set) o evento para "acordar" a tarefa que enviou a mensagem
                        router.ack_events[msg_id].set()
                        self.logger.debug(f"[PeerServer] ACK recebido de {remote_peer_id} para msg {msg_id}")
                    else:
                        # Caso o ACK chegue atrasado (após o timeout de 5s)
                        self.logger.warning(f"[PeerServer] ACK recebido para ID desconhecido ou expirado: {msg_id}")

                elif msg.get('type') == 'PUB':
                    payload = msg.get('payload')
                    dst = msg.get('dst')  # '*' (global) ou '#namespace'
                    self.logger.info(f"[PUB] {remote_peer_id}: {dst} {payload}")

                    if msg.get("require_ack"):
                        timestamp = (              
                            datetime.now(timezone.utc)
                            .replace(microsecond=0)
                            .isoformat()
                            .replace("+00:00", "Z")
                            )   
                        
                        ack_msg = {
                            "type": "ACK",
                            "msg_id": msg.get("msg_id"), 
                            "timestamp": timestamp,
                            "ttl": 1
                        }
                        await self.send_json(writer, ack_msg)
                        self.logger.debug(f"[PeerServer] ACK de PUB enviado em {dst} enviado para {remote_peer_id}")

                    if self.autonomous_mode:
                        auto_reply_msg = {
                           "type": "SEND",
                            "msg_id": str(uuid.uuid4()),
                            "src": self.my_id,
                            "dst": remote_peer_id,
                            "payload": f"{self.my_id} agradece pela mensagem: {payload}",
                            "require_ack": False,
                            "ttl": 1
                        }
                        await self.send_json(writer, auto_reply_msg)
                        self.logger.debug(f"[PeerServer] Resposta automática de PUB enviado em {dst} para {remote_peer_id}")

                elif msg.get('type') == 'BYE':
                    msg_id = msg.get('msg_id')
                    msg_reason = msg.get('reason')
                    
                    self.logger.info(f"[PeerServer] BYE recebido de {remote_peer_id}, motivo: {msg_reason}")

                    bye_ok = {
                        "type": "BYE_OK",
                        "msg_id": msg_id,
                        "src": self.my_id,
                        "dst": remote_peer_id,
                        "ttl": 1
                        }
                    
                    await self.send_json(writer, bye_ok)
                    break #executa bloco finally que fecha conexão

                else:
                    self.logger.warning(f"[PeerServer] Tipo de mensagem não reconhecida enviada por {remote_peer_id}")



        except Exception as e:
            self.logger.error(f"[PeerServer] Erro no recebimento de mensagens do peer {remote_peer_id}: {e}")

        finally:
            #encerrar conexões
            writer.close()
            await writer.wait_closed()

            self.connections.pop(remote_peer_id)

            self.peer_table.on_connection_lost(remote_peer_id, 'Encerramento do envio de mensagens')

            self.logger.info(f"[PeerServer] Conexão encerrada com {remote_peer_id}")


    async def send_json(self, writer : asyncio.StreamWriter, msg : dict):
        encoded_msg = (json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8") #transforma dicionário de msg em json file termiado em \n
        writer.write(encoded_msg)
        await writer.drain() #drain() garante que dados são enviados de acordo com a disponibilidade da rede


    def _register_connections(self, remote_peer_id: str, reader, writer, direction, features):
        nova_conexao = ConnectionInfo(
                    peer_id = remote_peer_id,
                    reader = reader,
                    writer = writer,
                    direction = direction,
                    features = features
                )  
        
        self.connections[remote_peer_id] = nova_conexao 
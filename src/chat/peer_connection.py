from dataclasses import dataclass, field
import asyncio
import json
from typing import Dict


from peer_table import PeerRecord, ConnectionState

MAX_MSG_SIZE = 32768 #tamanho máximo de msg em bytes

@dataclass
class ConnectionInfo: #stores connection data

    peer_id: str
    reader = asyncio.StreamReader
    writer = asyncio.StreamWriter

class PeerServer:

    def __init__(self, host, port, state, logger: None, peer_table):
        self.host = host # endereço de ip do meu próprio computador, listen
        self.port = port
        self.state = state
        self.logger = logger
        self.peer_table = peer_table
        self._server = None # Guardará a instância do servidor asyncio
        self._running = False

        self.connections: Dict[str, ConnectionInfo] = {}

    async def start(self):

        self.logger.info(f"[PeerServer] Escutando em: {self.host}:{self.port}")
        self._server = await asyncio.start_server(self.accept_new_reader, self.host, self.port)
        self._running = True
        
    async def accept_new_peer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter): #handles connections while a peer in conected
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
                peer_id = data_hello.get('peer_id')
                features = data_hello.get('features')

                self.logger.info(f"[PeerServer] HELLO recebido de {peer_id}")

                nova_conexao = ConnectionInfo(
                    peer_id = peer_id,
                    reader = reader,
                    writer = writer,
                    features = features,
                    # ... outros campos
                )   

                self.connections[peer_id] = nova_conexao    

                #aqui deve ficar a função de manda hello ok devolta para esse peer

            else:
                self.logger.warning("[PeerServer] Handshake inválido: esperando por HELLO")
                writer.close()
                await writer.wait_closed()

        except Exception as e:
            self.logger.error(f"[PeerServer] Erro no processamento do handshake: {e}")
            writer.close() 
            await writer.wait_closed()  

        finally:
            self.logger.error(f"[PeerServer] Fechando conexão...")
            writer.close() 
            await writer.wait_closed()  


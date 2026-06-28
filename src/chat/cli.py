import asyncio
import logging

from peer_table import ConnectionState

class CLI:

    def __init__(self, peer_server, peer_table, router, logger: logging.Logger | None = None):
        self.peer_server = peer_server
        self.peer_table = peer_table 
        self.logger = logger or logging.getLogger(__name__)
        self.router = router

    async def run(self):
        loop = asyncio.get_event_loop()

        while True:
            cmd = await loop.run_in_executor(None, input, "> ")
            cmd = cmd.strip() #separa argumentos inseridos em cmd
            cmd = cmd.split()

            if cmd[0] in ('/quit', '/q'):
                self.logger.info("[CLI] Quitting...")
                break

            elif cmd[0] == '/peers':

                if len(cmd) == 1 or cmd[1] == '*':
                    self.logger.info("[CLI] Listando todos os peers:")
                    for record in self.peer_table.all():
                        peer_id = record.peer_id
                        self.logger.info(f"[CLI] {peer_id}")

                elif len(cmd) == 2:
                    namespace = cmd[1]
                    self.logger.info(f"[CLI] Listando todos os peers em {namespace}:")
                    peers_in_namespace = self.peer_table.in_namespace(namespace)
                    for element in peers_in_namespace:
                        self.logger.info(f"[CLI] {peer_id}")

                else:
                    self.logger.info("[CLI] Erro em /peers")

            elif cmd[0] in ('/msg', '/m'):

                if len(cmd) < 3:
                    self.logger.info(f"[CLI] Parâmetros errados para /msg")
                else:
                    peer_id = cmd[1]
                    payload = cmd[2:]
                    if self.peer_table.contains(peer_id):
                        mensagem = ' '.join(payload)
                        await self.router.send(peer_id= peer_id, namespace=None, message=mensagem, mode='SEND', require_ack=True)
                        self.logger.info(f"[CLI] Mensagem enviada para {peer_id}")
                    else:
                        self.logger.info(f"[CLI] {peer_id} não está na peer table")

            elif cmd[0] == '/pub':

                if len(cmd) < 3:
                    self.logger.info(f"[CLI] Parâmetros errados para /pub")
                else:
                    region = cmd[1]
                    payload = cmd[2:]  
                    if region == '*':
                        mensagem = ' '.join(payload)
                        await self.router.send(namespace='*', message=mensagem, mode= 'PUB', require_ack= False)
                        self.logger.info("[CLI] Mensagem global enviada")
                    else:
                        peers_in_namespace = self.peer_table.in_namespace(region)
                        if peers_in_namespace == []:
                            self.logger.info("[CLI] Namespace vazio ou inexistente, logo nenhuma mensagem enviada")
                        else:
                            await self.router.send(namespace=region, message=mensagem, mode= 'PUB', require_ack= False)
                            self.logger.info(f"[CLI] Mensagem enviada para peers em namespace: {region}")


            elif cmd[0] == "/log":

                if len(cmd) == 2:
                    level = cmd[1].upper()
                    
                    # 2. Valida se o nível informado é suportado pelo Python logging
                    if level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
                        self.logger.info("[CLI] Nível inválido. Use: DEBUG, INFO, WARNING ou ERROR")
                    
                    else: 
                        logging.getLogger().setLevel(getattr(logging, level))
                        self.logger.setLevel(getattr(logging, level))
                        
                        self.logger.info(f"[CLI] Nível de log ajustado para {level}")
                
                else:
                    self.logger.info("[CLI] Parâmetros errados para /log")

            elif cmd[0] == '/rtt':
                self.logger.info("[CLI] rtt's dos peers requeridos:")

                if len(cmd) == 1:  #checa se quer tabela geral de todos rtts
                    for record in self.peer_table.all():
                        peer_id = record.peer_id
                        mean_rtt = record.rtt_ms
                        connection_state = record.connection_state

                        if connection_state == ConnectionState.STALE:
                            self.logger.info(f"[CLI] Stale connection for {peer_id}")
                        elif mean_rtt is not None:
                            self.logger.info(f"[CLI] Mean RTT for {peer_id}: {mean_rtt:.1f} ms")
                        else:
                            self.logger.info(f"[CLI] No connection for {peer_id}")

                else:
                    peer_id = cmd[1]
                    record = self.peer_table.get(peer_id)

                    if record:
                        if record.connection_state == ConnectionState.STALE:
                            self.logger.info(f"[CLI] Stale connection for {peer_id}")
                        elif record.rtt_ms is not None:
                            self.logger.info(f"[CLI] Mean RTT for {peer_id}: {mean_rtt:.1f} ms")
                        else:
                            self.logger.info(f"[CLI] No connection for {peer_id}")

                    else:
                        self.logger.info(f"[CLI] Peer {peer_id} not found in the table.")

            elif cmd[0] == '/conn':
                self.logger.info("[CLI] Conexões ativas:")

                inbound_peers = []
                outbound_peers = []

                for peer_id, connection in list(self.peer_server.connections.items()):
                    if connection.direction == 'inbound':
                        inbound_peers.append(peer_id)
                    elif connection.direction == 'outbound':
                        outbound_peers.append(peer_id)

                self.logger.info(f"[CLI] Conexões de Entrada (Inbound): {', '.join(inbound_peers) if inbound_peers else 'Nenhuma'}")
                self.logger.info(f"[CLI] Conexões de Saída (Outbound): {', '.join(outbound_peers) if outbound_peers else 'Nenhuma'}")

            elif cmd[0] == '/reconnect':
                self.logger.info("[CLI] Tentativa de reconexão com peers")
                try:
                    await self.router.trigger_reconcile()
                    self.logger.info("[CLI] Sucesso em reconexão")
                except Exception as e:
                    self.logger.warning(f"[CLI] Erro em reconexão: {e}")

            else:
                self.logger.info("[CLI] Comando inválido, use: /peers, /msg, /pub, /conn, /rtt, /reconnect, /log, /quit")

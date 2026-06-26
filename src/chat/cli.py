import asyncio
import logging
import rendezvous_connection

from peer_table import ConnectionState

class CLI:

    def __init__(self, peer_table, router, logger: logging.Logger | None = None):
        self.peer_table = peer_table 
        self.logger = logger or logging.getLogger(__name__)
        self.router = router

    async def run(self):
        self.logger.info("[CLI] digite '/quit' para sair")

        loop = asyncio.get_event_loop()

        while True:
            cmd = await loop.run_in_executor(None, input, "> ")
            cmd = cmd.strip()
            cmd = cmd.split()
            com = cmd.pop(0)

            match com:
                case '/help':
                    self.logger.info('[CLI] Comandos disponíveis:')
                    self.logger.info('[CLI]     /help                       - Mostra esta mensagem')
                    self.logger.info('[CLI]     /peers [* or #namespace]    - Descobre novos peers')
                    self.logger.info('[CLI]     /msg <peer_id> <message>    - Envia uma mensagem direta')
                    self.logger.info('[CLI]     /pub * <message>            - Publica uma mensagem a todos os peers conhecidos')
                    self.logger.info('[CLI]     /pub #<namespace> <msg>     - Publica uma mensagem a todos os peers de um mesmo grupo')
                    self.logger.info('[CLI]     /conn                       - Mostra conexões inbound/outbound ativas no momento')
                    self.logger.info('[CLI]     /rtt                        - Mostra o RTT (Round-Trip Time) para os peers conectados')
                    self.logger.info('[CLI]     /reconnect                  - Força reconexão a um determinado peer (manual reconcile)')
                    self.logger.info('[CLI]     /log <LEVEL>                - Seleciona nivelamento de log (DEBUG, INFO, WARNING, ERROR)')
                    self.logger.info('[CLI]     /quit                       - Sai da aplicação')

<<<<<<< HEAD
            elif cmd[0] == '\peers':

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
                    self.logger.info("[CLI] Error in cmd \peers")

            elif cmd[0] in ('\msg', '\m'):

                if len(cmd) != 3:
                    self.logger.info(f"[CLI] Parâmetros errads para \msg")
                else:
                    peer_id = cmd[1]
                    payload = cmd[2]
                    if self.peer_table.contains(peer_id):
                        await self.router.send(peer_id= peer_id, namespace=None, message=payload, mode='SEND', require_ack=True)
                        self.logger.info(f"[CLI] Mensagem enviada para {peer_id}")
                    else:
                        self.logger.info(f"[CLI] {peer_id} não está na peer table")

            elif cmd[0] == '\pub':

                if len(cmd) != 3:
                    self.logger.info(f"[CLI] Parâmetros errados para \pub")
                else:
                    region = cmd[1]
                    payload = cmd[2]  
                    if region == '*':
                        await self.router.send(peer_id= peer_id, namespace='*', message=payload, mode= 'PUB', require_ack= False)
                        self.logger.info("[CLI] Mensagem global enviada")
                    else:
                        peers_in_namespace = self.peer_table.in_namespace(region)
                        if peers_in_namespace == []:
                            self.logger.info("[CLI] Namespace vazio ou inexistente, logo nenhuma mensagem enviada")
                        else:
                            await self.router.send(peer_id= peer_id, namespace=region, message=payload, mode= 'PUB', require_ack= False)
                            self.logger.info(f"[CLI] Mensagem enviada para peers em namespace: {region}")


            elif cmd[0] == r'\rtt':
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
=======
                case '/peers':
                    args = str(cmd.pop(0))
>>>>>>> origin/aluno2

                    if not args:
                        disc = self.state.namespace()
                    if args[0] == '#':
                        disc = args[1:] or None
                    elif args == '*':
                        disc = None
                    else:
                        self.logger.info("[CLI] Uso incorreto: /peers [* or #namespace]")
                        continue

                    try:
                        response = await self.rdv.discover(disc)
                        peers = response.get('peers', [])

                    except Exception as err:
                        if disc is None and 'namespace' in str(err).lower():
                            self.logger.info("[CLI] Servidor requer uma sala. Use: /peers #<namespace>")
                        else:
                            self.logger.warning(f"[CLI] Descobreta mal-sucedida: {err}")

                case '/msg':
                    peer = cmd.pop(0)

                    if not self.router.peer_server.connections:
                        self.logger.info('[CLI] Sem peers ativos. Use /peers')
                    else:
                        await self.router.send(peer, ' '.join(cmd))

                case '/pub':
                    sel = cmd.pop(0)

                    if sel == '*':
                        await self.router.pub_all(' '.join(cmd))
                    elif cmd[0][0] == '#':
                        target = cmd.pop(0)
                        await self.pub_namespace(target, " ".join(cmd))
                    else:
                        self.logger.warning('[CLI] Alvo de PUB inválido.')

                case '/reconnect':
                    self.logger.info("[CLI] Tentando alcançar o peer...")

                    try:
                        await self.router.trigger_reconcile()

                    except Exception as err:
                        self.logger.warning(f'[CLI] Reconexão mal-sucedida: {err}')

                case '/log':
                    command = str(cmd).upper()

                    if command not in {'DEBUG', 'INFO', 'ERROR', 'WARNING'} or len(command) < 3:
                        self.logger.warning('[CLI] Modo inválido. Selecione entre: DEBUG, INFO, WARNING ou ERROR')
                    else:
                        logging.getLogger().setLevel(getattr(logging, command))
                        self.logger.setLevel(getattr(logging, command))
                        self.logger.info(f'[CLI] Selecionado para: {command}')
                case _:
                    self.logger.warning(f'[CLI] Comando desconhecido: {com}')


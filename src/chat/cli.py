import asyncio
import logging
import rendezvous_connection

from peer_table import ConnectionState

class CLI:

    def __init__(self, peer_table, logger: logging.Logger | None = None):
        self.peer_table = peer_table #router
        self.logger = logger or logging.getLogger(__name__)

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

                case '/peers':
                    args = str(cmd.pop(0))

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


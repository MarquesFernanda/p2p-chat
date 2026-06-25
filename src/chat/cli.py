import asyncio
import logging

from peer_table import ConnectionState

class CLI:

    def __init__(self, peer_table, logger: logging.Logger | None = None):
        self.peer_table = peer_table #router
        self.logger = logger or logging.getLogger(__name__)

    async def run(self):
        loop = asyncio.get_event_loop()

        while True:
            cmd = await loop.run_in_executor(None, input, "> ")
            cmd = cmd.strip() #separa argumentos inseridos em cmd
            cmd = cmd.split()

            if cmd[0] in ('\quit', '\q'):
                self.logger.info("[CLI] Quitting...")
                break

            elif cmd[0] == r'\rtt':
                self.logger.info("[CLI] Requested peer rtt's:")

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
                        

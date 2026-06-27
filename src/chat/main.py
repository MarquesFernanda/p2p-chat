import asyncio
import json
import logging
from typing import Dict, Optional, Set

def log_config(self, app: str, file_level: str = 'DEBUG', file: Optional[str] = None):

    logger_pai = logging.getLogger()
    logger_pai.setLevel(file_level) #bota como nível mínimo debug sempre

    #limpa todos Handlers caso haja algum de configurações anteriores
    if logger_pai.hasHandlers():
        logger_pai.handlers.clear()

    if file:
        file_handler = logging.FileHandler(filename= file, mode= 'a', encoding= "utf-8")
        file_handler_formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        ) #define formato de mensagens

        file_handler.setLevel(file_level)

        file_handler.setFormatter(file_handler_formatter) #define file handler
        logger_pai.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler_formatter = logging.Formatter('%(levelname)s %(message)s')
        console_handler.setFormatter(console_handler_formatter)

        def meu_filtro_cli(record):
            msg = record.getMessage()
            return any(tag in msg for tag in ["[CLI]", "[P2P]", "[MSG]", "[PUB]"])

        console_handler.addFilter(meu_filtro_cli)
        logger_pai.addHandler(console_handler)

        logger_pai.info(f"[P2P] Logs de nível 'INFO' impressos no terminal e logs de nível {file_level} impressos em {file}")

    else:
        logging.basicConfig(
            level=getattr(logging, file_level),
            format="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        logger_pai.info("[P2P] Todos logs serão impressos no terminal")

    return logging.getLogger(app)

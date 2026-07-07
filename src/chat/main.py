'''
IDENTIFICAÇÃO
GRUPO 11
Daniel Carvalho Mendes (231012737);
Fernanda Marques Pereira (222024560);
Thiago Carvalho Silva (222006463)

'''

import argparse
import asyncio
import logging
import sys
from pathlib import Path
import json
from typing import Any, Dict, Optional


def merging(config: Dict[str, Any], cli: Dict[str, Any]):
    out = {}

    for key, value in cli.items():
        if value is not None:
            out[key.replace('-', '_')] = value

    return {**config, **out}


def setup_log(opt: str, app_name: str, file: Optional[str], is_log: bool):
    rtlog = logging.getLogger()
    rtlog.setLevel(getattr(logging, opt))

    if rtlog.hasHandlers():
        rtlog.handlers.clear()

    if file and not is_log:
        handler = logging.FileHandler(file, mode='w')
        formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        )

        handler.setFormatter(formatter)
        rtlog.addHandler(handler)

        console = logging.StreamHandler(sys.stderr)
        console.setLevel(logging.INFO)
        cons_formatter = logging.Formatter("%(message)s")
        console.setFormatter(cons_formatter)

        class CLIFilter(logging.Filter):
            def filter(self, record):
                return (
                    "[CLI]" in record.getMessage()
                    or "[P2P]" in record.getMessage()
                    or "[MSG]" in record.getMessage()
                    or "[PUB]" in record.getMessage()
                )

        console.addFilter(CLIFilter())
        rtlog.addHandler(console)

        rtlog.info(f'[P2P] Logging iniciado: {file}')

    else:
        logging.basicConfig(
            level=getattr(logging, opt),
            format="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        )
        rtlog.info('[P2P] Logging para o arquivo desativado. Todas as mensagens aparecerão na tela.')

    return logging.getLogger(app_name)


def loadfile(path: str):
    p = Path(path)
    out = {}

    if p.exists():
        file = open(p, 'r')
        out = json.load(file)
        file.close()

    return out


def get_config_args() -> argparse.ArgumentParser:
    conf = argparse.ArgumentParser(description="Inicializando cliente de chat p2p...")

    conf.add_argument("--app-name")
    conf.add_argument("--name")
    conf.add_argument("--namespace")
    conf.add_argument("--listen-host")
    conf.add_argument("--listen-port", type=int)
    conf.add_argument("--rdv-host")
    conf.add_argument("--rdv-port", type=int)
    conf.add_argument("--rdv-ttl", type=int)
    conf.add_argument("--discover-interval", type=int)
    conf.add_argument("--keepalive-interval", type=int)
    conf.add_argument("--msg-ttl", type=int)
    conf.add_argument("--log-level", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    conf.add_argument("--config", default="config.json")
    conf.add_argument("--log-file")
    conf.add_argument("--no-log-file", action="store_true", default=False)

    return conf



async def initializer():
    args = vars(get_config_args().parse_args())
    ldfile = loadfile(args.get('config'))
    logger = logging.getLogger('peer-to-peer chat')

    if args.get('config'):
        logger.info(f"[P2P] Configurações determinadas pelo {args['config']}")

    args_merging = {
        key: value
        for key, value in args.items()
        if key not in ('log_file', 'no_log_file')
    }

    end_merge = merging(ldfile, args_merging)

    from dataclasses import fields
    from p2p_client import ConfiguracoesJson as Settings, p2pChatApp

    allowed = {f.name for f in fields(Settings)}
    unknown = sorted(i for i in end_merge.keys() if i not in allowed)

    if unknown:
        logger.warning('[P2P] Ignoring unknown config keys %s', ', '.join(unknown))

    end_merge.setdefault("app_name", "pyp2p-chat")
    end_merge.setdefault("listen_host", "0.0.0.0")
    end_merge.setdefault("discover_interval", 10)
    end_merge.setdefault("keepalive_interval", 30)
    end_merge.setdefault("rdv_ttl", 300)
    end_merge.setdefault("fixed_msg_ttl", 1)
    end_merge.setdefault("log_level", "INFO")
    end_merge.setdefault("autonomous_mode", True)

    logger = setup_log(
        end_merge["log_level"],
        end_merge["app_name"],
        args.get("log_file"),
        args.get("no_log_file", False)
    )


    app = p2pChatApp(Settings(**{k: v for k, v in end_merge.items() if k in allowed}), logger)

    try:
        await app.run()
        return 0
    except Exception:
        logger.exception('[P2P] Fatal error')
        return 1


def main():
    try:
        exit_code = asyncio.run(initializer())
    except KeyboardInterrupt:
        exit_code = 130
    except Exception as err:
        logging.basicConfig()
        logging.exception(f"[P2P] Erro inesperado: {err}")
        exit_code = 1

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()

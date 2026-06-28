import argparse
import asyncio
import logging
import sys  # CORREÇÃO 1: 'sys' estava faltando nos imports (usado em setup_log via logging.StreamHandler(sys.stderr))
from pathlib import Path
import json
from typing import Any, Dict, Optional  # CORREÇÃO 2: Dict e Optional estavam faltando nos imports


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
        handler = logging.FileHandler(file, mode='a')
        # CORREÇÃO 3: parâmetros do Formatter estavam errados.
        # 'format' → 'fmt', 'date_format' → 'datefmt'
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

        # CORREÇÃO 4: o CLIFilter estava sendo adicionado ao handler de arquivo (handler),
        # mas o console nunca recebia o filtro.  A intenção correta é filtrar apenas o console.
        console.addFilter(CLIFilter())
        rtlog.addHandler(console)

        rtlog.info(f'[P2P] Logging iniciado: {file}')

    else:
        # CORREÇÃO 5: logging.basicConfig() não aceita 'opt' como parâmetro, o correto é 'level'
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

    # Argumentos posicionais obrigatórios foram trocados por opcionais (--flag)
    # pois o arquivo de config já pode suprir a maioria deles
    conf.add_argument("--app-name")
    conf.add_argument("--name")
    conf.add_argument("--namespace")
    conf.add_argument("--listen-host")
    conf.add_argument("--listen-port", type=int)
    # CORREÇÃO 6: rdv-host era string mas estava tipado como int
    conf.add_argument("--rdv-host")
    conf.add_argument("--rdv-port", type=int)
    conf.add_argument("--rdv-ttl", type=int)
    conf.add_argument("--discover-interval", type=int)
    conf.add_argument("--keepalive-interval", type=int)
    conf.add_argument("--msg-ttl", type=int)
    # CORREÇÃO 7: parâmetro 'opt' não existe em add_argument; o correto é 'choices'
    conf.add_argument("--log-level", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    # CORREÇÃO 8: --config e --no-log-file nunca foram adicionados ao parser,
    # mas eram usados em initializer() via args.get('config') e args.get('no_log_file')
    conf.add_argument("--config", default="config.json")
    conf.add_argument("--log-file")
    conf.add_argument("--no-log-file", action="store_true", default=False)

    return conf


# CORREÇÃO 9: initializer() usava 'await' internamente mas não era declarada como 'async'
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
    # CORREÇÃO 10: Settings não estava importado; a classe correta é ConfiguraçõesJson (de p2p.py)
    # e o import deve acontecer aqui para evitar importação circular
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

    # CORREÇÃO 11: variável estava sendo referenciada como 'merged' mas o nome correto é 'end_merge'
    logger = setup_log(
        end_merge["log_level"],
        end_merge["app_name"],
        args.get("log_file"),
        args.get("no_log_file", False)
    )

    # CORREÇÃO 12: _validate_settings não estava definida em nenhum lugar.
    # Removida a chamada; a validação mínima já ocorre via fields() acima.
    # Se desejar validação adicional, implemente a função e reative aqui.

    app = p2pChatApp(Settings(**{k: v for k, v in end_merge.items() if k in allowed}), logger)

    try:
        # CORREÇÃO 13: run() em p2p.py não retornava código de saída; agora retorna 0 em sucesso
        await app.run()
        return 0
    except Exception:
        logger.exception('[P2P] Fatal error')
        return 1


def main():
    try:
        # CORREÇÃO 14: initializer agora é async, então asyncio.run() está correto
        exit_code = asyncio.run(initializer())
    except KeyboardInterrupt:
        exit_code = 130
    except Exception as err:
        # CORREÇÃO 15: o except original chamava logging.basicConfig() sem exibir o erro
        logging.basicConfig()
        logging.exception(f"[P2P] Erro inesperado: {err}")
        exit_code = 1
    # CORREÇÃO 16: raise SystemExit sem argumento sempre sai com código 0 (sucesso).
    # O correto é passar o código de retorno calculado.
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
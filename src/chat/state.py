import logging
from typing import Dict, List, Any, Optional


class State:

    def __init__(self, logger: logging.Logger | None = None):
        self._name = None
        self._namespace = None

        # lista de dicts de peers formato do discover
        self._peers_by_ns: Dict[str, List[dict]] = {}
        self.logger = logger or logging.getLogger(__name__)

    def set_identity(self, name: str, namespace: str):
        self._name = name
        self._namespace = namespace

    def peer_id(self) -> str:
        return f"{self._name}@{self._namespace}"

    def namespace(self) -> Optional[str]:
        return self._namespace

    def peers(self, namespace: Optional[str] = None):
        if not namespace:
            return {ns: list(lst) for ns, lst in self._peers_by_ns.items()}

        ns = (namespace or "*").strip() or "*"
        return list(self._peers_by_ns.get(ns, []))

    def namespaces(self) -> List[str]:
        return sorted(self._peers_by_ns.keys())

    def find_peer(self, name: str, namespace: Optional[str] = None) -> Optional[dict]:
        ns = (namespace or "*").strip() or "*"
        for p in self._peers_by_ns.get(ns, []):
            if p.get("name") == name:
                return p
        return None

    def update_namespace_peers(self, namespace: str, peers: list[dict]) -> None:
        ns = (namespace or "*").strip() or "*"
        clean = [p for p in peers if isinstance(p, dict)]
        self._peers_by_ns[ns] = self._dedup(clean)
        self.logger.debug(f"[State] Set peers for #{ns}: {len(self._peers_by_ns[ns])} item(s)")

    def update_bulk(self, peers: list[dict]) -> None:
        grouped: Dict[str, List[dict]] = {}
        for p in peers:
            if not isinstance(p, dict):
                continue

            ns = (p.get("namespace") or "*").strip() or "*"
            grouped.setdefault(ns, []).append(p)

        for ns, lst in grouped.items():
            self.update_namespace_peers(ns, lst)

    def _dedup(self, peers: list[dict]) -> list[dict]:
        seen = set()
        out: list[dict] = []
        for p in peers:
            key = (p.get("name"), p.get("ip"), p.get("port")), p.get("namespace")
            if key not in seen:
                seen.add(key)
                out.append(p)
        return out
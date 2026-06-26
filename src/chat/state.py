import logging
from typing import Dict, List, Any, Optional

class State:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.log = logger or logging.getLogger(__name__)
        self.my_username: Optional[str] = None
        self.my_room: Optional[str] = None
        
        #ista de peers
        self.registry: Dict[str, List[dict]] = {}

    def set_identity(self, name: str, namespace: str) -> None:
        self.my_username = name
        self.my_room = namespace
        self.log.info(f"[State] Identidade local configurada {self.peer_id()}")

    def peer_id(self) -> str:
        if self.my_username and self.my_room:
            return f"{self.my_username}@{self.my_room}"
        return "anonimo@desconhecido"
        
    def namespace(self) -> Optional[str]:
        return self.my_room
        
    def peers(self, namespace: Optional[str] = None) -> Any:
        if namespace is not None:
            return self.registry.get(namespace, [])
        return self.registry
        
    def namespaces(self) -> List[str]:
        return list(self.registry.keys())
        
    def find_peer(self, name: str, namespace: Optional[str] = None) -> Optional[dict]:
        if namespace is not None:
            for node in self.registry.get(namespace, []):
                if node.get("name") == name:
                    return node
        else:
            for room_nodes in self.registry.values():
                for node in room_nodes:
                    if node.get("name") == name:
                        return node
        return None
        
    def update_namespace_peers(self, namespace: str, peers: list[dict]) -> None:
        self.registry[namespace] = self._clean_and_filter(peers)
        self.log.debug(f"[State] Sala '{namespace}' atualizada. Total de peers ativos: {len(self.registry[namespace])}")
        
    def update_bulk(self, peers: list[dict]) -> None:
        segregated_data: Dict[str, List[dict]] = {}
        
        for p in peers:
            target_room = p.get("namespace") or self.my_room or "default"
            if target_room not in segregated_data:
                segregated_data[target_room] = []
            segregated_data[target_room].append(p)
            
        for room, node_list in segregated_data.items():
            self.registry[room] = self._clean_and_filter(node_list)
        
    def _clean_and_filter(self, raw_peers: list[dict]) -> list[dict]:
        processed_keys = set()
        cleaned_list = []
        
        for node in raw_peers:
            node_name = node.get("name")
            node_room = node.get("namespace") or self.my_room
            
            if node_name == self.my_username and node_room == self.my_room:
                continue
                
            unique_signature = (node_name, node_room)
            if unique_signature not in processed_keys:
                processed_keys.add(unique_signature)
                cleaned_list.append(node)
                
        return cleaned_list
from __future__ import annotations

import asyncio
import inspect
import json
import re
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from chia.protocols import full_node_protocol
from chia.full_node.full_node_api import FullNodeAPI
from chia.server.outbound_message import NodeType
from chia_rs import G2Element, SpendBundle
from chia_rs.sized_bytes import bytes32


@dataclass
class Peer:
    peer_node_id: bytes32
    peer_host: str
    connection_type: NodeType = NodeType.FULL_NODE
    closed: list[Any] = field(default_factory=list)

    async def close(self, *args: Any, **kwargs: Any) -> None:
        self.closed.append({"args": repr(args), "kwargs": repr(kwargs)})


def pending_attributes(source: str) -> list[str]:
    names = set(re.findall(r"self\.full_node\.([A-Za-z_][A-Za-z0-9_]*pending[A-Za-z0-9_]*)", source, re.I))
    names.update(re.findall(r"self\.full_node\.([A-Za-z_][A-Za-z0-9_]*(?:request|response)[A-Za-z0-9_]*)", source, re.I))
    return sorted(names)


def make_full_node() -> MagicMock:
    node = MagicMock()
    # Common downstream members are permissive mocks. The campaign is about the
    # handler's authorization/state check, not bypassing transaction validation.
    node.mempool_manager = MagicMock()
    node.mempool_manager.get_mempool_item = MagicMock(return_value=None)
    node.mempool_manager.seen = MagicMock(return_value=False)
    node.mempool_manager.seen_bundle_hashes = {}
    node.add_transaction = AsyncMock(return_value=(None, None))
    node.respond_transaction = AsyncMock(return_value=None)
    node.server = MagicMock()
    node.config = {}
    node.constants = MagicMock()
    return node


def configure_pending(node: Any, attrs: list[str], tx_id: bytes32, expected_peer: bytes32, mode: str) -> None:
    for attr in attrs:
        if mode == "empty-set":
            value: Any = set()
        elif mode == "global-set":
            value = {tx_id}
        elif mode == "peer-map-correct":
            value = {tx_id: expected_peer}
        elif mode == "peer-map-wrong":
            value = {tx_id: bytes32([0xEE] * 32)}
        elif mode == "nested-map":
            value = {expected_peer: {tx_id}}
        else:
            raise ValueError(mode)
        setattr(node, attr, value)


async def invoke(api: Any, request: Any, peer: Peer) -> dict[str, Any]:
    before = len(peer.closed)
    outcome: dict[str, Any] = {"returned": None, "exception": None}
    try:
        outcome["returned"] = repr(await api.respond_transaction(request, peer))
    except Exception as error:
        outcome["exception"] = f"{type(error).__name__}: {error}"
    outcome["closed"] = len(peer.closed) > before
    outcome["close_events"] = peer.closed[before:]
    return outcome


async def main() -> None:
    source = inspect.getsource(FullNodeAPI.respond_transaction)
    attrs = pending_attributes(source)
    print("RESPOND_TRANSACTION_SIGNATURE=" + str(inspect.signature(FullNodeAPI.respond_transaction)))
    print("PENDING_ATTRIBUTES=" + json.dumps(attrs))
    print("SOURCE_BEGIN")
    print(source)
    print("SOURCE_END")

    transaction = SpendBundle([], G2Element())
    tx_id = transaction.name()
    request = full_node_protocol.RespondTransaction(transaction)
    peer_a = Peer(bytes32([0xAA] * 32), "10.0.0.1")
    peer_b = Peer(bytes32([0xBB] * 32), "10.0.0.2")

    matrix: list[dict[str, Any]] = []
    for mode in ("empty-set", "global-set", "peer-map-correct", "peer-map-wrong", "nested-map"):
        node = make_full_node()
        configure_pending(node, attrs, tx_id, peer_a.peer_node_id, mode)
        api = FullNodeAPI(node)
        current_peer = Peer(peer_b.peer_node_id, peer_b.peer_host)
        result = await invoke(api, request, current_peer)
        matrix.append({"mode": mode, "sender": "B", **result})

    print("MATRIX=" + json.dumps(matrix, sort_keys=True))

    # Detect the dangerous form directly from source and behavior. A global set
    # keyed only by transaction id cannot authenticate which peer was asked.
    source_uses_peer_binding = bool(
        re.search(r"pending[^\n]{0,160}peer_node_id|peer_node_id[^\n]{0,160}pending", source, re.I)
    )
    global_case = next(x for x in matrix if x["mode"] == "global-set")
    empty_case = next(x for x in matrix if x["mode"] == "empty-set")

    print(f"SOURCE_USES_PEER_BINDING={source_uses_peer_binding}")
    print(f"EMPTY_STATE_CLOSES={empty_case['closed']}")
    print(f"GLOBAL_PENDING_WRONG_PEER_CLOSES={global_case['closed']}")
    print(f"GLOBAL_PENDING_WRONG_PEER_EXCEPTION={global_case['exception']}")

    # A candidate exists only if the handler distinguishes unsolicited traffic
    # in the empty control but accepts the same response from the wrong peer when
    # another peer's tx-id is globally pending.
    candidate = empty_case["closed"] and not global_case["closed"] and not source_uses_peer_binding
    if candidate:
        print("RESULT=CROSS_PEER_PENDING_TRANSACTION_AUTHORIZATION_CONFUSION")
    else:
        print("RESULT=NO_CROSS_PEER_PENDING_TRANSACTION_CONFUSION")


if __name__ == "__main__":
    asyncio.run(main())

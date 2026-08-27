from __future__ import annotations

import asyncio
import inspect
import json
import re
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

from chia._tests.core.mempool.test_mempool_manager import IDENTITY_PUZZLE, IDENTITY_PUZZLE_HASH, TestCoins, add_spendbundle, setup_mempool
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

N = 16


@dataclass
class Peer:
    peer_node_id: bytes32
    peer_host: str
    connection_type: NodeType = NodeType.FULL_NODE
    sent: list[Any] = field(default_factory=list)
    closed: list[Any] = field(default_factory=list)

    async def send_message(self, message: Any) -> None:
        self.sent.append(message)

    async def close(self, *args: Any, **kwargs: Any) -> None:
        self.closed.append({"args": repr(args), "kwargs": repr(kwargs)})


def pending_info():
    source = inspect.getsource(FullNodeAPI.new_transaction) + "\n" + inspect.getsource(FullNodeAPI.respond_transaction)
    names = re.findall(r"self\.full_node\.([A-Za-z_][A-Za-z0-9_]*(?:pending|requesting|requested)[A-Za-z0-9_]*)", source, re.I)
    if not names:
        raise RuntimeError("pending attribute not found")
    name = sorted(set(names), key=lambda value: (("tx" not in value.lower() and "transaction" not in value.lower()), len(value)))[0]
    container: Any = set() if re.search(rf"{re.escape(name)}\.(?:add|discard|remove)", source) else {}
    peer_bound = bool(re.search(r"peer_node_id[^\n]{0,240}pending|pending[^\n]{0,240}peer_node_id", source, re.I))
    return name, container, peer_bound


def announcement(transaction_id: bytes32):
    signature = inspect.signature(full_node_protocol.NewTransaction)
    arguments = []
    for name in signature.parameters:
        lower = name.lower()
        if "transaction" in lower and ("id" in lower or "hash" in lower):
            arguments.append(transaction_id)
        elif "cost" in lower or "fee" in lower:
            arguments.append(uint64(1))
        else:
            raise RuntimeError(name)
    return full_node_protocol.NewTransaction(*arguments)


def transaction(index: int):
    coin = Coin(bytes32((50_000 + index).to_bytes(32, "big")), IDENTITY_PUZZLE_HASH, uint64(1_000_000))
    spend = make_spend(
        coin,
        IDENTITY_PUZZLE,
        SerializedProgram.to([[ConditionOpcode.CREATE_COIN, IDENTITY_PUZZLE_HASH, uint64(1_000_000)]]),
    )
    return coin, SpendBundle([spend], G2Element())


class Node:
    def __init__(self, manager: Any, pending_name: str, container: Any):
        self.mempool_manager = manager
        setattr(self, pending_name, container)
        self.sync_store = MagicMock()
        self.sync_store.get_sync_mode.return_value = False
        self.server = MagicMock()
        self.config = {}
        self.constants = manager.constants

    async def add_transaction(self, transaction: SpendBundle, name: bytes32, *args: Any, **kwargs: Any):
        return await add_spendbundle(self.mempool_manager, transaction, name)

    async def respond_transaction(self, transaction: SpendBundle, *args: Any, **kwargs: Any):
        return await self.add_transaction(transaction, transaction.name(), *args, **kwargs)

    def __getattr__(self, name: str):
        if name.startswith("broadcast") or name.startswith("send"):
            async def noop(*args: Any, **kwargs: Any) -> None:
                return None
            return noop
        raise AttributeError(name)


async def invoke(function: Any, request: Any, peer: Peer):
    before = len(peer.closed)
    try:
        returned = await function(request, peer)
        error = None
    except Exception as exception:
        returned = None
        error = f"{type(exception).__name__}: {exception}"
    return {"closed": len(peer.closed) > before, "error": error, "returned": repr(returned)}


async def scenario(mode: str, pending_name: str, container_type: type, coins: list[Coin], bundles: list[SpendBundle]):
    async with setup_mempool(TestCoins(coins, {})) as manager:
        node = Node(manager, pending_name, container_type())
        api = FullNodeAPI(node)
        attacker = Peer(bytes32([0xD0] * 32), "198.51.100.200")
        honest = [Peer(bytes32((60_000 + index).to_bytes(32, "big")), f"198.51.100.{10 + index}") for index in range(N)]
        owners: dict[bytes32, bytes32] = {}
        details = []
        for index, (honest_peer, bundle) in enumerate(zip(honest, bundles)):
            announced = await invoke(api.new_transaction, announcement(bundle.name()), honest_peer)
            owners[bundle.name()] = honest_peer.peer_node_id
            if mode == "attack":
                attacker_result = await invoke(api.respond_transaction, full_node_protocol.RespondTransaction(bundle), attacker)
                honest_result = await invoke(api.respond_transaction, full_node_protocol.RespondTransaction(bundle), honest_peer)
            elif mode == "baseline":
                attacker_result = None
                honest_result = await invoke(api.respond_transaction, full_node_protocol.RespondTransaction(bundle), honest_peer)
            elif mode == "control":
                async def controlled(request: Any, peer: Peer):
                    owner = owners.get(request.transaction.name())
                    if owner is not None and owner != peer.peer_node_id:
                        await peer.close(ban_time=600)
                        return None
                    return await api.respond_transaction(request, peer)

                attacker_result = await invoke(controlled, full_node_protocol.RespondTransaction(bundle), attacker)
                honest_result = await invoke(controlled, full_node_protocol.RespondTransaction(bundle), honest_peer)
            else:
                raise ValueError(mode)
            details.append(
                {
                    "index": index,
                    "announcement": announced,
                    "attacker": attacker_result,
                    "honest": honest_result,
                    "in_pool": manager.get_mempool_item(bundle.name()) is not None,
                }
            )
        return {
            "mode": mode,
            "honest_closed": sum(bool(peer.closed) for peer in honest),
            "attacker_closed_events": len(attacker.closed),
            "in_pool": sum(manager.get_mempool_item(bundle.name()) is not None for bundle in bundles),
            "details": details,
        }


async def main() -> None:
    pending_name, container, peer_bound = pending_info()
    pairs = [transaction(index) for index in range(N)]
    coins = [pair[0] for pair in pairs]
    bundles = [pair[1] for pair in pairs]
    baseline = await scenario("baseline", pending_name, type(container), coins, bundles)
    attack = await scenario("attack", pending_name, type(container), coins, bundles)
    control = await scenario("control", pending_name, type(container), coins, bundles)
    evidence = {
        "N": N,
        "pending_attribute": pending_name,
        "source_peer_bound": peer_bound,
        "baseline": baseline,
        "attack": attack,
        "control": control,
    }
    print("EVIDENCE=" + json.dumps(evidence, sort_keys=True))
    candidate = (
        baseline["honest_closed"] == 0
        and baseline["in_pool"] == N
        and attack["honest_closed"] == N
        and attack["attacker_closed_events"] == 0
        and attack["in_pool"] == N
        and control["honest_closed"] == 0
        and control["attacker_closed_events"] >= N
        and control["in_pool"] == N
        and not peer_bound
    )
    print(f"BASELINE_HONEST_CLOSED={baseline['honest_closed']}")
    print(f"ATTACK_HONEST_CLOSED={attack['honest_closed']}")
    print(f"ATTACKER_CLOSED_EVENTS={attack['attacker_closed_events']}")
    print(f"ATTACK_VALID_TXS_IN_POOL={attack['in_pool']}")
    print(f"CONTROL_HONEST_CLOSED={control['honest_closed']}")
    print(f"CONTROL_ATTACKER_CLOSED_EVENTS={control['attacker_closed_events']}")
    if candidate:
        print("RESULT=CONFIRMED_CROSS_PEER_RACE_AMPLIFICATION")
    else:
        print("RESULT=NO_CROSS_PEER_RACE_AMPLIFICATION")


if __name__ == "__main__":
    asyncio.run(main())

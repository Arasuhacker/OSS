from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from chia._tests.util.spend_sim import sim_and_client
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program
from chia.wallet.cat_wallet.cat_utils import CAT_MOD, SpendableCAT, construct_cat_puzzle, unsigned_spend_bundle_for_spendable_cats
from chia.wallet.lineage_proof import LineageProof
from chia_rs import G2Element, SpendBundle
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64


@dataclass
class Case:
    amount: int
    output_amount: int
    extra_delta: int
    lineage_amount: int
    output_count: int
    condition_shape: str
    built: bool
    push_status: str
    push_error: str | None
    accepted: bool
    cat_output_total: int | None
    unauthorized_gain: int | None
    bundle_id: str | None


def make_spendable(*, amount: int, output_amount: int, extra_delta: int, lineage_amount: int, output_count: int, condition_shape: str):
    tail_hash = bytes32([0x77] * 32)
    inner = Program.to(1)
    cat_puzzle = construct_cat_puzzle(CAT_MOD, tail_hash, inner)
    parent = Coin(bytes32([0x11] * 32), cat_puzzle.get_tree_hash(), uint64(amount))
    coin = Coin(parent.name(), cat_puzzle.get_tree_hash(), uint64(amount))
    lineage = LineageProof(parent.parent_coin_info, inner.get_tree_hash(), uint64(lineage_amount))
    conditions = []
    if condition_shape == "normal":
        for _ in range(output_count):
            conditions.append([51, inner.get_tree_hash(), output_amount])
    elif condition_shape == "negative":
        for _ in range(output_count):
            conditions.append([51, inner.get_tree_hash(), -abs(output_amount)])
    elif condition_shape == "oversized-int":
        for _ in range(output_count):
            conditions.append([51, inner.get_tree_hash(), 2**65 + output_amount])
    elif condition_shape == "improper":
        conditions = [51, inner.get_tree_hash(), output_amount]
    else:
        raise ValueError(condition_shape)
    inner_solution = Program.to(conditions)
    signature = inspect.signature(SpendableCAT)
    kwargs: dict[str, Any] = {}
    for name, parameter in signature.parameters.items():
        lower = name.lower()
        if lower == "coin":
            kwargs[name] = coin
        elif "limitations_program_hash" in lower or ("limitations_program" in lower and "hash" in lower):
            kwargs[name] = tail_hash
        elif lower == "inner_puzzle":
            kwargs[name] = inner
        elif lower == "inner_solution":
            kwargs[name] = inner_solution
        elif lower == "limitations_solution":
            kwargs[name] = Program.to([])
        elif lower == "extra_delta":
            kwargs[name] = extra_delta
        elif "lineage" in lower:
            kwargs[name] = lineage
        elif parameter.default is not inspect.Parameter.empty:
            continue
        else:
            raise RuntimeError(f"unknown SpendableCAT field: {name}")
    return SpendableCAT(**kwargs), coin, cat_puzzle


def additions_from_bundle(bundle: SpendBundle):
    try:
        return bundle.additions()
    except Exception:
        return []


async def push_case(parameters):
    amount, output_amount, extra_delta, lineage_amount, output_count, shape = parameters
    built = False
    status = "BUILD_ERROR"
    error = None
    accepted = False
    output_total = None
    gain = None
    bundle_id = None
    try:
        spendable, coin, cat_puzzle = make_spendable(
            amount=amount,
            output_amount=output_amount,
            extra_delta=extra_delta,
            lineage_amount=lineage_amount,
            output_count=output_count,
            condition_shape=shape,
        )
        unsigned = unsigned_spend_bundle_for_spendable_cats([spendable])
        bundle = SpendBundle(unsigned.coin_spends, G2Element()) if getattr(unsigned, "aggregated_signature", None) is None else unsigned
        built = True
        bundle_id = bundle.name().hex()
        async with sim_and_client(pass_prefarm=True) as (simulator, client):
            await simulator.coin_store.new_block(
                height=uint32(0),
                timestamp=uint64(1),
                included_reward_coins=[],
                tx_additions=[(coin.name(), coin, False)],
                tx_removals=[],
            )
            result = await client.push_tx(bundle)
            if isinstance(result, tuple):
                inclusion_status, inclusion_error = result
            else:
                inclusion_status, inclusion_error = result, None
            status = getattr(inclusion_status, "name", str(inclusion_status))
            error = None if inclusion_error is None else getattr(inclusion_error, "name", str(inclusion_error))
            accepted = "SUCCESS" in status.upper()
        additions = additions_from_bundle(bundle)
        output_total = sum(int(coin.amount) for coin in additions if coin.puzzle_hash == cat_puzzle.get_tree_hash())
        gain = output_total - amount
    except Exception as exception:
        error = f"{type(exception).__name__}: {exception}"
    return Case(amount, output_amount, extra_delta, lineage_amount, output_count, shape, built, status, error, accepted, output_total, gain, bundle_id)


async def main() -> None:
    print("SPENDABLE_CAT_SIGNATURE=" + str(inspect.signature(SpendableCAT)))
    print("UNSIGNED_BUNDLE_SIGNATURE=" + str(inspect.signature(unsigned_spend_bundle_for_spendable_cats)))
    amounts = [1, 2, 127, 128, 255, 256, 2**31 - 1, 2**32, 2**63 - 1]
    cases = []
    for amount in amounts:
        cases.append((amount, amount, 0, amount, 1, "normal"))
        for output_amount in {0, 1, max(0, amount - 1), amount, amount + 1, min(2**64 - 1, amount * 2)}:
            for delta in {0, 1, -1, amount, -amount, 2**63 - 1, -(2**63)}:
                for lineage_amount in {amount, max(0, amount - 1), min(2**64 - 1, amount + 1)}:
                    cases.append((amount, output_amount, delta, lineage_amount, 1, "normal"))
        cases.extend(
            [
                (amount, amount + 1, 0, amount, 2, "normal"),
                (amount, amount, 0, amount, 1, "negative"),
                (amount, amount, 0, amount, 1, "oversized-int"),
                (amount, amount, 0, amount, 1, "improper"),
            ]
        )
    unique = []
    seen = set()
    for case in cases:
        if case not in seen:
            seen.add(case)
            unique.append(case)
    results = []
    for index, parameters in enumerate(unique):
        case = await push_case(parameters)
        results.append(case)
        if case.accepted and case.unauthorized_gain is not None and case.unauthorized_gain > case.extra_delta:
            print("CONSERVATION_BREAK=" + json.dumps(asdict(case), sort_keys=True))
        if index % 100 == 0:
            print(f"PROGRESS={index}/{len(unique)}")
    breaks = [case for case in results if case.accepted and case.unauthorized_gain is not None and case.unauthorized_gain > case.extra_delta]
    baselines = [
        case
        for case in results
        if case.output_amount == case.amount
        and case.extra_delta == 0
        and case.lineage_amount == case.amount
        and case.output_count == 1
        and case.condition_shape == "normal"
    ]
    Path("CAT_CONSERVATION_RESULTS.json").write_text(
        json.dumps(
            {
                "cases": [asdict(case) for case in results],
                "breaks": [asdict(case) for case in breaks],
                "baselines": [asdict(case) for case in baselines],
            },
            indent=2,
        )
    )
    print(f"CASES={len(results)}")
    print(f"BASELINE_ACCEPTED={sum(case.accepted for case in baselines)}/{len(baselines)}")
    print(f"CONSERVATION_BREAKS={len(breaks)}")
    if breaks and any(case.accepted for case in baselines):
        print("RESULT=CAT_UNAUTHORIZED_MINT_CONSERVATION_BREAK")
    elif not any(case.accepted for case in baselines):
        print("RESULT=HARNESS_BASELINE_INVALID")
    else:
        print("RESULT=NO_CAT_CONSERVATION_BREAK")


if __name__ == "__main__":
    asyncio.run(main())

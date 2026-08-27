from __future__ import annotations

import importlib
import inspect
import itertools
import json
import pkgutil
import re
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

from chia.consensus.default_constants import DEFAULT_CONSTANTS
from chia.types.blockchain_format.serialized_program import SerializedProgram


TARGET_NAMES = {
    "validate_tx_generator",
    "block_has_transactions_generator",
    "get_transactions_generator_bytes",
    "get_transactions_generator_program",
}


@dataclass
class Located:
    name: str
    module: str
    function: Callable[..., Any]


def locate_functions() -> dict[str, Located]:
    import chia.consensus as consensus_pkg
    found: dict[str, Located] = {}
    roots = list(consensus_pkg.__path__)
    for info in pkgutil.walk_packages(roots, consensus_pkg.__name__ + "."):
        if len(found) == len(TARGET_NAMES):
            break
        try:
            module = importlib.import_module(info.name)
        except Exception:
            continue
        for name in TARGET_NAMES - found.keys():
            value = getattr(module, name, None)
            if callable(value):
                found[name] = Located(name, info.name, value)
    # New helpers may live outside consensus.
    if len(found) != len(TARGET_NAMES):
        import chia
        for package_name in ("chia.full_node", "chia.types"):
            try:
                package = importlib.import_module(package_name)
            except Exception:
                continue
            for info in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
                if len(found) == len(TARGET_NAMES):
                    break
                try:
                    module = importlib.import_module(info.name)
                except Exception:
                    continue
                for name in TARGET_NAMES - found.keys():
                    value = getattr(module, name, None)
                    if callable(value):
                        found[name] = Located(name, info.name, value)
    return found


def invoke(located: Located, obj: Any, *, height: int, version: Any, legacy: Any, buffer: Any) -> tuple[str, Any]:
    sig = inspect.signature(located.function)
    kwargs: dict[str, Any] = {}
    for name, param in sig.parameters.items():
        lname = name.lower()
        if lname in {"self", "cls"}:
            continue
        if "constant" in lname:
            kwargs[name] = DEFAULT_CONSTANTS
        elif "height" in lname:
            try:
                from chia_rs.sized_ints import uint32
                kwargs[name] = uint32(max(0, height))
            except Exception:
                kwargs[name] = max(0, height)
        elif lname in {"version", "block_version"}:
            kwargs[name] = version
        elif "buffer" in lname:
            kwargs[name] = buffer
        elif "generator" in lname and lname not in {"generator_block_info"}:
            kwargs[name] = legacy
        elif any(token in lname for token in ("block", "info", "item", "obj")):
            kwargs[name] = obj
        elif param.default is not inspect.Parameter.empty:
            continue
        else:
            kwargs[name] = obj
    try:
        return "ok", located.function(**kwargs)
    except Exception as error:
        return "error", f"{type(error).__name__}: {error}"


def norm(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, bytes):
        return {"bytes": value.hex(), "len": len(value)}
    try:
        b = bytes(value)
        return {"bytes": b.hex(), "len": len(b), "type": type(value).__name__}
    except Exception:
        return repr(value)


def accepted(status: tuple[str, Any]) -> bool:
    if status[0] != "ok":
        return False
    value = status[1]
    if isinstance(value, bool):
        return value
    if value is None:
        return True
    if isinstance(value, tuple) and value and isinstance(value[0], bool):
        return bool(value[0])
    return True


def main() -> None:
    located = locate_functions()
    print("LOCATED=" + json.dumps({k: {"module": v.module, "signature": str(inspect.signature(v.function))} for k, v in located.items()}, sort_keys=True))
    missing = sorted(TARGET_NAMES - located.keys())
    print("MISSING=" + json.dumps(missing))
    if "validate_tx_generator" not in located:
        raise RuntimeError("validate_tx_generator was not located")

    hard_fork = int(getattr(DEFAULT_CONSTANTS, "HARD_FORK2_HEIGHT", 0))
    heights = sorted({0, max(0, hard_fork - 1), hard_fork, hard_fork + 1})

    valid_program = SerializedProgram.from_bytes(b"\x80")
    quote_program = SerializedProgram.from_bytes(b"\x01")
    legacy_values = [None, valid_program, quote_program]
    buffer_values = [None, b"", b"\x80", b"\x01", b"\xff", b"\x80\x00", b"\x00\x00\x00\x01\x80"]

    try:
        from chia_rs.sized_ints import uint8
        versions = [None, uint8(0), uint8(1), uint8(2), uint8(255)]
    except Exception:
        versions = [None, 0, 1, 2, 255]

    cases: list[dict[str, Any]] = []
    divergences: list[dict[str, Any]] = []

    for height, version, legacy, buffer in itertools.product(heights, versions, legacy_values, buffer_values):
        obj = SimpleNamespace(
            version=version,
            transactions_generator=legacy,
            transactions_generator_buffer=buffer,
        )
        results = {
            name: invoke(fn, obj, height=height, version=version, legacy=legacy, buffer=buffer)
            for name, fn in located.items()
        }
        valid = accepted(results["validate_tx_generator"])
        case = {
            "height": height,
            "hard_fork2_height": hard_fork,
            "version": None if version is None else int(version),
            "legacy": norm(legacy),
            "buffer": norm(buffer),
            "valid": valid,
            "results": {k: [v[0], norm(v[1])] for k, v in results.items()},
        }
        cases.append(case)

        if not valid:
            continue

        # All accepted states must have coherent helper interpretations.
        has = results.get("block_has_transactions_generator")
        get_bytes = results.get("get_transactions_generator_bytes")
        get_program = results.get("get_transactions_generator_program")

        reasons: list[str] = []
        if has and has[0] == "error":
            reasons.append("accepted-state-has-helper-errors")
        if get_bytes and get_bytes[0] == "error":
            reasons.append("accepted-state-bytes-helper-errors")
        if get_program and get_program[0] == "error":
            reasons.append("accepted-state-program-helper-errors")

        if has and has[0] == "ok":
            expected_presence = legacy is not None or buffer not in (None, b"")
            if bool(has[1]) != expected_presence:
                reasons.append("presence-helper-disagrees-with-populated-fields")

        if get_bytes and get_bytes[0] == "ok" and get_program and get_program[0] == "ok":
            bytes_value = get_bytes[1]
            program_value = get_program[1]
            if bytes_value is None and program_value is not None:
                reasons.append("program-present-but-bytes-absent")
            elif bytes_value is not None and program_value is None:
                reasons.append("bytes-present-but-program-absent")
            elif bytes_value is not None and program_value is not None:
                try:
                    if bytes(bytes_value) != bytes(program_value):
                        reasons.append("bytes-and-program-disagree")
                except Exception as error:
                    reasons.append(f"accepted-state-not-byte-comparable:{type(error).__name__}")

        # Version/fork invariants from the new-format design.
        v = None if version is None else int(version)
        post = height >= hard_fork
        if post and v == 0 and buffer not in (None, b""):
            reasons.append("post-fork-v0-buffer-accepted")
        if post and v == 1 and legacy is not None:
            reasons.append("post-fork-v1-legacy-field-accepted")
        if legacy is not None and buffer not in (None, b""):
            reasons.append("dual-generator-representations-accepted")
        if v not in (None, 0, 1):
            reasons.append("unknown-version-accepted")

        if reasons:
            item = {**case, "reasons": reasons}
            divergences.append(item)
            print("DIVERGENCE=" + json.dumps(item, sort_keys=True))

    Path("GENERATOR_VERSION_CASES.json").write_text(json.dumps(cases, indent=2, sort_keys=True))
    Path("GENERATOR_VERSION_DIVERGENCES.json").write_text(json.dumps(divergences, indent=2, sort_keys=True))
    print(f"CASES={len(cases)}")
    print(f"ACCEPTED={sum(1 for c in cases if c['valid'])}")
    print(f"DIVERGENCES={len(divergences)}")
    if divergences:
        print("RESULT=VERSIONED_GENERATOR_STATE_INTERPRETATION_DIVERGENCE")
    else:
        print("RESULT=NO_VERSIONED_GENERATOR_STATE_DIVERGENCE")


if __name__ == "__main__":
    main()

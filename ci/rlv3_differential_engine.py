from __future__ import annotations

import dataclasses
import importlib
import inspect
import json
import sys
from collections.abc import Mapping
from enum import Enum
from typing import Any


RESULTS: list[dict[str, Any]] = []


def load() -> tuple[Any, Any, Any, Any, Any]:
    rate_limiter_mod = importlib.import_module("chia.server.rate_limiter")
    rl3_mod = importlib.import_module("chia.server.rate_limits_v3")
    protocol_mod = importlib.import_module("chia.protocols.protocol_message_types")
    outbound_mod = importlib.import_module("chia.server.outbound_message")
    shared_mod = importlib.import_module("chia.protocols.shared_protocol")
    return (
        rate_limiter_mod.RateLimiter,
        rl3_mod,
        protocol_mod.ProtocolMessageTypes,
        outbound_mod.Message,
        shared_mod,
    )


def primitive(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if dataclasses.is_dataclass(value):
        return {f.name: primitive(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(primitive(k)): primitive(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [primitive(x) for x in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "__dict__"):
        return {k: primitive(v) for k, v in vars(value).items() if not k.startswith("_")}
    return repr(value)


def locate_limit_mapping(module: Any, protocol_enum: Any) -> Mapping[Any, Any]:
    candidates: list[tuple[int, str, Mapping[Any, Any]]] = []
    enum_values = set(protocol_enum)
    enum_numbers = {int(x.value) for x in protocol_enum}
    enum_names = {x.name for x in protocol_enum}
    for name, obj in vars(module).items():
        if not isinstance(obj, Mapping):
            continue
        score = 0
        for key in obj.keys():
            if key in enum_values:
                score += 4
            elif isinstance(key, int) and key in enum_numbers:
                score += 3
            elif isinstance(key, str) and key in enum_names:
                score += 2
        if score:
            candidates.append((score, name, obj))
    if not candidates:
        raise RuntimeError("No RATE_LIMITS_V3 protocol mapping discovered")
    candidates.sort(reverse=True, key=lambda x: x[0])
    score, name, mapping = candidates[0]
    print(f"DISCOVERED_MAPPING={name} score={score} entries={len(mapping)}")
    return mapping


def field(settings: Any, *names: str) -> int | None:
    for name in names:
        if isinstance(settings, Mapping) and name in settings:
            value = settings[name]
        elif hasattr(settings, name):
            value = getattr(settings, name)
        else:
            continue
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


def lookup(mapping: Mapping[Any, Any], msg_type: Any) -> Any | None:
    for key in (msg_type, msg_type.value, int(msg_type.value), msg_type.name):
        if key in mapping:
            return mapping[key]
    return None


def construct_limiter(cls: Any, incoming: bool = True, percentage: int = 100) -> Any:
    sig = inspect.signature(cls)
    kwargs: dict[str, Any] = {}
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        if name in {"incoming", "is_incoming"}:
            kwargs[name] = incoming
        elif name in {"percentage_of_limit", "percentage", "limit_percentage"}:
            kwargs[name] = percentage
        elif name in {"reset_seconds", "window_seconds"}:
            kwargs[name] = 10_000
        elif param.default is inspect.Parameter.empty:
            raise RuntimeError(f"Unknown required RateLimiter constructor argument: {name}")
    return cls(**kwargs)


def make_message(Message: Any, msg_type: Any, data: bytes, message_id: int | None = None) -> Any:
    # Chia Message normally accepts (uint8 type, Optional[uint16] id, bytes data).
    value = int(msg_type.value)
    attempts = [
        (value, message_id, data),
        (msg_type, message_id, data),
    ]
    try:
        sized = importlib.import_module("chia_rs.sized_ints")
        attempts.insert(0, (sized.uint8(value), None if message_id is None else sized.uint16(message_id), data))
    except Exception:
        pass
    errors: list[str] = []
    for args in attempts:
        try:
            return Message(*args)
        except Exception as error:
            errors.append(f"{args[0]!r}: {error!r}")
    raise RuntimeError("Unable to construct Message: " + " | ".join(errors))


def capability_variants(shared_mod: Any) -> list[Any]:
    variants: list[Any] = [None, [], set()]
    Capability = getattr(shared_mod, "Capability", None)
    if Capability is None:
        return variants
    members = list(Capability)
    for cap in members:
        if "RATE" not in cap.name.upper():
            continue
        variants.extend([[cap], {cap}])
        try:
            variants.append([(int(cap.value), "1")])
        except Exception:
            pass
    rate_caps = [c for c in members if "RATE" in c.name.upper()]
    if rate_caps:
        variants.extend([rate_caps, set(rate_caps)])
    return variants


def call_process(limiter: Any, message: Any, capability_options: list[Any]) -> tuple[bool, Any]:
    method = limiter.process_msg_and_check
    sig = inspect.signature(method)
    errors: list[str] = []
    for caps in capability_options:
        args: list[Any] = []
        kwargs: dict[str, Any] = {}
        viable = True
        for name, param in sig.parameters.items():
            if name in {"message", "msg"}:
                kwargs[name] = message
            elif "capabil" in name:
                kwargs[name] = caps
            elif param.default is inspect.Parameter.empty:
                viable = False
                break
        if not viable:
            continue
        try:
            return bool(method(**kwargs)), caps
        except Exception as error:
            errors.append(f"caps={caps!r}: {type(error).__name__}: {error}")
    # Common positional forms as fallback.
    for caps in capability_options:
        for args in ((message,), (message, caps)):
            try:
                return bool(method(*args)), caps
            except Exception as error:
                errors.append(f"args={args!r}: {type(error).__name__}: {error}")
    raise RuntimeError("Unable to invoke process_msg_and_check: " + " | ".join(errors[-20:]))


def fresh_call(cls: Any, Message: Any, shared_mod: Any, msg_type: Any, size: int, count: int, message_id: int | None = None) -> tuple[list[bool], Any]:
    limiter = construct_limiter(cls)
    caps_variants = capability_variants(shared_mod)
    decisions: list[bool] = []
    selected_caps: Any = None
    payload = b"A" * size
    for _ in range(count):
        decision, selected_caps = call_process(limiter, make_message(Message, msg_type, payload, message_id), caps_variants)
        decisions.append(decision)
    return decisions, selected_caps


def record(kind: str, msg_type: Any, settings: Any, expected: Any, observed: Any, **extra: Any) -> None:
    item = {
        "kind": kind,
        "message": msg_type.name,
        "value": int(msg_type.value),
        "settings": primitive(settings),
        "expected": expected,
        "observed": observed,
        **extra,
    }
    RESULTS.append(item)
    print("DIVERGENCE=" + json.dumps(item, sort_keys=True))


def main() -> int:
    RateLimiter, rl3_mod, ProtocolMessageTypes, Message, shared_mod = load()
    mapping = locate_limit_mapping(rl3_mod, ProtocolMessageTypes)
    print("RATE_LIMITER_SIGNATURE=" + str(inspect.signature(RateLimiter)))
    print("PROCESS_SIGNATURE=" + str(inspect.signature(RateLimiter.process_msg_and_check)))

    exercised = 0
    selected_capabilities: dict[str, str] = {}

    for msg_type in ProtocolMessageTypes:
        settings = lookup(mapping, msg_type)
        if settings is None:
            continue
        maximum_size = field(settings, "max_size", "max_message_size")
        frequency = field(settings, "frequency", "max_count", "count")
        max_total = field(settings, "max_total_size", "max_total_bytes", "total_size")

        # Size boundary: exactly max must pass and max+1 must fail when max_size is declared.
        if maximum_size is not None and 0 < maximum_size <= 20_000_000:
            exercised += 1
            decisions, caps = fresh_call(RateLimiter, Message, shared_mod, msg_type, maximum_size, 1)
            selected_capabilities[msg_type.name] = repr(caps)
            if decisions != [True]:
                record("declared-max-size-rejected", msg_type, settings, [True], decisions, size=maximum_size)
            decisions, _ = fresh_call(RateLimiter, Message, shared_mod, msg_type, maximum_size + 1, 1)
            if decisions != [False]:
                record("max-size-plus-one-accepted", msg_type, settings, [False], decisions, size=maximum_size + 1)

        # Frequency boundary at a one-byte payload, capped to keep CI bounded.
        if frequency is not None and 0 < frequency <= 20_000:
            exercised += 1
            decisions, caps = fresh_call(RateLimiter, Message, shared_mod, msg_type, 1, frequency + 1)
            selected_capabilities[msg_type.name] = repr(caps)
            expected = [True] * frequency + [False]
            if decisions != expected:
                record("frequency-boundary-mismatch", msg_type, settings, expected[-5:], decisions[-5:], frequency=frequency, accepted=sum(decisions))

        # Total-byte boundary. Use deterministic equal chunks where possible.
        if max_total is not None and max_total > 0 and max_total <= 100_000_000:
            chunk = maximum_size if maximum_size and maximum_size > 0 else min(max_total, 1_000_000)
            chunk = max(1, min(chunk, max_total))
            full = max_total // chunk
            remainder = max_total % chunk
            count = full + (1 if remainder else 0)
            if count <= 20_000:
                exercised += 1
                limiter = construct_limiter(RateLimiter)
                caps_variants = capability_variants(shared_mod)
                decisions: list[bool] = []
                caps: Any = None
                for index in range(count):
                    size = remainder if remainder and index == count - 1 else chunk
                    decision, caps = call_process(limiter, make_message(Message, msg_type, b"B" * size), caps_variants)
                    decisions.append(decision)
                extra, _ = call_process(limiter, make_message(Message, msg_type, b"C"), caps_variants)
                if not all(decisions) or extra:
                    record(
                        "total-byte-boundary-mismatch",
                        msg_type,
                        settings,
                        {"through_limit": True, "plus_one": False},
                        {"through_limit": decisions[-10:], "plus_one": extra},
                        max_total=max_total,
                        chunk=chunk,
                    )

        # Message-ID semantics must not alter byte/count decisions.
        if maximum_size is not None and 0 < maximum_size <= 1_000_000:
            exercised += 1
            baseline, _ = fresh_call(RateLimiter, Message, shared_mod, msg_type, min(maximum_size, 16), 2, None)
            for message_id in (0, 1, 65535):
                decisions, _ = fresh_call(RateLimiter, Message, shared_mod, msg_type, min(maximum_size, 16), 2, message_id)
                if decisions != baseline:
                    record("message-id-changes-limiter-decision", msg_type, settings, baseline, decisions, message_id=message_id)

    print(f"EXERCISED_BOUNDARIES={exercised}")
    print(f"DIVERGENCE_COUNT={len(RESULTS)}")
    Path = __import__("pathlib").Path
    Path("RLV3_DIFFERENTIAL_RESULTS.json").write_text(json.dumps({"exercised": exercised, "divergences": RESULTS, "capabilities": selected_capabilities}, indent=2, sort_keys=True))
    if RESULTS:
        print("RESULT=RATE_LIMITS_V3_DECLARATION_IMPLEMENTATION_DIVERGENCE")
        return 0
    print("RESULT=NO_DECLARATION_IMPLEMENTATION_DIVERGENCE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

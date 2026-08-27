from __future__ import annotations

import importlib
import inspect
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass
class Finding:
    message: str
    value: int
    local_caps: list[str]
    peer_caps: list[str]
    strong_accepts: int
    downgraded_accepts: int
    probe_count: int
    reason: str


def name_cap(c: Any) -> str:
    return getattr(c, "name", repr(c))


def load_symbols():
    rl = importlib.import_module("chia.server.rate_limiter")
    proto = importlib.import_module("chia.protocols.protocol_message_types")
    shared = importlib.import_module("chia.protocols.shared_protocol")
    outbound = importlib.import_module("chia.server.outbound_message")
    return rl, proto.ProtocolMessageTypes, shared.Capability, outbound.Message


def make_message(Message: Any, msg_type: Any, size: int):
    data = b"X" * size
    try:
        ints = importlib.import_module("chia_rs.sized_ints")
        return Message(ints.uint8(int(msg_type.value)), None, data)
    except Exception:
        return Message(int(msg_type.value), None, data)


def limiter_instance(RateLimiter: Any):
    sig = inspect.signature(RateLimiter)
    kwargs = {}
    for name, parameter in sig.parameters.items():
        if name == "self":
            continue
        lower = name.lower()
        if "incoming" in lower:
            kwargs[name] = True
        elif "percentage" in lower:
            kwargs[name] = 100
        elif "reset" in lower or "window" in lower:
            kwargs[name] = 10_000
        elif parameter.default is inspect.Parameter.empty:
            raise RuntimeError(f"unknown RateLimiter argument: {name}")
    return RateLimiter(**kwargs)


def invoke(limiter: Any, message: Any, local_caps: list[Any], peer_caps: list[Any]):
    method = limiter.process_msg_and_check
    signature = inspect.signature(method)
    kwargs = {}
    for name, parameter in signature.parameters.items():
        lower = name.lower()
        if lower in {"message", "msg"}:
            kwargs[name] = message
        elif "our" in lower and "cap" in lower:
            kwargs[name] = local_caps
        elif "peer" in lower and "cap" in lower:
            kwargs[name] = peer_caps
        elif "cap" in lower:
            kwargs[name] = peer_caps
        elif parameter.default is inspect.Parameter.empty:
            break
    else:
        try:
            return bool(method(**kwargs))
        except Exception:
            pass
    errors = []
    for args in ((message, local_caps, peer_caps), (message, peer_caps), (message,)):
        try:
            return bool(method(*args))
        except Exception as error:
            errors.append(f"{type(error).__name__}: {error}")
    raise RuntimeError("unable to invoke limiter: " + " | ".join(errors))


def run_sequence(RateLimiter: Any, Message: Any, msg_type: Any, local_caps: list[Any], peer_caps: list[Any], count: int):
    limiter = limiter_instance(RateLimiter)
    return [invoke(limiter, make_message(Message, msg_type, 1), local_caps, peer_caps) for _ in range(count)]


def variants(Capability: Any):
    rate_caps = [capability for capability in Capability if "RATE" in capability.name.upper()]
    candidates = [[], rate_caps]
    candidates.extend([[capability] for capability in rate_caps])
    candidates.extend(rate_caps[:index] for index in range(len(rate_caps) + 1))
    candidates.extend([candidate for candidate in rate_caps if candidate != omitted] for omitted in rate_caps)
    unique = []
    seen = set()
    for candidate in candidates:
        key = tuple(sorted(int(capability.value) for capability in candidate))
        if key not in seen:
            seen.add(key)
            unique.append(candidate)
    return rate_caps, unique


def find_first_reject(RateLimiter: Any, Message: Any, msg_type: Any, local_caps: list[Any], peer_caps: list[Any], maximum: int = 5_000):
    limiter = limiter_instance(RateLimiter)
    for index in range(1, maximum + 1):
        if not invoke(limiter, make_message(Message, msg_type, 1), local_caps, peer_caps):
            return index
    return None


def main() -> None:
    rate_limiter_module, ProtocolMessageTypes, Capability, Message = load_symbols()
    RateLimiter = rate_limiter_module.RateLimiter
    rate_caps, peer_variants = variants(Capability)
    strong = rate_caps
    print("RATE_CAPABILITIES=" + json.dumps([name_cap(capability) for capability in rate_caps]))
    print("PROCESS_SIGNATURE=" + str(inspect.signature(RateLimiter.process_msg_and_check)))

    interesting_names = {
        "request_transaction", "respond_transaction", "new_transaction", "send_transaction",
        "request_block", "respond_block", "request_blocks", "respond_blocks",
        "request_coin_state", "respond_to_coin_updates", "register_for_coin_updates",
        "register_for_ph_updates", "respond_to_ph_updates", "mempool_updates",
    }
    messages = [message for message in ProtocolMessageTypes if message.name in interesting_names]
    findings: list[Finding] = []
    matrix = []

    for message_type in messages:
        first_reject = find_first_reject(RateLimiter, Message, message_type, strong, strong)
        probe = 512 if first_reject is None else min(max(first_reject + 5, 20), 5_000)
        strong_decisions = run_sequence(RateLimiter, Message, message_type, strong, strong, probe)
        strong_accepts = sum(strong_decisions)
        for peer_caps in peer_variants:
            decisions = run_sequence(RateLimiter, Message, message_type, strong, peer_caps, probe)
            accepted = sum(decisions)
            row = {
                "message": message_type.name,
                "value": int(message_type.value),
                "local_caps": [name_cap(capability) for capability in strong],
                "peer_caps": [name_cap(capability) for capability in peer_caps],
                "probe": probe,
                "strong_accepts": strong_accepts,
                "variant_accepts": accepted,
                "strong_first_reject": first_reject,
            }
            matrix.append(row)
            if accepted > strong_accepts and first_reject is not None:
                finding = Finding(
                    message=message_type.name,
                    value=int(message_type.value),
                    local_caps=[name_cap(capability) for capability in strong],
                    peer_caps=[name_cap(capability) for capability in peer_caps],
                    strong_accepts=strong_accepts,
                    downgraded_accepts=accepted,
                    probe_count=probe,
                    reason="peer capability omission increases accepted message budget",
                )
                findings.append(finding)
                print("DOWNGRADE=" + json.dumps(asdict(finding), sort_keys=True))

    source_hits = {}
    for module_name, needles in {
        "chia.full_node.full_node_api": ["request_transaction", "respond_transaction"],
        "chia.server.ws_connection": ["process_msg_and_check", "rate_limiter", "peer_capabilities"],
    }.items():
        try:
            module = importlib.import_module(module_name)
            source = inspect.getsource(module)
            source_hits[module_name] = {needle: needle in source for needle in needles}
            Path(module_name.replace(".", "_") + ".source.txt").write_text(source)
        except Exception as error:
            source_hits[module_name] = {"error": repr(error)}

    output = {
        "rate_capabilities": [name_cap(capability) for capability in rate_caps],
        "findings": [asdict(finding) for finding in findings],
        "matrix": matrix,
        "source_hits": source_hits,
    }
    Path("RLV3_CAPABILITY_DOWNGRADE_RESULTS.json").write_text(json.dumps(output, indent=2, sort_keys=True))
    security_names = {
        "request_transaction", "respond_transaction", "send_transaction",
        "register_for_coin_updates", "register_for_ph_updates", "request_coin_state",
    }
    security_relevant = [finding for finding in findings if finding.message in security_names]
    print("SOURCE_HITS=" + json.dumps(source_hits, sort_keys=True))
    print(f"FINDING_COUNT={len(findings)}")
    print(f"SECURITY_RELEVANT_COUNT={len(security_relevant)}")
    if security_relevant:
        print("RESULT=PEER_CONTROLLED_RATE_LIMIT_CAPABILITY_DOWNGRADE")
    elif findings:
        print("RESULT=CAPABILITY_DOWNGRADE_ONLY_NONCRITICAL_MESSAGES")
    else:
        print("RESULT=NO_CAPABILITY_DOWNGRADE")


if __name__ == "__main__":
    main()

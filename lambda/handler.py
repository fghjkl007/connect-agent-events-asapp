"""Kinesis consumer for Amazon Connect Agent Event Stream.

Applies the filter chain in README.md and emits start/stop decisions.
Replace `dispatch` body with real ASAPP API calls plus an idempotency guard.
"""
import base64
import json
import logging
from typing import Iterable, Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ALLOWED_EVENT_TYPE = "STATE_CHANGE"
ALLOWED_CHANNEL = "VOICE"
ALLOWED_INITIATION = frozenset({"INBOUND", "TRANSFER", "QUEUE_TRANSFER"})
TALKING_STATES = frozenset({"CONNECTED", "CONNECTED_ONHOLD"})
STATE_GONE = "ENDED"


def handler(event: dict, _ctx) -> None:
    for record in event.get("Records", []):
        payload = json.loads(base64.b64decode(record["kinesis"]["data"]))
        for decision in decide(payload):
            dispatch(decision)


def decide(ev: dict) -> Iterable[dict]:
    """Yield {'action', 'contactId', 'initialContactId', 'agent', 'ts'}."""
    if ev.get("EventType") != ALLOWED_EVENT_TYPE:
        return
    prev = _index(ev.get("PreviousAgentSnapshot", {}))
    curr = _index(ev.get("CurrentAgentSnapshot", {}))
    agent = ev.get("CurrentAgentSnapshot", {}).get("Configuration", {}).get("Username")

    for cid in set(prev) | set(curr):
        contact = curr.get(cid) or prev[cid]
        if not _is_relevant(contact):
            continue
        before = prev.get(cid, {}).get("State")
        after = curr.get(cid, {}).get("State", STATE_GONE)
        action = _action(before, after)
        if action is None:
            continue
        yield {
            "action": action,
            "contactId": cid,
            "initialContactId": contact.get("InitialContactId") or cid,
            "agent": agent,
            "ts": contact.get("StateStartTimestamp"),
        }


def _index(snapshot: dict) -> dict:
    return {c["ContactId"]: c for c in snapshot.get("Contacts", [])}


def _is_relevant(contact: dict) -> bool:
    return (
        contact.get("Channel") == ALLOWED_CHANNEL
        and contact.get("InitiationMethod") in ALLOWED_INITIATION
    )


def _action(before: Optional[str], after: str) -> Optional[str]:
    if before == after:
        return None
    if after == "CONNECTED":
        return "start"
    if after == "CONNECTED_ONHOLD":
        return "stop"
    if after == STATE_GONE and before in TALKING_STATES:
        return "stop"
    return None


def dispatch(decision: dict) -> None:
    logger.info("decision %s", json.dumps(decision))
    # TODO: DynamoDB conditional write on `ts` for idempotency, then
    # call ASAPP /start-streaming or /stop-streaming.

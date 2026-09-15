import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambda"))
from handler import decide  # noqa: E402

TS = "2026-09-14T16:07:45.812Z"


def _ev(prev_state, curr_state, init="INBOUND", channel="VOICE", cid="c1"):
    def snap(state):
        if state is None:
            return {"Contacts": []}
        return {"Contacts": [{
            "ContactId": cid, "Channel": channel, "InitiationMethod": init,
            "State": state, "StateStartTimestamp": TS,
        }]}
    return {
        "EventType": "STATE_CHANGE",
        "PreviousAgentSnapshot": snap(prev_state),
        "CurrentAgentSnapshot": {**snap(curr_state), "Configuration": {"Username": "agent1"}},
    }


def _actions(ev):
    return [d["action"] for d in decide(ev)]


def test_first_connect_starts():
    assert _actions(_ev("CONNECTING", "CONNECTED")) == ["start"]


def test_hold_stops():
    assert _actions(_ev("CONNECTED", "CONNECTED_ONHOLD")) == ["stop"]


def test_resume_starts():
    assert _actions(_ev("CONNECTED_ONHOLD", "CONNECTED")) == ["start"]


def test_end_from_talking_stops():
    assert _actions(_ev("CONNECTED", "ENDED")) == ["stop"]


def test_contact_disappears_stops():
    assert _actions(_ev("CONNECTED", None)) == ["stop"]


def test_end_without_talking_ignored():
    assert _actions(_ev("INCOMING", "ENDED")) == []


def test_same_state_ignored():
    assert _actions(_ev("CONNECTED", "CONNECTED")) == []


def test_outbound_ignored():
    assert _actions(_ev("CONNECTING", "CONNECTED", init="OUTBOUND")) == []


def test_chat_ignored():
    assert _actions(_ev("CONNECTING", "CONNECTED", channel="CHAT")) == []


def test_transfer_allowed():
    assert _actions(_ev("CONNECTING", "CONNECTED", init="TRANSFER")) == ["start"]


def test_heartbeat_ignored():
    ev = _ev("CONNECTED", "CONNECTED_ONHOLD")
    ev["EventType"] = "HEART_BEAT"
    assert _actions(ev) == []

from __future__ import annotations

from pathlib import Path

import jsonschema
import pytest

from knx_nats_bridge.writer_rules import WriterRules, extract_value


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "writer-rules.yaml"
    p.write_text(body, encoding="utf-8")
    return p


def test_loads_valid_mapping(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "unifi.events.fassade.person"
            ga: "14/3/1"
            dpt: "1.005"
            payload_path: "$.score"
            description: "Person at fassade"
          - subject: "ems-esp.boiler_data"
            ga: "15/2/1"
            dpt: "1.001"
            payload_path: "$.burnstart_active"
        """,
    )
    table = WriterRules.load(path)
    assert len(table) == 2
    assert set(table.subjects()) == {"unifi.events.fassade.person", "ems-esp.boiler_data"}
    [m] = table.for_subject("unifi.events.fassade.person")
    assert m.ga == "14/3/1"
    assert m.dpt == "1.005"
    assert m.payload_path == "$.score"
    assert m.description == "Person at fassade"


def test_loads_deadband_fields(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "solaredge-1.powerflow"
            ga: "15/4/0"
            dpt: "14.056"
            payload_path: "$.grid.power"
            min_delta: 25
            min_delta_pct: 2
          - subject: "warp.evse.state"
            ga: "15/6/0"
            dpt: "5.010"
            payload_path: "$.charger_state"
            min_delta: 0
        """,
    )
    table = WriterRules.load(path)
    [pv] = table.for_subject("solaredge-1.powerflow")
    assert pv.min_delta == 25
    assert pv.min_delta_pct == 2
    [evse] = table.for_subject("warp.evse.state")
    assert evse.min_delta == 0
    assert evse.min_delta_pct is None


def test_deadband_fields_optional(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "unifi.events.fassade.person"
            ga: "14/3/1"
            dpt: "1.005"
            payload_path: "$.knx_value"
        """,
    )
    [m] = WriterRules.load(path).for_subject("unifi.events.fassade.person")
    assert m.min_delta is None
    assert m.min_delta_pct is None


def test_seed_on_start_parsed_and_defaults_false(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "warp.evse.state"
            ga: "15/6/0"
            dpt: "5.010"
            payload_path: "$.error_state"
            seed_on_start: true
          - subject: "warp.evse.state"
            ga: "15/6/2"
            dpt: "5.010"
            payload_path: "$.charger_state"
          - subject: "wallbox.knx.meter"
            ga: "15/6/10"
            dpt: "14.056"
            payload_path: "$.power"
        """,
    )
    table = WriterRules.load(path)
    by_ga = {r.ga: r for r in table}
    assert by_ga["15/6/0"].seed_on_start is True
    assert by_ga["15/6/2"].seed_on_start is False  # default
    assert by_ga["15/6/10"].seed_on_start is False
    # A subject is seedable if ANY of its rules opts in.
    assert table.seed_subjects() == ["warp.evse.state"]


def test_rejects_unknown_rule_key(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "x"
            ga: "1/2/3"
            dpt: "14.056"
            payload_path: "$.power"
            bogus_key: true
        """,
    )
    with pytest.raises(jsonschema.ValidationError):
        WriterRules.load(path)


def test_rejects_negative_min_delta(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "x"
            ga: "1/2/3"
            dpt: "14.056"
            payload_path: "$.power"
            min_delta: -1
        """,
    )
    with pytest.raises(jsonschema.ValidationError):
        WriterRules.load(path)


def test_fan_out_multiple_mappings_per_subject(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "ems-esp.boiler_data"
            ga: "15/2/1"
            dpt: "1.001"
            payload_path: "$.burnstart_active"
          - subject: "ems-esp.boiler_data"
            ga: "15/2/2"
            dpt: "9.001"
            payload_path: "$.curflowtemp"
        """,
    )
    table = WriterRules.load(path)
    assert len(table.for_subject("ems-esp.boiler_data")) == 2


def test_rejects_invalid_ga_format(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "x"
            ga: "14.3.1"
            dpt: "1.001"
            payload_path: "$.foo"
        """,
    )
    with pytest.raises(jsonschema.ValidationError):
        WriterRules.load(path)


def test_rejects_invalid_dpt_format(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "x"
            ga: "1/2/3"
            dpt: "binary"
            payload_path: "$.foo"
        """,
    )
    with pytest.raises(jsonschema.ValidationError):
        WriterRules.load(path)


def test_rejects_unknown_dpt(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "x"
            ga: "1/2/3"
            dpt: "999.999"
            payload_path: "$.foo"
        """,
    )
    with pytest.raises(ValueError, match="unknown DPT"):
        WriterRules.load(path)


def test_rejects_invalid_payload_path(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "x"
            ga: "1/2/3"
            dpt: "1.001"
            payload_path: "score"
        """,
    )
    with pytest.raises(jsonschema.ValidationError):
        WriterRules.load(path)


def test_rejects_unknown_field(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "x"
            ga: "1/2/3"
            dpt: "1.001"
            payload_path: "$.foo"
            transform: "negate"
        """,
    )
    with pytest.raises(jsonschema.ValidationError):
        WriterRules.load(path)


def test_rejects_loop_subject(tmp_path: Path) -> None:
    # Subject under the reader prefix would echo back via the KNX bus.
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "knx.14.3.1"
            ga: "14/3/1"
            dpt: "1.001"
            payload_path: "$.value"
        """,
    )
    with pytest.raises(ValueError, match="reader prefix"):
        WriterRules.load(path, reader_subject_prefix="knx")


def test_allows_subject_without_reader_prefix_overlap(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        mappings:
          - subject: "anomaly.foo.warning"
            ga: "15/0/1"
            dpt: "1.001"
            payload_path: "$.firing"
        """,
    )
    table = WriterRules.load(path, reader_subject_prefix="knx")
    assert len(table) == 1


def test_extract_value_root() -> None:
    assert extract_value({"a": 1}, "$") == {"a": 1}


def test_extract_value_nested() -> None:
    assert extract_value({"a": {"b": {"c": 42}}}, "$.a.b.c") == 42


def test_extract_value_missing_key_raises() -> None:
    with pytest.raises(KeyError):
        extract_value({"a": 1}, "$.b")


def test_extract_value_descends_into_scalar_raises() -> None:
    with pytest.raises(KeyError):
        extract_value({"a": 1}, "$.a.b")

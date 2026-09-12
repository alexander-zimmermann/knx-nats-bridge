"""Unit tests for knxproj-link's XML rewriting (no archive round trip)."""

from __future__ import annotations

import pytest

from knx_nats_bridge.tools.knxproj_to_kaenx import Collector, parse_device_spec
from knx_nats_bridge.tools.link_knxproj import (
    group_address_ids,
    installed_objects,
    kaenx_encode,
    link_device,
    verify_installed,
)

_PROJECT = """<KNX>
  <GroupAddresses>
    <GroupAddress Id="P-1-0_GA-10" Address="252" Name="A" />
    <GroupAddress Id="P-1-0_GA-11" Address="2313" Name="B" />
    <GroupAddress Id="P-1-0_GA-12" Address="4352" Name="C" />
  </GroupAddresses>
  <DeviceInstance Id="P-1-0_DI-9" Address="163"
    ProductRefId="M-00FA_H-BASALTE.2DCORE.2DS4-1_P-BASALTE.2DCORE.2DS4"
    Hardware2ProgramRefId="M-00FA_H-BASALTE.2DCORE.2DS4-1_HP-AF65-11-3620">
    <GroupObjectTree>
      <Nodes />
    </GroupObjectTree>
    <Security />
  </DeviceInstance>
  <DeviceInstance Id="P-1-0_DI-8" Address="162"
    ProductRefId="M-00FA_H-KNX.2DNATS.2DBRIDGE-1_P-KNX.2DNATS.2DBRIDGE"
    Hardware2ProgramRefId="M-00FA_H-KNX.2DNATS.2DBRIDGE-1_HP-AF64-11-3167">
    <ComObjectInstanceRefs>
      <ComObjectInstanceRef RefId="O-1_R-1" Links="GA-99 GA-98" />
    </ComObjectInstanceRefs>
    <GroupObjectTree />
  </DeviceInstance>
</KNX>
"""


def test_kaenx_encode() -> None:
    assert kaenx_encode("KNX-NATS-BRIDGE") == "KNX.2DNATS.2DBRIDGE"
    assert kaenx_encode("PLAIN9") == "PLAIN9"


def test_group_address_ids_decode_raw_addresses() -> None:
    assert group_address_ids(_PROJECT) == {"0/0/252": "GA-10", "1/1/9": "GA-11", "2/1/0": "GA-12"}


def _collectors() -> list[Collector]:
    a = Collector(main_group=0, dpt_main=5, dpt_sub=10, direction="both")
    a.entries = [("0/0/252", "A")]
    b = Collector(main_group=1, dpt_main=1, dpt_sub=1, direction="both")
    b.entries = [("1/1/9", "B"), ("2/1/0", "C"), ("7/7/7", "not in ETS")]
    return [a, b]


def test_link_device_inserts_before_group_object_tree() -> None:
    spec = parse_device_spec("@x=Basalte Core S4:both", 101)
    xml, report = link_device(_PROJECT, spec, _collectors(), group_address_ids(_PROJECT))
    assert report.device_found and report.objects == 2 and report.links == 3
    assert report.replaced == 0
    device = xml[xml.index('Address="163"') : xml.index('Address="162"')]
    assert (
        "    <ComObjectInstanceRefs>\n"
        '      <ComObjectInstanceRef RefId="O-1_R-1" Links="GA-10" />\n'
        '      <ComObjectInstanceRef RefId="O-2_R-2" Links="GA-11 GA-12" />\n'
        "    </ComObjectInstanceRefs>\n"
        "    <GroupObjectTree>"
    ) in device
    # The other device is untouched.
    assert 'Links="GA-99 GA-98"' in xml


def test_link_device_replaces_existing_links() -> None:
    spec = parse_device_spec("@x=KNX-NATS-Bridge:split", 100)
    xml, report = link_device(_PROJECT, spec, _collectors()[:1], group_address_ids(_PROJECT))
    assert report.replaced == 2
    assert 'Links="GA-99 GA-98"' not in xml
    assert xml.count("<ComObjectInstanceRefs>") == 1  # replaced, not added
    assert 'RefId="O-1_R-1" Links="GA-10"' in xml


def test_link_device_unknown_device() -> None:
    spec = parse_device_spec("@x=Ghost:both", 102)
    _, report = link_device(_PROJECT, spec, _collectors(), {})
    assert report.device_found is False


def test_installed_objects_and_verification() -> None:
    app = (
        '<ComObject Id="M-00FA_A-1_O-1" Name="hg0-dpt5.010-both" Number="1" />'
        '<ComObject Id="M-00FA_A-1_O-2" Name="hg1-dpt1.001-both" Number="2" />'
    )
    installed = installed_objects(app)
    assert installed == {1: "hg0-dpt5.010-both", 2: "hg1-dpt1.001-both"}
    verify_installed(_collectors(), installed, "Basalte")

    # A device still on an older cut is refused, naming the difference.
    with pytest.raises(SystemExit, match="object 2: installed 'hg1-dpt1.xxx-both'"):
        verify_installed(_collectors(), {1: "hg0-dpt5.010-both", 2: "hg1-dpt1.xxx-both"}, "B")

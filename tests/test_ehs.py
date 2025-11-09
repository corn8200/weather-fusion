from weatherfusion.processing import ehs


def test_classify_heat_bands():
    cat, guide = ehs.classify_heat(130)
    assert cat == "Extreme Danger"
    assert guide["continuous_heavy_work_min"] == "0"

    cat, guide = ehs.classify_heat(85)
    assert cat == "Caution"
    assert "Normal" in guide["work_rest_min"]

    cat, guide = ehs.classify_heat(70)
    assert cat is None
    assert guide["radio_checkins"] == "start"


def test_classify_freeze_with_wind():
    badge, note = ehs.classify_freeze(27, breezy=True)
    assert badge == "Hard Freeze"
    assert "wind-chill" in note.lower()

    badge, note = ehs.classify_freeze(40, breezy=False)
    assert badge is None and note is None

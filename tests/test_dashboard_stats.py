from run365days.dashboard import stats


def _act(id, date, km, sec, kcal=300, cad=170.0, pace=None):
    return {
        "id": id,
        "date": date,
        "distance_km": km,
        "duration_sec": sec,
        "calories": kcal,
        "avg_cadence": cad,
        "pace_sec_per_km": pace if pace is not None else (int(round(sec / km)) if km else None),
    }


ACTS = [
    _act("a", "2021-01-01", 5.0, 1500),  # 300 s/km
    _act("b", "2021-01-02", 10.0, 3600, kcal=800, cad=180.0),  # 360 s/km
    _act("c", "2021-01-02", 2.0, 400, kcal=None, cad=None),  # same day, too short for fastest
    _act("d", "2021-02-15", 6.0, 1620),  # 270 s/km -> fastest
]


def test_days_in_year():
    assert stats.days_in_year(2021) == 365
    assert stats.days_in_year(2020) == 366


def test_totals():
    t = stats.totals(ACTS, 2021)
    assert t["runs"] == 4
    assert t["days"] == 365
    assert t["active_days"] == 3
    assert t["distance_km"] == 23.0
    assert t["duration_sec"] == 7120
    assert t["calories"] == 1400
    assert t["avg_pace_sec_per_km"] == round(7120 / 23)
    assert t["avg_cadence"] == (170 + 180 + 170) / 3
    assert t["avg_distance_km"] == 5.75


def test_totals_empty():
    t = stats.totals([], 2021)
    assert t["runs"] == 0 and t["avg_pace_sec_per_km"] is None and t["avg_distance_km"] == 0.0


def test_monthly_has_twelve_rows_and_best_pace():
    rows = stats.monthly(ACTS)
    assert len(rows) == 12
    jan, feb, mar = rows[0], rows[1], rows[2]
    assert jan["runs"] == 3 and jan["distance_km"] == 17.0
    assert jan["best_pace_sec_per_km"] == 300 and jan["best_pace_activity_id"] == "a"
    assert feb["best_pace_activity_id"] == "d"
    assert mar["runs"] == 0 and mar["avg_pace_sec_per_km"] is None


def test_week_index_and_weekly():
    # 2021-01-01 is a Friday, so week 0 is Jan 1-3 and week 1 starts Monday Jan 4
    assert stats.week_index(stats.date(2021, 1, 1), 2021) == 0
    assert stats.week_index(stats.date(2021, 1, 3), 2021) == 0
    assert stats.week_index(stats.date(2021, 1, 4), 2021) == 1
    weeks = stats.weekly(ACTS, 2021)
    assert len(weeks) == 53
    assert weeks[0]["week_start"] == "2021-01-01"
    assert weeks[1]["week_start"] == "2021-01-04"
    assert weeks[0]["runs"] == 3 and weeks[0]["distance_km"] == 17.0
    assert weeks[0]["longest_km"] == 10.0
    assert weeks[0]["activity_ids"] == ["a", "b", "c"]
    assert weeks[0]["avg_pace_sec_per_km"] == round(5500 / 17)
    assert weeks[1]["runs"] == 0 and weeks[1]["avg_pace_sec_per_km"] is None


def test_daily_distance_covers_year_and_sums_same_day():
    daily = stats.daily_distance(ACTS, 2021)
    assert len(daily) == 365
    assert daily[0] == {"date": "2021-01-01", "distance_km": 5.0, "activity_id": "a"}
    assert daily[1]["distance_km"] == 12.0 and daily[1]["activity_id"] == "b"
    assert daily[2] == {"date": "2021-01-03", "distance_km": 0.0, "activity_id": None}


def test_training_load_is_exponential_average():
    daily = [{"date": f"d{i}", "distance_km": 42.0} for i in range(3)]
    load = stats.training_load(daily)
    assert load[0]["ctl"] == 1.0  # 42 / 42
    assert load[0]["atl"] == 6.0  # 42 / 7
    assert load[0]["tsb"] == -5.0
    assert load[1]["ctl"] == round(1 + (42 - 1) / 42, 2)


def test_personal_bests():
    pb = stats.personal_bests(ACTS)
    assert pb["longest"]["id"] == "b"
    assert pb["fastest"]["id"] == "d"  # c is faster but under 5 km
    assert pb["longest_time"]["id"] == "b"
    assert pb["most_calories"]["id"] == "b"
    assert pb["top_cadence"]["id"] == "b"


def test_personal_bests_empty():
    assert stats.personal_bests([]) == {
        "longest": None,
        "fastest": None,
        "longest_time": None,
        "most_calories": None,
        "top_cadence": None,
    }

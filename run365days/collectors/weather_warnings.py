"""Fetch weather warnings and tropical cyclone signals from HKO."""
from datetime import datetime
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup

from run365days.models.weather import WeatherWarning

_SIGNALS_URL = "https://www.hko.gov.hk/en/wxinfo/climat/warndb/warndba.shtml"
_HISTORY_URL = "https://www.hko.gov.hk//cgi-bin/climat/warndb_ea.pl"


def _load_signal_metadata() -> Dict[str, dict]:
    """Return {signal_name: {Idx, Type}} from the HKO warnings reference page."""
    bs = BeautifulSoup(requests.get(_SIGNALS_URL).text, "html.parser")
    result = {}
    for table in bs.find_all(class_="self_row2_table"):
        tds = table.find_all("td")
        for img in tds[0].find_all("img"):
            src = img.get("src", "")
            name = img.get("alt", "").lower().title()
            result[name] = {
                "Idx": src[src.rfind("/") + 1 : src.rfind(".")],
                "Type": tds[1].text.strip(),
            }
    return result


def fetch_day(date_str: str, signal_meta: Dict[str, dict]) -> List[WeatherWarning]:
    """Fetch all warning records for *date_str* ('YYYY-MM-DD')."""
    html = requests.get(
        _HISTORY_URL,
        params={"start_ym": datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")},
    ).text

    marker = "Tropical Cyclone Warning_Signals"
    bs = BeautifulSoup(html[html.find(marker) + len(marker) :], "html.parser")

    records = []
    for table in bs.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) != 6:
                continue
            signal = tds[1].text.strip().upper()
            start = str(
                datetime.strptime(
                    tds[3].text.strip() + " " + tds[2].text.strip(), "%d/%b/%Y %H:%M"
                )
            )
            end = str(
                datetime.strptime(
                    tds[5].text.strip() + " " + tds[4].text.strip(), "%d/%b/%Y %H:%M"
                )
            )
            name_key = tds[1].text.strip().lower().title()
            warning_type = signal_meta.get(name_key, {}).get("Type", "Unknown")
            icon = tds[0].find("img").get("src", "")

            records.append(
                WeatherWarning(
                    date=date_str,
                    warning_type=warning_type,
                    warning_signal=signal,
                    start_time=start,
                    end_time=end,
                    icon_url=icon,
                )
            )
    return records


def fetch_range(start_date: str, end_date: str) -> List[WeatherWarning]:
    signal_meta = _load_signal_metadata()
    all_records: List[WeatherWarning] = []
    for d in pd.date_range(start_date, end_date):
        all_records.extend(fetch_day(d.strftime("%Y-%m-%d"), signal_meta))
    return all_records

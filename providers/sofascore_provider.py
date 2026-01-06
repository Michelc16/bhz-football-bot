import logging
import os
import time
import unicodedata
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

import pytz
import requests

log = logging.getLogger("bhz-football-bot.sofascore")

SOFASCORE_BASE = "https://api.sofascore.com/api/v1"
COMPETITION_FALLBACK = "SofaScore"
TZ = pytz.timezone("America/Sao_Paulo")
TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "15"))
MAX_RETRIES = 3
RETRY_BACKOFF = (1, 2, 4)

SOFASCORE_TEAM_IDS = {
    "Cruzeiro": 1241,
    "Atletico-MG": 1237,
    "America-MG": 1234,
}

TEAM_CANONICAL = {
    "cruzeiro": "Cruzeiro",
    "cruzeiroec": "Cruzeiro",
    "cruzeiroesporteclube": "Cruzeiro",
    "atleticomg": "Atletico-MG",
    "atletico": "Atletico-MG",
    "atléticomg": "Atletico-MG",
    "atlético-mg": "Atletico-MG",
    "atlético": "Atletico-MG",
    "america-mg": "America-MG",
    "américa-mg": "America-MG",
    "americamg": "America-MG",
    "america": "America-MG",
    "américa": "America-MG",
    "america mineiro": "America-MG",
    "américa mineiro": "America-MG",
    "coelho": "America-MG",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://www.sofascore.com",
    "Referer": "https://www.sofascore.com/",
}


def fetch_matches(cfg, teams: List[str], date_from: date, date_to: date) -> List[Dict[str, str]]:
    target_keys = {_normalized_key(team) for team in teams}
    collected: List[Dict[str, str]] = []
    seen_events = set()
    for team in teams:
        canonical = _normalize_name(team)
        team_id = SOFASCORE_TEAM_IDS.get(canonical)
        if not team_id:
            log.warning(f"[WARN] Não há mapping de SofaScore para {team}. Ignorando.")
            continue
        events = _fetch_team_events(team_id)
        if not events:
            continue
        log.info(f"[INFO] SofaScore: {len(events)} jogos carregados para {team}.")
        for event in events:
            normalized = _normalize_event(event)
            if not normalized:
                continue
            event_id = normalized["external_id"]
            if event_id in seen_events:
                continue
            match_date = datetime.strptime(normalized["match_datetime"], "%Y-%m-%d %H:%M:%S").date()
            if not (date_from <= match_date <= date_to):
                continue
            home_key = _normalized_key(normalized["home_team"])
            away_key = _normalized_key(normalized["away_team"])
            if home_key not in target_keys and away_key not in target_keys:
                continue
            seen_events.add(event_id)
            collected.append(normalized)
    return collected


def _fetch_team_events(team_id: int) -> List[Dict]:
    url = f"{SOFASCORE_BASE}/team/{team_id}/events/next/0"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()
            events = data.get("events") or data.get("matches") or []
            return events
        except requests.RequestException as exc:
            status = getattr(exc.response, "status_code", None)
            if status in {403, 404}:
                log.warning(f"[WARN] SofaScore [{team_id}] retornou {status} em {url}. Ignorando este time.")
                return []
            if attempt == MAX_RETRIES:
                log.warning(f"[WARN] SofaScore [{team_id}] falhou após {attempt} tentativas: {exc}")
                return []
            backoff = RETRY_BACKOFF[min(attempt - 1, len(RETRY_BACKOFF) - 1)]
            log.info(f"[INFO] Retry em {backoff}s para SofaScore [{team_id}] ({attempt}/{MAX_RETRIES})")
            time.sleep(backoff)
    return []


def _normalize_event(event: Dict) -> Optional[Dict[str, str]]:
    event_id = event.get("id")
    timestamp = event.get("startTimestamp")
    if event_id is None or timestamp is None:
        return None
    try:
        start_dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc).astimezone(TZ)
    except (TypeError, ValueError):
        return None
    dt_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    home = _extract_team_name(event.get("homeTeam"))
    away = _extract_team_name(event.get("awayTeam"))
    if not home or not away:
        return None
    venue = _extract_venue(event.get("venue"))
    competition = _extract_competition(event.get("tournament") or event.get("competition"))
    status = _extract_status(event.get("status"))
    return {
        "external_id": f"sofascore|{event_id}",
        "competition": competition,
        "match_datetime": dt_str,
        "home_team": home,
        "away_team": away,
        "venue": venue,
        "status": status or "scheduled",
        "source": "sofascore.com",
    }


def _extract_team_name(team_info) -> Optional[str]:
    if isinstance(team_info, dict):
        return team_info.get("name") or team_info.get("shortName")
    return None


def _extract_venue(venue_info) -> str:
    if isinstance(venue_info, dict):
        return venue_info.get("name") or ""
    return ""


def _extract_competition(comp_info) -> str:
    if isinstance(comp_info, dict):
        return comp_info.get("name") or COMPETITION_FALLBACK
    return COMPETITION_FALLBACK


def _extract_status(status_info) -> Optional[str]:
    if isinstance(status_info, dict):
        return status_info.get("description") or status_info.get("type")
    if isinstance(status_info, str):
        return status_info
    return None


def _normalize_name(value: Optional[str]) -> str:
    if not value:
        return ""
    key = _normalized_key(value)
    return TEAM_CANONICAL.get(key, value.strip())


def _normalized_key(value: Optional[str]) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFD", value)
    normalized = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    normalized = "".join(ch for ch in normalized.lower() if ch.isalnum())
    return normalized

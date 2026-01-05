import hashlib
import json
import logging
from datetime import date, datetime
from typing import Dict, List, Optional

import pytz
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

log = logging.getLogger("bhz-football-bot.ge_mineiro")

GE_URL = "https://ge.globo.com/mg/futebol/campeonato-mineiro/"
COMPETITION_NAME = "Campeonato Mineiro"
TZ = pytz.timezone("America/Sao_Paulo")
TARGET_TEAMS = {"cruzeiro", "atletico-mg", "america-mg"}

TEAM_ALIASES: Dict[str, str] = {
    "cruzeiro": "Cruzeiro",
    "cruzeiro ec": "Cruzeiro",
    "cruzeiro esporte clube": "Cruzeiro",
    "atlético-mg": "Atletico-MG",
    "atletico-mg": "Atletico-MG",
    "atlético mineiro": "Atletico-MG",
    "atletico mineiro": "Atletico-MG",
    "galo": "Atletico-MG",
    "américa-mg": "America-MG",
    "america-mg": "America-MG",
    "américa mineiro": "America-MG",
    "america mineiro": "America-MG",
    "coelho": "America-MG",
}


def fetch_matches(date_from: date, date_to: date) -> List[Dict[str, str]]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )

    try:
        response = session.get(GE_URL, timeout=20)
        response.raise_for_status()
    except requests.RequestException as exc:
        log.error(f"[ERROR] Falha ao acessar {GE_URL}: {exc}")
        return []

    soup = BeautifulSoup(response.text, "lxml")
    _log_round_counts(soup)

    raw_events = _extract_jsonld_matches(soup)
    if not raw_events:
        log.warning("[WARN] Nenhum jogo encontrado via JSON-LD. Estrutura da página pode ter mudado.")
    matches: List[Dict[str, str]] = []
    for event in raw_events:
        match = _convert_event(event)
        if not match:
            continue
        match_date = datetime.strptime(match["match_datetime"], "%Y-%m-%d %H:%M:%S").date()
        if not (date_from <= match_date <= date_to):
            continue
        if not _is_target_match(match):
            continue
        matches.append(match)

    log.info(f"[INFO] Total de jogos coletados (após filtros): {len(matches)}")
    return matches


def _log_round_counts(soup: BeautifulSoup) -> None:
    counts: Dict[str, int] = {}
    for header in soup.find_all(["h2", "h3", "h4"]):
        text = header.get_text(" ", strip=True)
        if not text or "rodada" not in text.lower():
            continue
        container = header.parent
        if not container:
            continue
        matches = container.find_all("li") or container.find_all("article")
        counts[text] = len(matches)
    if counts:
        for rodada, qtd in counts.items():
            log.info(f"[INFO] {rodada}: {qtd} jogos listados")
    else:
        log.warning("[WARN] Não foi possível identificar seções de rodada na página do GE.")


def _extract_jsonld_matches(soup: BeautifulSoup) -> List[Dict[str, str]]:
    events: List[Dict[str, str]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        content = script.string or script.text or ""
        content = content.strip()
        if not content:
            continue
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            continue
        events.extend(_collect_events_from_json(data))
    return events


def _collect_events_from_json(payload) -> List[Dict[str, str]]:
    collected: List[Dict[str, str]] = []
    if isinstance(payload, list):
        for item in payload:
            collected.extend(_collect_events_from_json(item))
        return collected
    if isinstance(payload, dict):
        if payload.get("@type") in {"SportsEvent", "Event"}:
            collected.append(payload)
        for key in ("@graph", "graph", "events", "event", "itemListElement"):
            inner = payload.get(key)
            if inner:
                collected.extend(_collect_events_from_json(inner))
    return collected


def _convert_event(event: Dict[str, str]) -> Optional[Dict[str, str]]:
    start_value = event.get("startDate") or event.get("startTime") or event.get("start_date")
    if not start_value:
        return None
    try:
        dt = dateparser.parse(str(start_value))
    except (ValueError, TypeError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = TZ.localize(dt)
    else:
        dt = dt.astimezone(TZ)

    if "T" not in str(start_value) or (dt.hour == 0 and dt.minute == 0):
        log.warning("[WARN] Horário não identificado em um jogo. Assumindo 00:00:00.")
    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")

    home = _extract_participant(event, "homeTeam")
    away = _extract_participant(event, "awayTeam")
    if not home or not away:
        return None

    venue = None
    venue_block = event.get("location") or event.get("venue")
    if isinstance(venue_block, dict):
        venue = venue_block.get("name") or venue_block.get("address")
    elif isinstance(venue_block, str):
        venue = venue_block

    status = event.get("eventStatus") or event.get("status") or "scheduled"

    canonical_home = _canonical_name(home)
    canonical_away = _canonical_name(away)

    external_id = _build_external_id(dt_str, canonical_home, canonical_away, venue)
    return {
        "external_id": external_id,
        "competition": COMPETITION_NAME,
        "match_datetime": dt_str,
        "home_team": canonical_home,
        "away_team": canonical_away,
        "venue": venue,
        "status": status or "scheduled",
        "source": "ge.globo.com",
    }


def _extract_participant(event: Dict[str, str], key: str) -> Optional[str]:
    data = event.get(key)
    if isinstance(data, dict):
        name = data.get("name") or data.get("alternateName")
        if name:
            return name
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                name = item.get("name")
                if name:
                    return name
    if isinstance(data, str):
        return data
    return None


def _canonical_name(name: str) -> str:
    return TEAM_ALIASES.get(name.strip().lower(), name.strip())


def _is_target_match(match: Dict[str, str]) -> bool:
    home = match.get("home_team", "").strip().lower()
    away = match.get("away_team", "").strip().lower()
    return home in TARGET_TEAMS or away in TARGET_TEAMS


def _build_external_id(dt_str: str, home: str, away: str, venue: Optional[str]) -> str:
    base = "|".join(["ge_mineiro", dt_str or "", home or "", away or "", venue or ""])
    return hashlib.sha1(base.lower().encode("utf-8")).hexdigest()

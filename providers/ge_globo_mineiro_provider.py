import hashlib
import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytz
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

log = logging.getLogger("bhz-football-bot.ge_globo")

GE_URL = "https://ge.globo.com/mg/futebol/campeonato-mineiro/"
COMPETITION_NAME = "Campeonato Mineiro"
TZ = pytz.timezone("America/Sao_Paulo")
CACHE_DIR = Path(".cache")
CACHE_FILE = CACHE_DIR / "ge_mineiro.html"

TEAM_ALIASES = {
    "cruzeiro": "Cruzeiro",
    "cruzeiro ec": "Cruzeiro",
    "cruzeiro esporte clube": "Cruzeiro",
    "atlético-mg": "Atletico-MG",
    "atletico-mg": "Atletico-MG",
    "atlético mineiro": "Atletico-MG",
    "atletico mineiro": "Atletico-MG",
    "atlético": "Atletico-MG",
    "galo": "Atletico-MG",
    "américa-mg": "America-MG",
    "america-mg": "America-MG",
    "américa mineiro": "America-MG",
    "america mineiro": "America-MG",
    "américa": "America-MG",
    "coelho": "America-MG",
}


def canonicalize(name: str) -> str:
    if not name:
        return name
    return TEAM_ALIASES.get(name.strip().lower(), name.strip())


def fetch_matches(cfg, teams: List[str], date_from: date, date_to: date) -> List[Dict[str, str]]:
    target_set = {canonicalize(team).lower() for team in teams}
    html = _load_ge_page()
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    _log_round_counts(soup)

    events = _extract_matches(soup)
    log.info(f"[INFO] GE: {len(events)} eventos brutos")

    filtered: List[Dict[str, str]] = []
    for event in events:
        normalized = _normalize_event(event, date_from, date_to)
        if not normalized:
            continue
        if not _is_target_match(normalized, target_set):
            continue
        filtered.append(normalized)

    log.info(f"[INFO] GE: {len(filtered)} jogos após filtros")
    return filtered


def _load_ge_page() -> Optional[str]:
    use_cache = os.getenv("GE_CACHE", "0").strip() == "1"
    offline = os.getenv("GE_OFFLINE", "0").strip() == "1"

    if offline and CACHE_FILE.exists():
        log.info(f"[INFO] GE_OFFLINE=1 -> usando cache {CACHE_FILE}")
        return CACHE_FILE.read_text(encoding="utf-8")

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        }
    )

    for attempt in range(1, 3):
        try:
            response = session.get(GE_URL, timeout=20)
            response.raise_for_status()
            html = response.text
            log.info(f"[INFO] GET {response.url} ({len(html)} bytes)")
            if use_cache:
                CACHE_DIR.mkdir(parents=True, exist_ok=True)
                CACHE_FILE.write_text(html, encoding="utf-8")
            return html
        except requests.RequestException as exc:
            log.warning(f"[WARN] Tentativa {attempt}/2 falhou ao acessar GE: {exc}")
    log.error("[ERROR] Não foi possível baixar a página do GE.")
    if CACHE_FILE.exists():
        log.info(f"[INFO] Usando cache como fallback: {CACHE_FILE}")
        return CACHE_FILE.read_text(encoding="utf-8")
    return None


def _log_round_counts(soup: BeautifulSoup) -> None:
    sections = soup.select("section[class*='jogos']")
    counts: Dict[str, int] = {}
    for section in sections:
        header = section.find(["h2", "h3", "h4"])
        if not header:
            continue
        title = header.get_text(" ", strip=True)
        games = section.find_all("article") or section.find_all("li")
        if games:
            counts[title] = len(games)
    if counts:
        for rodada, qty in counts.items():
            log.info(f"[INFO] {rodada}: {qty} jogos listados")
    else:
        log.warning("[WARN] Não foi possível identificar as rodadas na página do GE.")


def _extract_matches(soup: BeautifulSoup) -> List[Dict[str, str]]:
    events = _extract_jsonld(soup)
    if events:
        return events
    log.warning("[WARN] JSON-LD indisponível. Tentando fallback em HTML bruto.")
    return _extract_from_html(soup)


def _extract_jsonld(soup: BeautifulSoup) -> List[Dict[str, str]]:
    events: List[Dict[str, str]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        content = (script.string or script.text or "").strip()
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


def _extract_from_html(soup: BeautifulSoup) -> List[Dict[str, str]]:
    matches: List[Dict[str, str]] = []
    sections = soup.select("section[class*='jogos']") or soup.find_all("section")
    for section in sections:
        cards = section.find_all("article") or section.find_all("li")
        for card in cards:
            team_nodes = card.find_all("strong")
            if len(team_nodes) < 2:
                continue
            home = team_nodes[0].get_text(strip=True)
            away = team_nodes[1].get_text(strip=True)
            raw_text = card.get_text(" ", strip=True)
            date_token, time_token = _parse_date_time_from_text(raw_text)
            stadium = _parse_stadium_from_text(raw_text)
            matches.append(
                {
                    "home_team": home,
                    "away_team": away,
                    "date": date_token,
                    "time": time_token or "00:00",
                    "stadium": stadium,
                }
            )
    return matches


def _parse_date_time_from_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    if not text:
        return None, None
    tokens = [t.strip() for t in re.split(r"[•\-|]", text) if t.strip()]
    date_token = None
    time_token = None
    for token in tokens:
        if re.match(r"\d{1,2}/\d{1,2}", token):
            date_token = token
        if re.match(r"\d{1,2}:\d{2}", token):
            time_token = token
    return date_token, time_token


def _parse_stadium_from_text(text: str) -> Optional[str]:
    possible = ["Mineirão", "Arena MRV", "Independência", "Soares", "Estádio", "Arena"]
    for marker in possible:
        if marker.lower() in text.lower():
            return marker
    return None


def _normalize_event(event: Dict[str, str], date_from: date, date_to: date) -> Optional[Dict[str, str]]:
    dt = None
    raw_datetime = event.get("match_datetime") or event.get("startDate") or event.get("startTime")
    if raw_datetime:
        try:
            dt = dateparser.parse(str(raw_datetime))
        except (ValueError, TypeError):
            dt = None
    if dt is None:
        date_token = event.get("date")
        time_token = event.get("time") or "00:00"
        if not date_token:
            return None
        dt = _build_datetime_from_tokens(date_token, time_token, date_from, date_to)
        if dt and (time_token == "00:00" or time_token is None):
            log.warning("[WARN] Horário não informado em um jogo. Assumindo 00:00:00.")
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = TZ.localize(dt)
    else:
        dt = dt.astimezone(TZ)
    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")

    home = canonicalize(event.get("home_team") or event.get("homeTeam") or event.get("home"))
    away = canonicalize(event.get("away_team") or event.get("awayTeam") or event.get("away"))
    if not home or not away:
        return None

    venue = event.get("stadium") or ""
    if isinstance(venue, dict):
        venue = venue.get("name") or venue.get("address") or ""

    return {
        "external_id": _build_external_id(dt_str, home, away, venue or ""),
        "competition": COMPETITION_NAME,
        "match_datetime": dt_str,
        "home_team": home,
        "away_team": away,
        "venue": venue or "",
        "status": event.get("eventStatus") or event.get("status") or "scheduled",
        "source": "ge.globo.com",
    }


def _build_datetime_from_tokens(date_token: str, time_token: str, date_from: date, date_to: date) -> Optional[datetime]:
    try:
        day, month = [int(x) for x in date_token.split("/")]
    except Exception:
        return None
    candidate_years = sorted({date_from.year, date_to.year})
    for year in candidate_years:
        try:
            dt = datetime.strptime(f"{day:02d}/{month:02d}/{year:04d} {time_token}", "%d/%m/%Y %H:%M")
        except ValueError:
            continue
        if date_from <= dt.date() <= date_to:
            return TZ.localize(dt)
    # fallback assume date_from year
    try:
        dt = datetime.strptime(f"{day:02d}/{month:02d}/{candidate_years[0]:04d} {time_token}", "%d/%m/%Y %H:%M")
        return TZ.localize(dt)
    except ValueError:
        return None


def _is_target_match(match: Dict[str, str], target_set: set) -> bool:
    home = (match.get("home_team") or "").strip().lower()
    away = (match.get("away_team") or "").strip().lower()
    return home in target_set or away in target_set


def _build_external_id(dt_str: str, home: str, away: str, venue: str) -> str:
    base = "|".join(["ge_mineiro", COMPETITION_NAME, dt_str or "", home or "", away or "", venue or ""]).lower()
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


if __name__ == "__main__":  # pragma: no cover (debug helper)
    today = datetime.utcnow().date()
    start = today - timedelta(days=7)
    end = today + timedelta(days=180)
    matches = fetch_matches(None, ["Cruzeiro", "Atletico-MG", "America-MG"], start, end)
    for match in matches[:5]:
        print(match)
    print(f"Total coletado: {len(matches)}")

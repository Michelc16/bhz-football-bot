import difflib
import hashlib
import json
import logging
import os
import re
import unicodedata
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

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
DEBUG_HTML_PATH = Path("debug_ge_mineiro.html")


def normalize_name_key(name: str) -> str:
    if not name:
        return ""
    normalized = unicodedata.normalize("NFD", name)
    normalized = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    normalized = re.sub(r"[\s\-_]+", "", normalized)
    return normalized.lower()


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

NORMALIZED_ALIASES = {normalize_name_key(k): v for k, v in TEAM_ALIASES.items()}


def canonicalize(name: str) -> str:
    if not name:
        return name
    normalized = normalize_name_key(name)
    if normalized in NORMALIZED_ALIASES:
        return NORMALIZED_ALIASES[normalized]
    for key, canonical in NORMALIZED_ALIASES.items():
        if key in normalized:
            return canonical
    close = difflib.get_close_matches(normalized, list(NORMALIZED_ALIASES.keys()), n=1, cutoff=0.75)
    if close:
        return NORMALIZED_ALIASES[close[0]]
    return name.strip()


def _normalized_for_comparison(name: str) -> str:
    return normalize_name_key(canonicalize(name))


def fetch_matches(cfg, teams: List[str], date_from: date, date_to: date) -> List[Dict[str, str]]:
    target_set = {_normalized_for_comparison(team) for team in teams}
    html = _load_ge_page()
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    _log_round_counts(soup)

    events = _extract_matches(html, soup)
    if not events:
        _diagnose_missing_data(html, soup)
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


def _extract_matches(html: str, soup: BeautifulSoup) -> List[Dict[str, str]]:
    matches = _extract_matches_from_dom(soup)
    if matches:
        return matches
    json_ld = _extract_jsonld(soup)
    if json_ld:
        log.info("[INFO] JSON-LD encontrado como fallback.")
        return json_ld
    scripted = _extract_matches_from_scripts(soup)
    if scripted:
        log.info("[INFO] Dados embutidos extraídos via script.")
        return scripted
    return []


def _extract_matches_from_dom(soup: BeautifulSoup) -> List[Dict[str, str]]:
    sections = _locate_jogos_sections(soup)
    matches: List[Dict[str, str]] = []
    for section in sections:
        matches.extend(_parse_section_matches(section))
    return matches


def _locate_jogos_sections(soup: BeautifulSoup) -> List[BeautifulSoup]:
    sections: List[BeautifulSoup] = []
    seen = set()
    for header in soup.find_all(["h2", "h3", "h4", "h5"]):
        title = header.get_text(" ", strip=True).lower()
        if "jogos" in title or "rodada" in title:
            container = header.find_parent(["section", "div"]) or header.parent
            if container and id(container) not in seen:
                sections.append(container)
                seen.add(id(container))
    if not sections:
        fallback = soup.find("section", class_=re.compile("jogos", re.I))
        if fallback:
            sections.append(fallback)
        else:
            sections.append(soup)
    return sections


def _parse_section_matches(section: BeautifulSoup) -> List[Dict[str, str]]:
    matches: List[Dict[str, str]] = []
    cards = _find_candidate_cards(section)
    for card in cards:
        match = _parse_game_card(card)
        if match:
            matches.append(match)
    return matches


def _find_candidate_cards(section: BeautifulSoup) -> List[BeautifulSoup]:
    candidates: List[BeautifulSoup] = []
    for tag in section.find_all(["article", "li", "div"], recursive=True):
        text = tag.get_text(" ", strip=True)
        if not text:
            continue
        if re.search(r"[x×]", text):
            candidates.append(tag)
    return candidates


def _parse_game_card(card: BeautifulSoup) -> Optional[Dict[str, str]]:
    text = card.get_text(" ", strip=True)
    lines = [line.strip() for line in card.stripped_strings if line.strip()]
    teams_line = _find_teams_line(lines, text)
    if not teams_line:
        return None
    home, away = _extract_team_names(teams_line)
    if not home or not away:
        return None
    date_token, time_token = _parse_date_time_from_text(text)
    stadium = _parse_stadium_from_lines(lines)
    stadium = stadium or _parse_stadium_from_text(text)
    return {
        "home_team": home,
        "away_team": away,
        "date": date_token,
        "time": time_token or "00:00",
        "stadium": stadium or "",
        "raw_text": text,
    }


def _find_teams_line(lines: List[str], raw_text: str) -> Optional[str]:
    for line in lines:
        if re.search(r"[x×]", line):
            return line
    return raw_text


def _extract_team_names(teams_line: str) -> Tuple[Optional[str], Optional[str]]:
    if not teams_line:
        return None, None
    parts = re.split(r"\s+[x×]\s+", teams_line, maxsplit=1)
    if len(parts) != 2:
        return None, None
    home = parts[0].strip().rstrip("•-–")
    away = re.split(r"\s+[•\-\(].*", parts[1])[0].strip()
    return home, away


def _extract_matches_from_scripts(soup: BeautifulSoup) -> List[Dict[str, str]]:
    matches: List[Dict[str, str]] = []
    scripts = soup.find_all("script")
    for script in scripts:
        script_text = (script.string or script.text or "").strip()
        if not script_text:
            continue
        payload_text = _extract_json_payload_from_script(script_text)
        if not payload_text:
            continue
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            continue
        matches.extend(_collect_events_from_json(payload))
    return matches


def _extract_json_payload_from_script(script_text: str) -> Optional[str]:
    if script_text.startswith("{") or script_text.startswith("["):
        return script_text
    if "__NEXT_DATA__" in script_text:
        start = script_text.find("{", script_text.find("__NEXT_DATA__"))
        if start != -1:
            return _consume_braced_fragment(script_text, start)
    assign_match = re.search(r"=\s*({.*})\s*;", script_text, re.S)
    if assign_match:
        return assign_match.group(1)
    return None


def _consume_braced_fragment(text: str, start: int) -> Optional[str]:
    depth = 0
    for idx in range(start, len(text)):
        ch = text[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
    return None


def _collect_events_from_json(payload) -> List[Dict[str, str]]:
    collected: List[Dict[str, str]] = []
    if isinstance(payload, list):
        for item in payload:
            collected.extend(_collect_events_from_json(item))
        return collected
    if isinstance(payload, dict):
        if payload.get("@type") in {"SportsEvent", "Event"}:
            collected.append(payload)
        for key in ("@graph", "graph", "events", "event", "itemListElement", "matches"):
            inner = payload.get(key)
            if inner:
                collected.extend(_collect_events_from_json(inner))
    return collected


def _parse_date_time_from_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    if not text:
        return None, None
    tokens = [t.strip() for t in re.split(r"[•\-\|\n]", text) if t.strip()]
    date_token = None
    time_token = None
    for token in tokens:
        if re.match(r"\d{1,2}/\d{1,2}", token):
            date_token = token
        if re.match(r"\d{1,2}:\d{2}", token):
            time_token = token
    return date_token, time_token


def _parse_stadium_from_lines(lines: List[str]) -> Optional[str]:
    stadium_markers = [
        "mineirão",
        "arena",
        "independência",
        "estádio",
        "estadio",
        "soares",
        "itacolomi",
        "castelão",
    ]
    for line in lines:
        lower = line.lower()
        if any(marker in lower for marker in stadium_markers) and not re.search(r"\d", line):
            return line
    return None


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
    try:
        dt = datetime.strptime(f"{day:02d}/{month:02d}/{candidate_years[0]:04d} {time_token}", "%d/%m/%Y %H:%M")
        return TZ.localize(dt)
    except ValueError:
        return None


def _is_target_match(match: Dict[str, str], target_set: set) -> bool:
    home = _normalized_for_comparison(match.get("home_team") or "")
    away = _normalized_for_comparison(match.get("away_team") or "")
    return home in target_set or away in target_set


def _build_external_id(dt_str: str, home: str, away: str, venue: str) -> str:
    base = "|".join(["ge_mineiro", COMPETITION_NAME, dt_str or "", home or "", away or "", venue or ""]).lower()
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _diagnose_missing_data(html: str, soup: BeautifulSoup) -> None:
    try:
        DEBUG_HTML_PATH.write_text(html, encoding="utf-8")
        log.warning(f"[WARN] HTML salvo para diagnóstico em {DEBUG_HTML_PATH}")
    except Exception as exc:
        log.warning(f"[WARN] Falha ao salvar HTML de diagnóstico: {exc}")
    log.info(f"[INFO] Tamanho do HTML: {len(html)} bytes")
    snippet = html[:2000].replace("\n", " ")
    log.info(f"[INFO] Início do HTML: {snippet}")
    class_counter: Counter = Counter()
    for tag in soup.find_all(True):
        for cls in tag.get("class") or []:
            class_counter[cls] += 1
    for idx, (cls, qty) in enumerate(class_counter.most_common(20), start=1):
        log.info(f"[INFO] {idx:02d}. Classe '{cls}': {qty} ocorrências")
    keywords = ["JOGOS", "RODADA", "Cruzeiro", "Atlético", "América"]
    for keyword in keywords:
        pattern = re.compile(rf".{{0,60}}{re.escape(keyword)}.{{0,60}}", re.IGNORECASE)
        match = pattern.search(html)
        if match:
            context = match.group(0).replace("\n", " ").strip()
            log.info(f"[INFO] Contexto para '{keyword}': {context[:200]}")


if __name__ == "__main__":  # pragma: no cover (debug helper)
    today = datetime.utcnow().date()
    start = today - timedelta(days=7)
    end = today + timedelta(days=180)
    matches = fetch_matches(None, ["Cruzeiro", "Atletico-MG", "America-MG"], start, end)
    for match in matches[:5]:
        print(match)
    print(f"Total coletado: {len(matches)}")

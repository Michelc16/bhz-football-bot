import logging
import os
import re
import unicodedata
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import pytz
import requests
from bs4 import BeautifulSoup

log = logging.getLogger("bhz-football-bot.flashscore")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

TZ = pytz.timezone("America/Sao_Paulo")
TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
DATE_REGEX = re.compile(r"(\d{1,2})[./](\d{1,2})")
TIME_REGEX = re.compile(r"(\d{1,2}):(\d{2})")

TEAM_PAGES = {
    "Cruzeiro": "https://www.flashscore.com/team/cruzeiro/0SwtclaU",
    "Atletico-MG": "https://www.flashscore.com/team/atletico-mg/hGLC5Bah",
    "America-MG": "https://www.flashscore.com/team/america-mg/xUT0Bp8o",
}

TEAM_ALIASES = {
    "cruzeiro": "Cruzeiro",
    "cruzeiro ec": "Cruzeiro",
    "cruzeiro esporte clube": "Cruzeiro",
    "atletico-mg": "Atletico-MG",
    "atlético-mg": "Atletico-MG",
    "atletico mineiro": "Atletico-MG",
    "atlético mineiro": "Atletico-MG",
    "atletico": "Atletico-MG",
    "atlético": "Atletico-MG",
    "galo": "Atletico-MG",
    "america-mg": "America-MG",
    "américa-mg": "America-MG",
    "america mineiro": "America-MG",
    "américa mineiro": "America-MG",
    "america": "America-MG",
    "américa": "America-MG",
    "coelho": "America-MG",
}


def _normalized_key(value: Optional[str]) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFD", value)
    normalized = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    return "".join(ch for ch in normalized.lower() if ch.isalnum())


def _canonicalize_team(name: str) -> str:
    key = _normalized_key(name)
    return TEAM_ALIASES.get(key, name.strip())


def fetch_matches(cfg, teams: List[str], date_from: date, date_to: date) -> List[Dict[str, str]]:
    target_keys = {_normalized_key(team) for team in teams}
    collected: List[Dict[str, str]] = []
    seen_events = set()
    for team in teams:
        canonical = _canonicalize_team(team)
        base_url = TEAM_PAGES.get(canonical)
        if not base_url:
            log.warning(f"[WARN] Sem URL do FlashScore para {team}. Ignorando.")
            continue
        html = _load_flashscore_html(base_url)
        if not html:
            continue
        matches = _parse_flashscore_matches(html, date_from, date_to)
        log.info(f"[INFO] FlashScore: {len(matches)} eventos coletados para {team}.")
        for match in matches:
            match_id = match["external_id"]
            if match_id in seen_events:
                continue
            match_date = datetime.strptime(match["match_datetime"], "%Y-%m-%d %H:%M:%S").date()
            if not (date_from <= match_date <= date_to):
                continue
            home_key = _normalized_key(match["home_team"])
            away_key = _normalized_key(match["away_team"])
            if home_key not in target_keys and away_key not in target_keys:
                continue
            seen_events.add(match_id)
            collected.append(match)
    return collected


def _load_flashscore_html(base_url: str) -> Optional[str]:
    fixtures_url = base_url.rstrip("/") + "/fixtures/"
    try:
        response = requests.get(fixtures_url, headers=HEADERS, timeout=TIMEOUT)
        if response.status_code != 200:
            log.warning(f"[WARN] FlashScore retornou {response.status_code} em {fixtures_url}")
            return None
        log.info(f"[INFO] GET {fixtures_url} ({len(response.text)} bytes)")
        return response.text
    except requests.RequestException as exc:
        log.warning(f"[WARN] Falha ao buscar {fixtures_url}: {exc}")
    log.error("[ERROR] Não foi possível baixar a página do FlashScore.")
    return None


def _parse_flashscore_matches(html: str, date_from: date, date_to: date) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("div.event__match")
    if not cards:
        log.warning("[WARN] Nenhum card de jogo encontrado no FlashScore.")
    matches: List[Dict[str, str]] = []
    for card in cards:
        parsed = _parse_match_card(card, date_from, date_to)
        if parsed:
            matches.append(parsed)
    if matches:
        return matches
    fallback = _parse_flashscore_text_fallback(soup, date_from, date_to)
    return fallback


def _parse_match_card(card: "BeautifulSoup", date_from: date, date_to: date) -> Optional[Dict[str, str]]:
    date_token = _extract_date_token(card)
    time_token = _extract_time_token(card)
    dt = _build_datetime(date_token, time_token, date_from, date_to)
    if not dt:
        log.warning("[WARN] Não foi possível montar datetime para um jogo. Ignorando.")
        return None
    home = _extract_participant(card, "home")
    away = _extract_participant(card, "away")
    if not home or not away:
        log.warning("[WARN] Time mandante ou visitante ausente em um card. Ignorando.")
        return None
    competition = _extract_text(card, [".event__title--type", ".event__stage"])
    venue = _extract_text(card, [".event__venue", ".event__match__venue"])

    event_id = card.get("data-event-id") or card.get("id")
    fallback_key = "|".join([dt.strftime("%Y-%m-%d %H:%M:%S"), home, away]).lower()
    key = (event_id or fallback_key).lower()
    external_id = f"flashscore|{key}"

    return {
        "external_id": external_id,
        "competition": competition or "FlashScore",
        "match_datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "home_team": _canonicalize_team(home),
        "away_team": _canonicalize_team(away),
        "venue": venue or "",
        "status": "scheduled",
        "source": "flashscore.com",
    }


def _extract_date_token(card: "BeautifulSoup") -> Optional[str]:
    candidate = card.get("data-event-date") or card.get("data-date")
    if candidate:
        return candidate
    calendar_row = card.find_parent("div", class_=re.compile("calendar__row", re.I))
    if calendar_row:
        date_node = calendar_row.select_one(".calendar__date")
        if date_node:
            match = DATE_REGEX.search(date_node.get_text(" ", strip=True))
            if match:
                return f"{match.group(1)}.{match.group(2)}"
    text = card.get_text(" ", strip=True)
    match = DATE_REGEX.search(text)
    if match:
        return f"{match.group(1)}.{match.group(2)}"
    return None


def _extract_time_token(card: "BeautifulSoup") -> Optional[str]:
    candidate = card.get("data-event-time") or card.get("data-time")
    if candidate:
        return candidate
    time_node = card.select_one(".event__time")
    if time_node:
        text = time_node.get_text(" ", strip=True)
        if TIME_REGEX.search(text):
            return TIME_REGEX.search(text).group(1)
    text = card.get_text(" ", strip=True)
    match = TIME_REGEX.search(text)
    if match:
        return match.group(1)
    return None


def _extract_participant(card: "BeautifulSoup", role: str) -> Optional[str]:
    selectors = [
        f".event__participant--{role} .event__participant__name",
        f".event__participant--{role}",
    ]
    for selector in selectors:
        node = card.select_one(selector)
        if node:
            text = node.get_text(" ", strip=True)
            if text:
                return text
    participants = card.select(".event__participant")
    if participants and len(participants) >= 2:
        node = participants[0] if role == "home" else participants[1]
        text = node.get_text(" ", strip=True)
        if text:
            return text
    return None


def _extract_text(card: "BeautifulSoup", selectors: List[str]) -> str:
    for selector in selectors:
        node = card.select_one(selector)
        if node:
            text = node.get_text(" ", strip=True)
            if text:
                return text
    return ""


def _parse_date_token(token: str) -> Optional[Tuple[Optional[int], int, int]]:
    if not token:
        return None
    iso_match = re.match(r"(\d{4})-(\d{2})-(\d{2})", token)
    if iso_match:
        return int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3))
    regex_match = DATE_REGEX.search(token)
    if regex_match:
        return None, int(regex_match.group(2)), int(regex_match.group(1))
    return None


def _parse_time_token(token: Optional[str]) -> Tuple[int, int]:
    if not token:
        return 0, 0
    match = TIME_REGEX.search(token)
    if match:
        return int(match.group(1)), int(match.group(2))
    return 0, 0


def _build_datetime(date_token: Optional[str], time_token: Optional[str], date_from: date, date_to: date) -> Optional[datetime]:
    parsed = _parse_date_token(date_token or "")
    if not parsed:
        return None


def _parse_flashscore_text_fallback(soup: BeautifulSoup, date_from: date, date_to: date) -> List[Dict[str, str]]:
    text = soup.get_text(" ", strip=True)
    section = _extract_upcoming_section(text)
    if not section:
        log.warning("[WARN] FlashScore fallback não encontrou seção aguardando partidas.")
        return []
    log.info("[INFO] FlashScore fallback: usando 'Upcoming matches' (texto estático)")
    candidates = [item.strip() for item in section.split(",") if item.strip()]
    parsed_matches: List[Dict[str, str]] = []
    for item in candidates:
        parsed = _parse_text_item(item, date_from, date_to)
        if parsed:
            parsed_matches.append(parsed)
    log.info(f"[INFO] FlashScore fallback: {len(parsed_matches)} itens extraídos do texto.")
    return parsed_matches


def _extract_upcoming_section(text: str) -> str:
    markers = ["Upcoming matches:", "Próximas partidas:"]
    end_markers = ["Show more", "Mostrar mais", "See more", "Ver mais"]
    for marker in markers:
        start = text.find(marker)
        if start == -1:
            continue
        start += len(marker)
        end = len(text)
        for end_marker in end_markers:
            idx = text.find(end_marker, start)
            if idx != -1:
                end = idx
                break
        return text[start:end].strip()
    return ""


def _parse_text_item(item: str, date_from: date, date_to: date) -> Optional[Dict[str, str]]:
    date_match = DATE_REGEX.search(item)
    if not date_match:
        return None
    day = int(date_match.group(1))
    month = int(date_match.group(2))
    rest = item[date_match.end():].strip(" .-–•")
    teams = _split_teams(rest)
    if not teams:
        return None
    day_year = _infer_year(day, month, date_from, date_to)
    if day_year is None:
        return None
    dt = datetime(day_year, month, day, 12, 0)
    dt = TZ.localize(dt)
    home = _canonicalize_team(teams[0])
    away = _canonicalize_team(teams[1])
    date_key = dt.strftime("%Y-%m-%d")
    external_id = f"flashscore|{date_key}|{home}|{away}"
    return {
        "external_id": external_id,
        "competition": "FlashScore",
        "match_datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "home_team": home,
        "away_team": away,
        "venue": "",
        "status": "scheduled",
        "source": "flashscore.com",
    }


def _split_teams(text: str) -> Optional[Tuple[str, str]]:
    separators = [" v ", " x ", " - "]
    for sep in separators:
        if sep in text:
            parts = text.split(sep, 1)
            if len(parts) == 2:
                return parts[0].strip(), parts[1].strip()
    return None


def _infer_year(day: int, month: int, date_from: date, date_to: date) -> Optional[int]:
    candidates = sorted({date_from.year, date_to.year})
    for candidate in candidates:
        try:
            candidate_date = date(candidate, month, day)
        except ValueError:
            continue
        if date_from <= candidate_date <= date_to:
            return candidate
    if date_from.month == 12 and month <= date_to.month:
        return date_to.year
    return date_from.year
    year, month, day = parsed
    hour, minute = _parse_time_token(time_token)
    if year:
        try:
            dt = datetime(year, month, day, hour, minute)
            return TZ.localize(dt)
        except ValueError:
            return None
    candidate_years = list({date_from.year, date_to.year, datetime.utcnow().year})
    candidate_years.sort()
    for candidate in candidate_years:
        try:
            dt = datetime(candidate, month, day, hour, minute)
        except ValueError:
            continue
        if date_from <= dt.date() <= date_to:
            return TZ.localize(dt)
    try:
        dt = datetime(date_from.year, month, day, hour, minute)
        return TZ.localize(dt)
    except ValueError:
        log.warning("[WARN] Data inválida ignorada.")
        return None

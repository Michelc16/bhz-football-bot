# -*- coding: utf-8 -*-
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import pytz
import requests
from dateutil import parser as dateparser
from dotenv import load_dotenv

from providers import fetch_matches as fetch_ge_mineiro_matches

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("bhz-football-bot")

NORMALIZED_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TZ = pytz.timezone("America/Sao_Paulo")

TEAM_ALIASES = {
    "cruzeiro": "Cruzeiro",
    "cruzeiro ec": "Cruzeiro",
    "cruzeiro esporte clube": "Cruzeiro",
    "atletico-mg": "Atletico-MG",
    "atlético-mg": "Atletico-MG",
    "atletico mineiro": "Atletico-MG",
    "atlético mineiro": "Atletico-MG",
    "america-mg": "America-MG",
    "américa-mg": "America-MG",
    "america mineiro": "America-MG",
    "américa mineiro": "America-MG",
}


BASE_DIR = Path(__file__).resolve().parent
DOTENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=DOTENV_PATH)


def canonicalize_team(name: str) -> str:
    if not name:
        return name
    return TEAM_ALIASES.get(name.strip().lower(), name.strip())


@dataclass
class Config:
    odoo_url: str
    odoo_token: str
    teams: List[str]
    days_back: int
    days_forward: int
    dry_run: bool
    timeout: int


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        if name == "ODOO_URL":
            log.error("[FATAL] Variável ODOO_URL não definida.")
            log.info(f"[INFO] Diretório atual: {os.getcwd()}")
            log.info(f"[INFO] Caminho .env esperado: {DOTENV_PATH}")
            log.info("[INFO] Se o arquivo estiver como 'env', renomeie para '.env'.")
        else:
            log.error(f"[FATAL] Variável {name} não definida.")
        raise SystemExit(1)
    return value


def to_date(value: datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise TypeError("Valor informado não é datetime/date")


def parse_datetime(value: str) -> datetime:
    dt = dateparser.parse(value)
    if dt is None:
        raise ValueError(f"Não foi possível interpretar data '{value}'")
    if dt.tzinfo is None:
        dt = TZ.localize(dt)
    else:
        dt = dt.astimezone(TZ)
    return dt


def normalize_datetime_str(value: str) -> str:
    dt = parse_datetime(value)
    return dt.strftime(NORMALIZED_DATETIME_FORMAT)


def deduplicate(matches: List[Dict[str, str]]) -> List[Dict[str, str]]:
    dedup: Dict[str, Dict[str, str]] = {}
    for match in matches:
        dedup[match["external_id"]] = match
    return list(dedup.values())


class OdooClient:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {cfg.odoo_token.strip()}",
            }
        )

    def post_matches(self, matches: List[Dict[str, str]], retry_on_datetime_error: bool = True) -> Dict[str, str]:
        url = self.cfg.odoo_url.rstrip("/") + "/bhz/football/api/matches"
        prepared = [self._prepare_payload(match) for match in matches]
        payload_dict = {"matches": prepared}
        payload = json.dumps(payload_dict)
        response = self.session.post(url, data=payload, timeout=self.cfg.timeout)
        body = response.text
        if response.status_code >= 400:
            log.error(
                "[ERROR] Odoo retornou status %s. Body: %s\nPayload:\n%s",
                response.status_code,
                body[:1000],
                json.dumps(payload_dict, indent=2, ensure_ascii=False),
            )
            if retry_on_datetime_error and "time data" in body.lower():
                log.warning("[WARN] Odoo reclamou de data/hora. Ajustando formato e reenviando...")
                for match in prepared:
                    if "date" in match:
                        match["date"] = normalize_datetime_str(match["date"])
                return self.post_matches(prepared, retry_on_datetime_error=False)
            return {"ok": False, "status_code": response.status_code, "raw": body[:500]}
        try:
            return response.json()
        except Exception:
            return {"ok": True, "raw": body[:500]}

    def _prepare_payload(self, match: Dict[str, str]) -> Dict[str, str]:
        home = (match.get("home_team") or match.get("home") or "").strip() or "Time"
        away = (match.get("away_team") or match.get("away") or "").strip() or "Adversário"
        date_value = match.get("match_datetime") or match.get("date")
        try:
            normalized_date = normalize_datetime_str(date_value)
        except Exception:
            normalized_date = datetime.utcnow().strftime(NORMALIZED_DATETIME_FORMAT)
        competition = (match.get("competition") or "Campeonato Mineiro").strip() or "Campeonato Mineiro"
        source = (match.get("source") or "FlashScore").strip() or "FlashScore"
        venue = (match.get("venue") or match.get("stadium") or "A definir").strip() or "A definir"
        external_base = f"{source}:{home}:{away}:{normalized_date}"
        external_id = external_base
        return {
            "external_id": external_id,
            "competition": competition,
            "date": normalized_date,
            "match_datetime": normalized_date,
            "home_team": home,
            "away_team": away,
            "source": source,
            "venue": venue,
        }


def load_config() -> Config:
    teams_env = [t.strip() for t in os.getenv("TEAMS", "Cruzeiro,Atletico-MG,America-MG").split(",") if t.strip()]
    canonical_teams = [canonicalize_team(team) for team in teams_env]
    return Config(
        odoo_url=env_required("ODOO_URL"),
        odoo_token=env_required("ODOO_TOKEN"),
        teams=canonical_teams,
        days_back=int(os.getenv("DAYS_BACK", "7")),
        days_forward=int(os.getenv("DAYS_FORWARD", "180")),
        dry_run=os.getenv("DRY_RUN", "0").strip() == "1",
        timeout=int(os.getenv("HTTP_TIMEOUT", "45")),
    )


def collect_matches(cfg: Config, date_from: date, date_to: date) -> Tuple[List[Dict[str, str]], Dict[str, int]]:
    raw_matches = fetch_ge_mineiro_matches(cfg, cfg.teams, date_from, date_to)
    filtered: List[Dict[str, str]] = []
    per_team_counts: Dict[str, int] = {team: 0 for team in cfg.teams}

    for match in raw_matches:
        try:
            match_dt = normalize_datetime_str(match["match_datetime"])
        except Exception as exc:
            log.warning(f"[WARN] Ignorando jogo com data inválida: {exc}")
            continue

        home = canonicalize_team(match.get("home_team", ""))
        away = canonicalize_team(match.get("away_team", ""))
        match["home_team"] = home
        match["away_team"] = away
        match["match_datetime"] = match_dt

        involves_target = False
        for team in cfg.teams:
            if team.lower() == home.lower() or team.lower() == away.lower():
                per_team_counts[team] += 1
                involves_target = True
        if not involves_target:
            continue
        filtered.append(match)

    for team, qty in per_team_counts.items():
        log.info(f"[INFO] Jogos encontrados para {team}: {qty}")
    return filtered, per_team_counts


def print_summary(cfg: Config, per_team_counts: Dict[str, int], dedup_matches: List[Dict[str, str]]) -> None:
    log.info("[INFO] --- Resumo ---")
    for team in cfg.teams:
        log.info(f"[INFO] {team}: {per_team_counts.get(team, 0)} jogos antes da deduplicação")

    dedup_per_team: Dict[str, int] = {team: 0 for team in cfg.teams}
    for match in dedup_matches:
        home = match.get("home_team", "").lower()
        away = match.get("away_team", "").lower()
        for team in cfg.teams:
            team_slug = team.lower()
            if team_slug == home or team_slug == away:
                dedup_per_team[team] += 1
    for team in cfg.teams:
        log.info(f"[INFO] {team}: {dedup_per_team.get(team, 0)} jogos após deduplicação")

    log.info(f"[INFO] Total deduplicado: {len(dedup_matches)}")


def print_matches_table(matches: List[Dict[str, str]]) -> None:
    if not matches:
        return
    log.info("[INFO] --- Jogos (deduplicados) ---")
    for match in matches:
        venue = match.get("venue") or "Sem estádio"
        log.info(
            f"[INFO] {match['match_datetime']} | {match['home_team']} x {match['away_team']} "
            f"({match.get('competition')}) - {venue}"
        )


def main() -> int:
    cfg = load_config()
    log.info(f"[INFO] Times monitorados: {cfg.teams}")
    today = to_date(datetime.utcnow())
    date_from = to_date(today - timedelta(days=cfg.days_back))
    date_to = to_date(today + timedelta(days=cfg.days_forward))
    log.info(f"[INFO] Janela de busca: {date_from} -> {date_to}")

    matches, per_team_counts = collect_matches(cfg, date_from, date_to)
    dedup_matches = deduplicate(matches)
    print_summary(cfg, per_team_counts, dedup_matches)

    if not dedup_matches:
        log.warning("[WARN] Nenhum jogo encontrado para enviar.")
        return 0

    if cfg.dry_run:
        log.info("[INFO] DRY_RUN=1 ativo. Nenhum dado será enviado ao Odoo.")
        print_matches_table(dedup_matches)
        return 0

    odoo = OdooClient(cfg)
    response = odoo.post_matches(dedup_matches)
    log.info(f"[OK] Enviado para Odoo. Resposta: {json.dumps(response)[:500]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

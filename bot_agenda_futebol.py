# -*- coding: utf-8 -*-
import os
import sys
import json
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional

import requests

SPORTAPI_BASE = "https://sportapi7.p.rapidapi.com"
# IDs baseados no catálogo público da SportAPI (RapidAPI). Ajuste se o provider alterar.
DEFAULT_TEAM_IDS = {
    "cruzeiro": 4249,
    "atletico-mg": 4251,
    "america-mg": 4245,
}

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("bhz-football-bot")


def env_required(name: str) -> str:
    v = os.getenv(name, "")
    v = v.strip()
    if not v:
        raise SystemExit(f"[FATAL] Variável {name} não definida.")
    # evita header inválido por newline/whitespace
    if any(c in v for c in ["\n", "\r", "\t"]):
        raise SystemExit(f"[FATAL] Variável {name} contém caracteres inválidos (quebra de linha/tab). Regrave o Secret.")
    return v


def safe_join(base: str, path: str) -> str:
    base = base.strip()
    path = path.strip()
    if not base:
        return path
    if base.endswith("/") and path.startswith("/"):
        return base[:-1] + path
    if not base.endswith("/") and not path.startswith("/"):
        return base + "/" + path
    return base + path


@dataclass
class Config:
    # RapidAPI / SportAPI
    rapidapi_key: str
    rapidapi_host: str

    # Odoo
    odoo_url: str       # endpoint completo OU base
    odoo_token: str     # token puro (sem Bearer)

    # comportamento
    season: int
    days_back: int
    days_forward: int
    teams: List[str]
    country: str
    timeout: int = 45
    retry_max: int = 3


def normalize_team_key(name: str) -> str:
    return (name or "").strip().lower()


def build_team_id_map() -> Dict[str, int]:
    mapping = dict(DEFAULT_TEAM_IDS)
    raw = os.getenv("SPORTAPI_TEAM_IDS", "").strip()
    if raw:
        try:
            overrides = json.loads(raw)
            for k, v in overrides.items():
                try:
                    mapping[normalize_team_key(k)] = int(v)
                except (TypeError, ValueError):
                    log.warning(f"[WARN] SPORTAPI_TEAM_IDS ignorou valor inválido para '{k}': {v}")
        except json.JSONDecodeError:
            log.warning("[WARN] SPORTAPI_TEAM_IDS inválido (JSON). Ignorando override.")
    return mapping


class SportAPIClient:
    """
    Client para SportAPI (sportapi7.p.rapidapi.com)
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.base_url = SPORTAPI_BASE
        self.session = requests.Session()
        key = cfg.rapidapi_key.strip()
        host = cfg.rapidapi_host.strip()
        headers = {
            "x-rapidapi-key": key,
            "x-rapidapi-host": host,
            "X-RapidAPI-Key": key,  # alguns proxies exigem capitalização
            "X-RapidAPI-Host": host,
        }
        self.session.headers.update(headers)

    def _extract_list(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        for key in ("data", "response", "results", "items", "teams", "events"):
            items = payload.get(key)
            if isinstance(items, list):
                return items
        return []

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = safe_join(self.base_url, path)
        params = params or {}
        backoff = 2
        for attempt in range(1, self.cfg.retry_max + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.cfg.timeout)
            except requests.Timeout:
                log.warning(f"[WARN] SportAPI timeout (tentativa {attempt}/{self.cfg.retry_max}) em {path}")
                if attempt >= self.cfg.retry_max:
                    raise
                time.sleep(backoff)
                backoff *= 2
                continue
            except requests.RequestException as exc:
                log.warning(f"[WARN] SportAPI erro de rede (tentativa {attempt}/{self.cfg.retry_max}): {exc}")
                if attempt >= self.cfg.retry_max:
                    raise
                time.sleep(backoff)
                backoff *= 2
                continue

            if response.status_code == 403:
                log.error("[ERROR] RapidAPI retornou 403 (assinatura ausente ou bloqueada).")
                log.error(response.text[:300])
                raise SystemExit(1)

            if response.status_code == 429:
                log.warning(f"[WARN] RapidAPI limitou (429) tentativa {attempt}/{self.cfg.retry_max}")
                if attempt >= self.cfg.retry_max:
                    raise RuntimeError(f"RapidAPI 429 repetido em {url}")
                time.sleep(backoff)
                backoff *= 2
                continue

            if response.status_code >= 400:
                raise RuntimeError(f"RapidAPI {response.status_code} em {url}: {response.text[:300]}")

            try:
                return response.json()
            except ValueError as exc:
                raise RuntimeError(f"Resposta inválida da SportAPI em {url}: {exc}")

        raise RuntimeError(f"SportAPI falhou após {self.cfg.retry_max} tentativas em {path}")

    def search_team_id(self, team_name: str, country: str) -> Optional[int]:
        params: Dict[str, Any] = {"name": team_name}
        if country:
            params["country"] = country
        data = self._get("/api/v4/football/teams/search", params=params)
        teams = self._extract_list(data)
        if not teams:
            return None
        normalized_country = (country or "").strip().lower()
        for team in teams:
            if "id" not in team:
                continue
            team_country = ""
            country_block = team.get("country") or {}
            if isinstance(country_block, dict):
                team_country = (country_block.get("name") or country_block.get("iso") or "").lower()
            elif isinstance(country_block, str):
                team_country = country_block.lower()
            if normalized_country and team_country and normalized_country not in team_country:
                continue
            try:
                return int(team["id"])
            except (TypeError, ValueError):
                continue
        # fallback para o primeiro item válido
        for team in teams:
            try:
                return int(team["id"])
            except (KeyError, TypeError, ValueError):
                continue
        return None

    def events_by_team(self, team_id: int, dfrom: date, dto: date) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        params: Dict[str, Any] = {
            "page": 1,
            "per_page": 50,
            "timezone": "UTC",
            "from": dfrom.isoformat(),
            "to": dto.isoformat(),
        }
        while True:
            data = self._get(f"/api/v4/football/events/team/{team_id}", params=params)
            events.extend(self._extract_list(data))
            meta = data.get("meta") or {}
            next_page = meta.get("next_page") or meta.get("nextPage")
            if not next_page:
                break
            params["page"] = next_page
        return events


class OdooClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {cfg.odoo_token.strip()}",
        })

    def post_matches(self, matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        # aceita tanto endpoint completo quanto base + path
        url = self.cfg.odoo_url.strip()
        if not url.startswith("http"):
            raise RuntimeError("ODOO_URL inválida")

        # Se usuário passou só base, completa
        if not url.endswith("/bhz/football/api/matches"):
            # tenta completar automaticamente
            url = safe_join(url, "/bhz/football/api/matches")

        payload = {"matches": matches}
        r = self.session.post(url, data=json.dumps(payload), timeout=self.cfg.timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"Odoo {r.status_code} em {url}: {r.text[:500]}")
        try:
            return r.json()
        except Exception:
            return {"ok": True, "raw": r.text}


def _extract_team_name(team_info: Any) -> Optional[str]:
    if isinstance(team_info, dict):
        for key in ("name", "short_name", "display_name", "team_name", "abbr"):
            value = team_info.get(key)
            if value:
                return str(value)
    elif isinstance(team_info, str):
        team_info = team_info.strip()
        if team_info:
            return team_info
    return None


def _extract_score(score_info: Any) -> Optional[int]:
    if isinstance(score_info, dict):
        for key in ("current", "normal_time", "display", "total", "fulltime"):
            value = score_info.get(key)
            if isinstance(value, (int, float)):
                return int(value)
            if isinstance(value, str) and value.isdigit():
                return int(value)
    elif isinstance(score_info, (int, float)):
        return int(score_info)
    return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(value)
        except (ValueError, OSError):
            return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _get_event_datetime(event: Dict[str, Any]) -> Optional[datetime]:
    dt_value = (
        event.get("start_at")
        or event.get("startAt")
        or event.get("kickoff_at")
        or event.get("datetime")
        or event.get("match_start")
        or event.get("matchStart")
    )
    return _parse_datetime(dt_value)


def normalize_event(event: Dict[str, Any], dt_override: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    event_id = event.get("id") or event.get("event_id") or event.get("fixture_id")
    if not event_id:
        return None

    dt_parsed = dt_override or _get_event_datetime(event)
    if not dt_parsed:
        return None

    home_name = _extract_team_name(event.get("home_team") or event.get("homeTeam") or event.get("home"))
    away_name = _extract_team_name(event.get("away_team") or event.get("awayTeam") or event.get("away"))
    if not home_name or not away_name:
        return None

    tournament = event.get("tournament") or event.get("league") or event.get("competition") or {}
    if isinstance(tournament, str):
        competition_name = tournament
        tournament_season = None
    else:
        competition_name = (tournament or {}).get("name")
        tournament_season = (tournament or {}).get("season")

    venue_block = event.get("venue") or {}
    if isinstance(venue_block, dict):
        venue_name = venue_block.get("name")
    else:
        venue_name = venue_block

    status_block = event.get("status") or {}
    if isinstance(status_block, dict):
        status = status_block.get("type") or status_block.get("short") or status_block.get("description")
    else:
        status = status_block

    round_info = event.get("round") or event.get("stage")
    if isinstance(round_info, dict):
        round_name = round_info.get("name")
    else:
        round_name = round_info

    return {
        "external_id": str(event_id),
        "match_datetime": dt_parsed.isoformat(),
        "competition": competition_name,
        "season": event.get("season") or tournament_season,
        "round": round_name,
        "home_team": home_name,
        "away_team": away_name,
        "home_goals": _extract_score(event.get("home_score") or event.get("homeScore")),
        "away_goals": _extract_score(event.get("away_score") or event.get("awayScore")),
        "status": status,
        "venue": venue_name,
        "raw": event,
    }


def main() -> int:
    # ---- Config via env/secrets ----
    cfg = Config(
        rapidapi_key=env_required("RAPIDAPI_KEY"),
        rapidapi_host=env_required("RAPIDAPI_HOST"),

        odoo_url=env_required("ODOO_URL"),
        odoo_token=env_required("ODOO_TOKEN"),

        season=int(os.getenv("SEASON", "2026").strip() or "2026"),
        days_back=int(os.getenv("DAYS_BACK", "7").strip() or "7"),
        days_forward=int(os.getenv("DAYS_FORWARD", "180").strip() or "180"),
        teams=[t.strip() for t in (os.getenv("TEAMS", "Cruzeiro,Atletico-MG,America-MG")).split(",") if t.strip()],
        country=os.getenv("COUNTRY", "Brazil").strip() or "Brazil",
        timeout=int(os.getenv("HTTP_TIMEOUT", "45").strip() or "45"),
        retry_max=int(os.getenv("RETRY_MAX", "3").strip() or "3"),
    )

    log.info(f"[INFO] Temporada: {cfg.season}")
    today = datetime.utcnow().date()
    dfrom = today - timedelta(days=cfg.days_back)
    dto = today + timedelta(days=cfg.days_forward)
    log.info(f"[INFO] Janela: {dfrom} -> {dto}")
    log.info(f"[INFO] Times: {cfg.teams}")
    log.info(f"[INFO] RapidAPI base: {SPORTAPI_BASE}")
    log.info(f"[INFO] RapidAPI host: {cfg.rapidapi_host}")
    log.info(f"[INFO] Odoo endpoint: {cfg.odoo_url}")

    api = SportAPIClient(cfg)
    odoo = OdooClient(cfg)
    team_id_map = build_team_id_map()

    all_matches: List[Dict[str, Any]] = []
    for team_name in cfg.teams:
        log.info(f"[INFO] Resolvendo team_id para '{team_name}'...")
        team_id: Optional[int] = None
        try:
            team_id = api.search_team_id(team_name, country=cfg.country)
        except Exception as exc:
            log.warning(f"[WARN] Falha na busca da SportAPI para '{team_name}': {exc}")

        if not team_id:
            fallback = team_id_map.get(normalize_team_key(team_name))
            if fallback:
                team_id = fallback
                log.info(f"[INFO] Fallback TEAM_IDS '{team_name}' -> {team_id}")

        if not team_id:
            log.error(f"[ERROR] Não foi possível resolver team_id para '{team_name}'. Pulando.")
            continue

        log.info(f"[INFO] Time '{team_name}' -> team_id {team_id}")

        try:
            events = api.events_by_team(team_id, dfrom=dfrom, dto=dto)
        except SystemExit:
            raise
        except Exception as exc:
            log.error(f"[ERROR] Falha ao listar jogos de {team_name}: {exc}")
            continue

        log.info(f"[INFO] Quantidade de jogos retornados (bruto) para {team_name}: {len(events)}")

        normalized_events: List[Dict[str, Any]] = []
        for event in events:
            event_dt = _get_event_datetime(event)
            if not event_dt:
                continue
            if event_dt.date() < dfrom or event_dt.date() > dto:
                continue
            normalized = normalize_event(event, dt_override=event_dt)
            if normalized:
                normalized_events.append(normalized)

        log.info(f"[INFO] Quantidade de jogos encontrados para {team_name}: {len(normalized_events)}")
        all_matches.extend(normalized_events)

        time.sleep(0.5)  # reduz chance de rate-limit

    # dedup por external_id
    dedup: Dict[str, Dict[str, Any]] = {}
    for m in all_matches:
        dedup[m["external_id"]] = m
    matches = list(dedup.values())
    log.info(f"[INFO] Total normalizado (dedup): {len(matches)}")

    if not matches:
        log.info("[INFO] Nada para enviar.")
        return 0

    resp = odoo.post_matches(matches)
    log.info(f"[OK] Enviado para Odoo. Resposta: {json.dumps(resp)[:500]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

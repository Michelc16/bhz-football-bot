# -*- coding: utf-8 -*-
import os
import json
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import requests

DEFAULT_RAPIDAPI_BASE = "https://free-api-live-football-data.p.rapidapi.com"
NORMALIZED_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
# IDs baseados no catálogo público da SportAPI (RapidAPI). Ajuste se necessário.
DEFAULT_TEAM_IDS = {
    "cruzeiro": 4249,
    "atletico-mg": 4251,
    "america-mg": 4245,
}

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("bhz-football-bot")


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"[FATAL] Variável {name} não definida.")
    if any(c in value for c in ["\n", "\r", "\t"]):
        raise SystemExit(
            f"[FATAL] Variável {name} contém caracteres inválidos (quebra de linha/tab). Regrave o Secret."
        )
    return value


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
    rapidapi_key: str
    rapidapi_host: str
    rapidapi_base: str
    odoo_url: str
    odoo_token: str
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


class FreeFootballAPIClient:
    """Client para Free API Live Football Data (RapidAPI)."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()
        key = cfg.rapidapi_key.strip()
        host = cfg.rapidapi_host.strip()
        headers = {
            "x-rapidapi-key": key,
            "x-rapidapi-host": host,
            "X-RapidAPI-Key": key,
            "X-RapidAPI-Host": host,
        }
        self.session.headers.update(headers)

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = safe_join(self.cfg.rapidapi_base, path)
        params = params or {}
        backoff = 2
        for attempt in range(1, self.cfg.retry_max + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.cfg.timeout)
            except requests.Timeout:
                log.warning(
                    f"[WARN] Free API timeout (tentativa {attempt}/{self.cfg.retry_max}) em {path}"
                )
                if attempt >= self.cfg.retry_max:
                    raise
                time.sleep(backoff)
                backoff *= 2
                continue
            except requests.RequestException as exc:
                log.warning(
                    f"[WARN] Free API erro de rede (tentativa {attempt}/{self.cfg.retry_max}): {exc}"
                )
                if attempt >= self.cfg.retry_max:
                    raise
                time.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code == 403:
                log.error("[ERROR] RapidAPI retornou 403 (assinatura ausente ou bloqueada).")
                log.error(resp.text[:300])
                raise SystemExit(1)

            if resp.status_code == 429:
                log.warning(f"[WARN] RapidAPI limitou (429) tentativa {attempt}/{self.cfg.retry_max}")
                if attempt >= self.cfg.retry_max:
                    raise RuntimeError(f"RapidAPI 429 repetido em {url}")
                time.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code >= 400:
                raise RuntimeError(f"RapidAPI {resp.status_code} em {url}: {resp.text[:300]}")

            try:
                return resp.json()
            except ValueError as exc:
                raise RuntimeError(f"Resposta inválida da Free API em {url}: {exc}")

        raise RuntimeError(f"Free API falhou após {self.cfg.retry_max} tentativas em {path}")

    def fixtures_by_team(self, team_id: int, season: int, dfrom: datetime, dto: datetime) -> List[Dict[str, Any]]:
        params = {
            "team": team_id,
            "season": season,
            "from": dfrom.date().isoformat(),
            "to": dto.date().isoformat(),
        }
        data = self._get("/fixtures", params=params)
        fixtures = (
            data.get("response")
            or data.get("fixtures")
            or data.get("data")
            or data.get("results")
            or []
        )
        if not isinstance(fixtures, list):
            return []
        return fixtures


class OdooClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {cfg.odoo_token.strip()}",
        })

    def post_matches(self, matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        url = self.cfg.odoo_url.strip()
        if not url.startswith("http"):
            raise RuntimeError("ODOO_URL inválida")
        if not url.endswith("/bhz/football/api/matches"):
            url = safe_join(url, "/bhz/football/api/matches")
        payload = {"matches": matches}
        resp = self.session.post(url, data=json.dumps(payload), timeout=self.cfg.timeout)
        if resp.status_code >= 400:
            raise RuntimeError(f"Odoo {resp.status_code} em {url}: {resp.text[:500]}")
        try:
            return resp.json()
        except Exception:
            return {"ok": True, "raw": resp.text}


def _extract_team_name(team_info: Any) -> Optional[str]:
    if isinstance(team_info, dict):
        for key in ("name", "shortName", "short_name", "displayName", "display_name"):
            val = team_info.get(key)
            if val:
                return str(val)
    elif isinstance(team_info, str):
        team_info = team_info.strip()
        if team_info:
            return team_info
    return None


def _extract_score(score_info: Any) -> Optional[int]:
    if isinstance(score_info, dict):
        for key in ("current", "display", "total", "normalTime", "normal_time"):
            val = score_info.get(key)
            if isinstance(val, (int, float)):
                return int(val)
            if isinstance(val, str) and val.isdigit():
                return int(val)
    elif isinstance(score_info, (int, float)):
        return int(score_info)
    elif isinstance(score_info, str) and score_info.isdigit():
        return int(score_info)
    return None


def _parse_timestamp(value: Any) -> Optional[datetime]:
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
        if value.isdigit():
            try:
                return datetime.utcfromtimestamp(int(value))
            except (ValueError, OSError):
                return None
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def normalize_datetime(value: str) -> Optional[str]:
    """
    Converte qualquer string datetime/ISO para o formato exigido pelo Odoo (YYYY-MM-DD HH:MM:SS).
    Mantemos o horário informado pela API (sem converter fuso) e removemos timezone se existir.
    """
    if value is None:
        return None
    dt = _parse_timestamp(value)
    if not dt:
        return None
    if dt.tzinfo is not None:
        # mantemos o horário informado, apenas removendo a info de fuso
        dt = dt.replace(tzinfo=None)
    return dt.strftime(NORMALIZED_DATETIME_FORMAT)


def parse_normalized_datetime(value: str) -> Optional[datetime]:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, NORMALIZED_DATETIME_FORMAT)
    except ValueError:
        return None


def normalize_fixture(fixture_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    fixture = fixture_payload.get("fixture") or {}
    league = fixture_payload.get("league") or {}
    teams = fixture_payload.get("teams") or {}
    venue = fixture_payload.get("venue") or {}
    goals = fixture_payload.get("goals") or {}

    event_id = fixture.get("id") or fixture_payload.get("id")
    if not event_id:
        return None

    dt_value = (
        fixture.get("date")
        or fixture.get("timestamp")
        or fixture_payload.get("date")
    )
    dt_parsed = _parse_timestamp(dt_value)
    if not dt_parsed:
        return None

    normalized_dt = normalize_datetime(dt_parsed.isoformat())
    if not normalized_dt:
        return None

    home_name = _extract_team_name((teams.get("home") or {}))
    away_name = _extract_team_name((teams.get("away") or {}))
    if not home_name or not away_name:
        return None

    competition_name = league.get("name")
    competition_season = league.get("season")

    status_block = fixture.get("status") or {}
    if isinstance(status_block, dict):
        status = status_block.get("long") or status_block.get("short") or status_block.get("type")
    else:
        status = status_block

    venue_name = venue.get("name") if isinstance(venue, dict) else venue

    round_info = league.get("round") or fixture_payload.get("round")
    if isinstance(round_info, dict):
        round_name = round_info.get("name")
    else:
        round_name = round_info

    return {
        "external_id": str(event_id),
        "match_datetime": normalized_dt,
        "competition": competition_name,
        "season": fixture.get("season") or competition_season,
        "round": round_name,
        "home_team": home_name,
        "away_team": away_name,
        "home_goals": _extract_score(goals.get("home")),
        "away_goals": _extract_score(goals.get("away")),
        "status": status,
        "venue": venue_name,
        "raw": fixture_payload,
    }


def event_matches_team(event: Dict[str, Any], team_name: str) -> bool:
    target = (team_name or "").strip().lower()
    if not target:
        return True
    for key in ("home_team", "away_team"):
        name = (event.get(key) or "").strip().lower()
        if not name:
            continue
        if name == target or target in name or name in target:
            return True
    return False


def main() -> int:
    cfg = Config(
        rapidapi_key=env_required("RAPIDAPI_KEY"),
        rapidapi_host=env_required("RAPIDAPI_HOST"),
        rapidapi_base=os.getenv("RAPIDAPI_BASE", DEFAULT_RAPIDAPI_BASE).strip() or DEFAULT_RAPIDAPI_BASE,
        odoo_url=env_required("ODOO_URL"),
        odoo_token=env_required("ODOO_TOKEN"),
        season=int(os.getenv("SEASON", "2026").strip() or "2026"),
        days_back=int(os.getenv("DAYS_BACK", "7").strip() or "7"),
        days_forward=int(os.getenv("DAYS_FORWARD", "180").strip() or "180"),
        teams=[t.strip() for t in os.getenv("TEAMS", "Cruzeiro,Atletico-MG,America-MG").split(",") if t.strip()],
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
    log.info(f"[INFO] RapidAPI base: {cfg.rapidapi_base}")
    log.info(f"[INFO] RapidAPI host: {cfg.rapidapi_host}")
    log.info("[INFO] Credenciais RapidAPI/Odoo mascaradas por segurança.")
    log.info(f"[INFO] Odoo endpoint: {cfg.odoo_url}")

    api = FreeFootballAPIClient(cfg)
    odoo = OdooClient(cfg)
    team_id_map = build_team_id_map()

    all_matches: List[Dict[str, Any]] = []
    endpoint_success_count = 0
    for team_name in cfg.teams:
        log.info(f"[INFO] Resolvendo team_id para '{team_name}'...")
        team_id: Optional[int] = team_id_map.get(normalize_team_key(team_name))
        if team_id:
            log.info(f"[INFO] TEAM_IDS '{team_name}' -> {team_id}")
        if not team_id:
            log.error(f"[ERROR] Não foi possível resolver team_id para '{team_name}'. Pulando.")
            continue

        log.info(f"[INFO] Time '{team_name}' -> team_id {team_id}")
        try:
            fixtures = api.fixtures_by_team(team_id, cfg.season, dfrom, dto)
        except SystemExit:
            raise
        except Exception as exc:
            log.error(f"[ERROR] Falha ao buscar jogos de {team_name}: {exc}")
            continue

        endpoint_success_count += 1

        log.info(f"[INFO] Quantidade de jogos retornados (bruto) para {team_name}: {len(fixtures)}")

        team_matches: List[Dict[str, Any]] = []
        for fx in fixtures:
            normalized = normalize_fixture(fx)
            if not normalized:
                continue
            event_dt = parse_normalized_datetime(normalized["match_datetime"])
            if not event_dt:
                log.warning(f"[WARN] Datetime inválido no evento {normalized.get('external_id')} - ignorando.")
                continue
            if event_dt.date() < dfrom or event_dt.date() > dto:
                continue
            if not event_matches_team(normalized, team_name):
                continue
            team_matches.append(normalized)

        if not team_matches:
            log.warning(f"[WARN] Nenhuma partida válida encontrada para '{team_name}' (team_id {team_id}).")
            continue

        log.info(f"[INFO] Quantidade de jogos encontrados para {team_name}: {len(team_matches)}")
        all_matches.extend(team_matches)
        time.sleep(0.5)

    dedup: Dict[str, Dict[str, Any]] = {}
    for match in all_matches:
        dedup[match["external_id"]] = match
    matches = list(dedup.values())
    log.info(f"[INFO] Total normalizado (dedup): {len(matches)}")

    if endpoint_success_count == 0:
        log.error("[ERROR] Nenhum endpoint válido encontrado para todos os times. Abortando.")
        return 1

    if not matches:
        log.info("[INFO] Nada para enviar.")
        return 0

    resp = odoo.post_matches(matches)
    log.info(f"[OK] Enviado para Odoo. Resposta: {json.dumps(resp)[:500]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

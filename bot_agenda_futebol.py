# -*- coding: utf-8 -*-
import os
import sys
import json
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

import requests

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
    # RapidAPI / API-Football
    rapidapi_key: str
    rapidapi_host: str
    rapidapi_base: str  # ex: https://api-football-v1.p.rapidapi.com

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


class RapidApiFootball:
    """
    API-Football via RapidAPI
    Base típica: https://api-football-v1.p.rapidapi.com
    Endpoints: /v3/teams, /v3/fixtures
    """
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "X-RapidAPI-Key": cfg.rapidapi_key.strip(),
            "X-RapidAPI-Host": cfg.rapidapi_host.strip(),
        })

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = safe_join(self.cfg.rapidapi_base, path)
        r = self.session.get(url, params=params, timeout=self.cfg.timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"RapidAPI {r.status_code} em {url}: {r.text[:300]}")
        return r.json()

    def find_team_id(self, team_name: str, country: str) -> int:
        data = self._get("/v3/teams", {"search": team_name, "country": country})
        items = data.get("response") or []
        if not items:
            raise RuntimeError(f"Time não encontrado na API-Football: {team_name} ({country})")
        # pega o primeiro match
        return int(items[0]["team"]["id"])

    def fixtures_by_team(self, team_id: int, season: int, dfrom: date, dto: date) -> List[Dict[str, Any]]:
        data = self._get("/v3/fixtures", {
            "team": team_id,
            "season": season,
            "from": dfrom.isoformat(),
            "to": dto.isoformat(),
        })
        return data.get("response") or []


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


def normalize_fixture(fx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Transforma o fixture da API-Football em payload que seu módulo Odoo espera.
    Ajuste os campos se o seu endpoint exigir algo diferente.
    """
    fixture = fx.get("fixture") or {}
    league = fx.get("league") or {}
    teams = fx.get("teams") or {}
    goals = fx.get("goals") or {}

    # data do jogo
    dt = fixture.get("date")
    if not dt:
        return None

    # Alguns módulos preferem datetime ISO sem timezone
    # API-Football vem tipo: 2026-02-01T20:00:00+00:00
    # vamos manter ISO (Odoo costuma aceitar)
    external_id = str(fixture.get("id", ""))

    home = (teams.get("home") or {}).get("name")
    away = (teams.get("away") or {}).get("name")

    # Segurança
    if not external_id or not home or not away:
        return None

    return {
        "external_id": external_id,
        "match_datetime": dt,
        "competition": league.get("name"),
        "season": league.get("season"),
        "round": league.get("round"),
        "home_team": home,
        "away_team": away,
        "home_goals": goals.get("home"),
        "away_goals": goals.get("away"),
        "status": (fixture.get("status") or {}).get("short"),
        "venue": (fixture.get("venue") or {}).get("name"),
        "raw": fx,  # útil pra debug
    }


def main() -> int:
    # ---- Config via env/secrets ----
    cfg = Config(
        rapidapi_key=env_required("RAPIDAPI_KEY"),
        rapidapi_host=env_required("RAPIDAPI_HOST"),
        rapidapi_base=os.getenv("RAPIDAPI_BASE", "https://api-football-v1.p.rapidapi.com").strip(),

        odoo_url=env_required("ODOO_URL"),
        odoo_token=env_required("ODOO_TOKEN"),

        season=int(os.getenv("SEASON", "2026").strip() or "2026"),
        days_back=int(os.getenv("DAYS_BACK", "7").strip() or "7"),
        days_forward=int(os.getenv("DAYS_FORWARD", "180").strip() or "180"),
        teams=[t.strip() for t in (os.getenv("TEAMS", "Cruzeiro,Atletico-MG,America-MG")).split(",") if t.strip()],
        country=os.getenv("COUNTRY", "Brazil").strip() or "Brazil",
    )

    log.info(f"[INFO] Temporada: {cfg.season}")
    today = datetime.utcnow().date()
    dfrom = today - timedelta(days=cfg.days_back)
    dto = today + timedelta(days=cfg.days_forward)
    log.info(f"[INFO] Janela: {dfrom} -> {dto}")
    log.info(f"[INFO] Times: {cfg.teams}")
    log.info(f"[INFO] RapidAPI base: {cfg.rapidapi_base}")
    log.info(f"[INFO] Odoo endpoint: {cfg.odoo_url}")

    api = RapidApiFootball(cfg)
    odoo = OdooClient(cfg)

    all_matches: List[Dict[str, Any]] = []
    for team_name in cfg.teams:
        log.info(f"[INFO] Resolvendo team_id para '{team_name}'...")
        team_id = api.find_team_id(team_name, country=cfg.country)
        log.info(f"[INFO] team_id '{team_name}': {team_id}")

        fixtures = api.fixtures_by_team(team_id, season=cfg.season, dfrom=dfrom, dto=dto)
        log.info(f"[INFO] Fixtures retornados para {team_name}: {len(fixtures)}")

        for fx in fixtures:
            nm = normalize_fixture(fx)
            if nm:
                all_matches.append(nm)

        # evita rate-limit
        time.sleep(0.25)

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

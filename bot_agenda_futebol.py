import os
import sys
import json
import time
import argparse
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple, Set

import requests


TZ_SP = ZoneInfo("America/Sao_Paulo")


def _now_sp() -> datetime:
    return datetime.now(TZ_SP)


def _dt_to_odoo_str(dt: datetime) -> str:
    # Odoo espera "YYYY-MM-DD HH:MM:SS"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _fatal(msg: str) -> None:
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(1)


def _info(msg: str) -> None:
    print(f"[INFO] {msg}")


def _warn(msg: str) -> None:
    print(f"[WARN] {msg}", file=sys.stderr)


def _get_env(name: str, required: bool = True, default: Optional[str] = None) -> Optional[str]:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        _fatal(f"Variável {name} não definida.")
    return val


def _norm_bearer(token: str) -> str:
    t = token.strip()
    if t.lower().startswith("bearer "):
        t = t.split(" ", 1)[1].strip()
    return t


@dataclass
class RapidAPIConfig:
    base: str
    key: str
    host: str
    timeout: int = 30


class APIFootballClient:
    """
    Client para API-Football via RapidAPI.
    Base típica: https://api-football-v1.p.rapidapi.com/v3
    Headers: x-rapidapi-key, x-rapidapi-host
    """

    def __init__(self, cfg: RapidAPIConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "x-rapidapi-key": cfg.key,
            "x-rapidapi-host": cfg.host,
            "accept": "application/json",
        })

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = self.cfg.base.rstrip("/") + "/" + path.lstrip("/")
        r = self.session.get(url, params=params, timeout=self.cfg.timeout)
        if r.status_code >= 400:
            # Tenta imprimir contexto útil
            try:
                payload = r.json()
            except Exception:
                payload = r.text
            _fatal(f"API-Football GET {url} falhou: {r.status_code} -> {payload}")
        try:
            return r.json()
        except Exception as e:
            _fatal(f"Resposta JSON inválida da API-Football em {url}: {e}")

    def find_team_id(self, team_name: str, country: str = "Brazil") -> int:
        """
        Resolve team_id via endpoint /teams (search).
        Retorna o primeiro match exato (case-insensitive) ou o primeiro resultado.
        """
        data = self._get("teams", {"search": team_name, "country": country})
        resp = data.get("response") or []
        if not resp:
            _fatal(f"Não encontrei time '{team_name}' na API-Football (country={country}).")

        # Tenta match exato pelo nome
        name_l = team_name.strip().lower()
        for item in resp:
            team = (item.get("team") or {})
            nm = str(team.get("name") or "").strip().lower()
            if nm == name_l:
                return int(team["id"])

        # Se não achou exato, pega o primeiro
        team = (resp[0].get("team") or {})
        return int(team["id"])

    def get_fixtures_by_team(
        self,
        team_id: int,
        season: int,
        date_from: date,
        date_to: date,
        timezone: str = "America/Sao_Paulo",
    ) -> List[Dict[str, Any]]:
        """
        Busca jogos (fixtures) por time, temporada e intervalo de datas.
        """
        params = {
            "team": team_id,
            "season": season,
            "from": date_from.isoformat(),
            "to": date_to.isoformat(),
            "timezone": timezone,
        }
        data = self._get("fixtures", params)
        return data.get("response") or []


class OdooMatchesClient:
    def __init__(self, odoo_url: str, odoo_token: str, timeout: int = 30):
        self.url = odoo_url.rstrip("/")
        self.token = _norm_bearer(odoo_token)
        self.timeout = timeout
        self.session = requests.Session()

    def post_matches(self, source: str, matches: List[Dict[str, Any]], dry_run: bool = False) -> Dict[str, Any]:
        payload = {"source": source, "matches": matches}

        if dry_run:
            _info(f"[DRY-RUN] Não enviando ao Odoo. Seriam {len(matches)} partidas.")
            # retorna algo “fake”
            return {"dry_run": True, "count": len(matches), "payload_preview": payload}

        r = self.session.post(
            self.url,
            json=payload,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            timeout=self.timeout,
        )
        if r.status_code >= 400:
            try:
                data = r.json()
            except Exception:
                data = r.text
            _fatal(f"Odoo POST falhou: {r.status_code} -> {data}")

        try:
            return r.json()
        except Exception:
            return {"status_code": r.status_code, "text": r.text}


def _parse_fixture_to_match(fx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Converte fixture da API-Football para o formato do seu módulo Odoo.
    Campos esperados no endpoint do Odoo (conforme seu exemplo):
      - external_id, match_datetime, competition, round, home_team, away_team,
        stadium, city, broadcast, ticket_url
    """
    fixture = fx.get("fixture") or {}
    league = fx.get("league") or {}
    teams = fx.get("teams") or {}
    venue = fixture.get("venue") or {}
    status = (fixture.get("status") or {}).get("short") or ""

    fixture_id = fixture.get("id")
    if not fixture_id:
        return None

    # data/hora já vem ajustada com timezone se você passou timezone=America/Sao_Paulo
    # Geralmente vem em fixture.date (ISO).
    dt_iso = fixture.get("date")
    if not dt_iso:
        return None

    # Parse ISO robusto (pode vir com offset)
    try:
        dt = datetime.fromisoformat(dt_iso.replace("Z", "+00:00"))
        # Converte para São Paulo
        dt = dt.astimezone(TZ_SP)
    except Exception:
        # fallback
        return None

    home = (teams.get("home") or {}).get("name") or ""
    away = (teams.get("away") or {}).get("name") or ""

    competition = str(league.get("name") or "").strip()
    round_name = str(league.get("round") or "").strip()

    stadium = str(venue.get("name") or "").strip()
    city = str(venue.get("city") or "").strip()

    # API-Football normalmente não fornece transmissão/ingresso.
    # Mantemos vazio.
    broadcast = ""
    ticket_url = ""

    # external_id estável (upsert)
    external_id = f"AFB-{fixture_id}"

    return {
        "external_id": external_id,
        "match_datetime": _dt_to_odoo_str(dt),
        "competition": competition,
        "round": round_name,
        "home_team": home,
        "away_team": away,
        "stadium": stadium,
        "city": city,
        "broadcast": broadcast,
        "ticket_url": ticket_url,
        # Se seu endpoint aceitar campos extras, você pode enviar também:
        # "status": status,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="BHZ Football Bot - Sync fixtures to Odoo (API-Football via RapidAPI)")
    parser.add_argument("--dry-run", action="store_true", help="Não envia ao Odoo, só mostra contagem")
    parser.add_argument("--season", type=int, default=int(os.getenv("FOOTBALL_SEASON", str(_now_sp().year))),
                        help="Temporada (ex: 2026). Default: ano atual ou env FOOTBALL_SEASON")
    parser.add_argument("--days-back", type=int, default=int(os.getenv("DAYS_BACK", "7")),
                        help="Quantos dias para trás buscar. Default 7 (env DAYS_BACK)")
    parser.add_argument("--days-ahead", type=int, default=int(os.getenv("DAYS_AHEAD", "180")),
                        help="Quantos dias para frente buscar. Default 180 (env DAYS_AHEAD)")
    parser.add_argument("--teams", type=str, default=os.getenv("TEAMS", "Cruzeiro,Atletico-MG,America-MG"),
                        help="Lista de times separados por vírgula. Default: Cruzeiro,Atletico-MG,America-MG (env TEAMS)")
    args = parser.parse_args()

    # ---- Config env ----
    apifut_base = _get_env("APIFUT_BASE", required=True)
    rapid_key = _get_env("RAPIDAPI_KEY", required=True)
    rapid_host = _get_env("RAPIDAPI_HOST", required=True)

    odoo_url = _get_env("ODOO_URL", required=True)
    odoo_token = _get_env("ODOO_TOKEN", required=True)

    # ---- Clients ----
    api = APIFootballClient(RapidAPIConfig(base=apifut_base, key=rapid_key, host=rapid_host))
    odoo = OdooMatchesClient(odoo_url=odoo_url, odoo_token=odoo_token)

    # ---- Parameters ----
    today = _now_sp().date()
    date_from = today - timedelta(days=args.days_back)
    date_to = today + timedelta(days=args.days_ahead)

    teams = [t.strip() for t in args.teams.split(",") if t.strip()]
    if not teams:
        _fatal("Nenhum time definido (use --teams ou env TEAMS).")

    _info(f"Temporada: {args.season}")
    _info(f"Janela: {date_from.isoformat()} -> {date_to.isoformat()}")
    _info(f"Times: {teams}")
    _info(f"Odoo endpoint: {odoo_url}")

    # ---- Resolve team IDs ----
    team_ids: Dict[str, int] = {}
    for t in teams:
        _info(f"Resolvendo time_id para '{t}'...")
        tid = api.find_team_id(t, country="Brazil")
        team_ids[t] = tid
        _info(f"  -> {t} = {tid}")

        # Pequena pausa para evitar rate-limit em contas free
        time.sleep(0.3)

    # ---- Fetch fixtures ----
    fixtures_all: List[Dict[str, Any]] = []
    for t, tid in team_ids.items():
        _info(f"Buscando fixtures de {t} (id={tid})...")
        fx = api.get_fixtures_by_team(team_id=tid, season=args.season, date_from=date_from, date_to=date_to,
                                      timezone="America/Sao_Paulo")
        _info(f"  -> {len(fx)} fixtures retornados")
        fixtures_all.extend(fx)
        time.sleep(0.3)

    # ---- Convert + dedupe ----
    matches: List[Dict[str, Any]] = []
    seen_ext: Set[str] = set()

    for fx in fixtures_all:
        m = _parse_fixture_to_match(fx)
        if not m:
            continue
        ext = m["external_id"]
        if ext in seen_ext:
            continue
        seen_ext.add(ext)

        # Filtra partidas sem nome de time (por segurança)
        if not m.get("home_team") or not m.get("away_team"):
            continue

        matches.append(m)

    # Ordena por data
    try:
        matches.sort(key=lambda x: x.get("match_datetime") or "")
    except Exception:
        pass

    _info(f"Total de partidas após dedupe/normalização: {len(matches)}")

    if not matches:
        _warn("Nenhuma partida encontrada para enviar.")
        return 0

    # ---- Post to Odoo ----
    source = os.getenv("SOURCE", "rapidapi_api_football")
    resp = odoo.post_matches(source=source, matches=matches, dry_run=args.dry_run)

    _info("Resposta do Odoo:")
    print(json.dumps(resp, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

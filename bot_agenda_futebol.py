#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bot externo para coletar agenda de jogos (ex: Mineiro 2026) e enviar para o endpoint
do módulo Odoo (Agenda de Futebol BHZ) via POST /bhz/football/api/matches.

Este arquivo NÃO depende de .env. Tudo é lido de variáveis de ambiente.
Exemplo:
  export ODOO_URL="https://SEU_DB.dev.odoo.com/bhz/football/api/matches"
  export ODOO_TOKEN="SEU_TOKEN"
  export APIFUT_TOKEN="SEU_TOKEN_API_FUTEBOL"
  python bot_agenda_futebol.py
"""

import os
import re
import time
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

import requests


# =========================
# Config (ENV VARS)
# =========================
ODOO_URL = os.getenv("ODOO_URL", "").strip()
ODOO_TOKEN = os.getenv("ODOO_TOKEN", "").strip()

APIFUT_BASE = os.getenv("APIFUT_BASE", "https://www.api-futebol.com.br").strip()
APIFUT_TOKEN = os.getenv("APIFUT_TOKEN", "").strip()

# comportamento do bot
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))
RETRY_MAX = int(os.getenv("RETRY_MAX", "2"))
RETRY_SLEEP_SECONDS = int(os.getenv("RETRY_SLEEP_SECONDS", "10"))

# alvo (por padrão: Cruzeiro, Atlético-MG, América-MG)
TARGET_TEAM_ALIASES = {
    "cruzeiro": "Cruzeiro",
    "cruzeiro ec": "Cruzeiro",
    "cruzeiro-esporte-clube": "Cruzeiro",

    "atlético": "Atlético-MG",
    "atletico": "Atlético-MG",
    "atlético-mg": "Atlético-MG",
    "atletico-mg": "Atlético-MG",
    "clube-atletico-mineiro": "Atlético-MG",
    "atlético mineiro": "Atlético-MG",
    "atletico mineiro": "Atlético-MG",

    "américa": "América-MG",
    "america": "América-MG",
    "américa-mg": "América-MG",
    "america-mg": "América-MG",
    "america futebol clube": "América-MG",
}


# =========================
# Util
# =========================
def die(msg: str, code: int = 2) -> None:
    raise SystemExit(f"[FATAL] {msg}")


def slug(s: str) -> str:
    s = (s or "").strip().lower()
    # troca múltiplos espaços por "-"
    s = re.sub(r"\s+", "-", s)
    # remove caracteres estranhos, mantendo letras/números/traço e alguns acentos comuns
    s = re.sub(r"[^a-z0-9\-áàâãéêíóôõúç\-]", "", s)
    s = re.sub(r"\-+", "-", s).strip("-")
    return s


def normalize_team(name: str) -> Optional[str]:
    """
    Normaliza nomes variantes para o padrão usado no Odoo.
    Se não for um dos times-alvo, retorna None.
    """
    raw = (name or "").strip()
    if not raw:
        return None

    key = slug(raw).replace("-", " ").strip()
    key2 = slug(raw).strip()

    # remove "saf" e ruídos comuns
    key = re.sub(r"\bsaf\b", "", key).strip()
    key2 = re.sub(r"\bsaf\b", "", key2).strip()

    # tenta match direto
    if key in TARGET_TEAM_ALIASES:
        return TARGET_TEAM_ALIASES[key]
    if key2 in TARGET_TEAM_ALIASES:
        return TARGET_TEAM_ALIASES[key2]

    # fallback: contém palavras-chave
    k = key.replace(" ", "")
    if "cruzeiro" in k:
        return "Cruzeiro"
    if "atletico" in k or "atlético" in k:
        return "Atlético-MG"
    if "america" in k or "américa" in k:
        return "América-MG"

    return None


def parse_datetime_to_odoo(value: str) -> Optional[str]:
    """
    Converte datas para "YYYY-MM-DD HH:MM:SS".
    Aceita:
      - ISO: "2026-01-10T16:30:00-03:00" / "2026-01-10T19:30:00Z"
      - "YYYY-MM-DD HH:MM"
      - "YYYY-MM-DD HH:MM:SS"
    """
    if not value:
        return None
    v = value.strip()

    try:
        if "T" in v:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        else:
            # tenta com segundos e sem segundos
            if len(v) >= 19:
                dt = datetime.strptime(v[:19], "%Y-%m-%d %H:%M:%S")
            else:
                dt = datetime.strptime(v[:16], "%Y-%m-%d %H:%M")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def stable_external_id(source: str, competition: str, home: str, away: str, dt: str) -> str:
    """
    Gera external_id estável para permitir UPSERT no Odoo.
    """
    raw = f"{source}|{competition}|{home}|{away}|{dt}"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:14]
    return f"{slug(competition)}/{slug(home)}-vs-{slug(away)}/{dt[:10]}/{h}"


def dedupe_by_external_id(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for it in items:
        eid = it.get("external_id")
        if not eid or eid in seen:
            continue
        seen.add(eid)
        out.append(it)
    return out


def validate_env() -> None:
    if not ODOO_URL:
        die("Variável ODOO_URL não definida.")
    if not ODOO_TOKEN:
        die("Variável ODOO_TOKEN não definida.")
    if not APIFUT_TOKEN:
        die("Variável APIFUT_TOKEN não definida.")


# =========================
# Provider: API Futebol
# =========================
def apifut_get(path: str) -> Any:
    url = f"{APIFUT_BASE.rstrip('/')}/{path.lstrip('/')}"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {APIFUT_TOKEN}"},
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def apifut_collect_mineiro_2026() -> Tuple[str, List[Dict[str, Any]]]:
    """
    Coleta jogos do Campeonato Mineiro 2026.
    Observação: a estrutura JSON pode variar por plano/endpoint.
    O código é tolerante a chaves comuns ("fases", "rodadas", "jogos", etc.).
    """
    source = "bot_apifutebol_mineiro_2026"
    competition = "Campeonato Mineiro"

    # Endpoint "canônico" (pode exigir ajuste dependendo do seu plano)
    campeonato_path = "campeonato/campeonato-mineiro/2026"
    data = apifut_get(campeonato_path)

    matches: List[Dict[str, Any]] = []

    fases = data.get("fases") or []
    for fase in fases:
        rodadas = fase.get("rodadas") or fase.get("rodada") or []
        for rodada in rodadas:
            jogos = rodada.get("jogos") or rodada.get("partidas") or []
            for j in jogos:
                home_raw = (
                    (j.get("time_mandante") or j.get("mandante") or {}).get("nome")
                    or j.get("mandante")
                    or ""
                )
                away_raw = (
                    (j.get("time_visitante") or j.get("visitante") or {}).get("nome")
                    or j.get("visitante")
                    or ""
                )
                if not home_raw or not away_raw:
                    continue

                home_norm = normalize_team(str(home_raw))
                away_norm = normalize_team(str(away_raw))

                # filtra: só jogos que envolvem pelo menos um dos 3 times
                if not home_norm and not away_norm:
                    continue

                dt_iso = (
                    j.get("data_realizacao_iso")
                    or j.get("data_realizacao")
                    or j.get("data")
                    or ""
                )
                match_dt = parse_datetime_to_odoo(str(dt_iso))
                if not match_dt:
                    continue

                round_name = (rodada.get("nome") or rodada.get("descricao") or "").strip()

                stadium = (j.get("estadio") or {}).get("nome") or j.get("estadio") or ""
                city = (j.get("cidade") or {}).get("nome") or j.get("cidade") or ""

                broadcast = j.get("transmissao") or ""
                ticket_url = j.get("url_ingresso") or ""

                home_team = home_norm or str(home_raw).strip()
                away_team = away_norm or str(away_raw).strip()

                matches.append(
                    {
                        "external_id": stable_external_id(
                            "apifutebol",
                            competition,
                            home_team,
                            away_team,
                            match_dt,
                        ),
                        "match_datetime": match_dt,
                        "competition": competition,
                        "round": round_name or "",
                        "home_team": home_team,
                        "away_team": away_team,
                        "stadium": str(stadium).strip(),
                        "city": str(city).strip(),
                        "broadcast": str(broadcast).strip(),
                        "ticket_url": str(ticket_url).strip(),
                    }
                )

    matches = dedupe_by_external_id(matches)
    return source, matches


# =========================
# Push to Odoo
# =========================
def post_to_odoo(source: str, matches: List[Dict[str, Any]]) -> requests.Response:
    payload = {"source": source, "matches": matches}

    return requests.post(
        ODOO_URL,
        json=payload,
        headers={
            "Authorization": f"Bearer {ODOO_TOKEN}",
            "Content-Type": "application/json",
        },
        timeout=HTTP_TIMEOUT,
    )


def push_with_retry(source: str, matches: List[Dict[str, Any]]) -> None:
    if not matches:
        print("[INFO] Nenhum jogo encontrado para enviar.")
        return

    print(f"[INFO] Enviando {len(matches)} jogos para o Odoo…")
    print(f"[INFO] ODOO_URL: {ODOO_URL}")
    print(f"[INFO] SOURCE: {source}")

    last_exc = None
    for attempt in range(1, RETRY_MAX + 2):  # ex: RETRY_MAX=2 => 3 tentativas
        try:
            r = post_to_odoo(source, matches)
            print("[INFO] Status:", r.status_code)

            # sempre imprime body (ajuda debug)
            body = r.text or ""
            if len(body) > 5000:
                body = body[:5000] + "\n... (truncado)"
            print("[INFO] Resposta:", body)

            if r.status_code in (200, 201):
                print("[OK] Sync concluído com sucesso.")
                return

            # tenta retry em casos típicos
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt <= (RETRY_MAX + 1) - 1:
                    print(f"[WARN] Erro {r.status_code}. Tentando novamente em {RETRY_SLEEP_SECONDS}s… (tentativa {attempt})")
                    time.sleep(RETRY_SLEEP_SECONDS)
                    continue

            # outros status => falha imediata
            die(f"Falhou ao enviar para Odoo. HTTP {r.status_code}. Veja logs acima.", 1)

        except Exception as e:
            last_exc = e
            if attempt <= (RETRY_MAX + 1) - 1:
                print(f"[WARN] Exceção: {e}. Tentando novamente em {RETRY_SLEEP_SECONDS}s… (tentativa {attempt})")
                time.sleep(RETRY_SLEEP_SECONDS)
                continue
            die(f"Falha final após tentativas. Última exceção: {e}", 1)

    if last_exc:
        die(f"Falha final: {last_exc}", 1)


# =========================
# Main
# =========================
def main() -> None:
    validate_env()

    # 1) coletar (Mineiro 2026 - API Futebol)
    source, matches = apifut_collect_mineiro_2026()

    # 2) enviar para o Odoo
    push_with_retry(source, matches)


if __name__ == "__main__":
    main()

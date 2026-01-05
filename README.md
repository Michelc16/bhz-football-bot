# BHZ Football Bot (GitHub Actions)

Bot externo para buscar a agenda de jogos do Cruzeiro, Atlético-MG e América-MG diretamente em fontes abertas (GE/Globo Esporte e outros sites públicos) e enviar para o módulo **Agenda de Futebol** do seu Odoo via endpoint REST:

`POST /bhz/football/api/matches`

## ✅ O que ele faz

1. Faz scraping da página oficial do [Campeonato Mineiro no GE](https://ge.globo.com/mg/futebol/campeonato-mineiro/) e coleta novos jogos/rodadas.
2. Normaliza nomes, competições e datas para o formato aceito pelo Odoo (`YYYY-MM-DD HH:MM:SS` em America/Sao_Paulo).
3. Deduplica partidas (hash determinístico `ge_mineiro+dt+mandante+visitante+estádio`).
4. Envia as partidas em lote para o Odoo (ou roda em modo DRY-RUN apenas para inspeção).

---

## Requisitos

- Python 3.10+ (recomendado 3.11)
- Bibliotecas listadas em `requirements.txt` (instaladas via `pip install -r requirements.txt`)
- Endpoint Odoo funcional e token válido

---

## Variáveis de ambiente

O script **não usa `.env`**. Configure as variáveis no GitHub Actions ou localmente.

Obrigatórias:

- `ODOO_URL`  
  Ex: `https://SEU_DB.dev.odoo.com/bhz/football/api/matches`

- `ODOO_TOKEN`  
  Token Bearer do seu módulo Odoo

Opcionais:

- `TEAMS`  
  Lista separada por vírgula (default: `Cruzeiro,Atletico-MG,America-MG`).

- `DAYS_BACK` / `DAYS_FORWARD`  
  Ajuste da janela usada para buscar jogos (default: `7` / `180`).

- `HTTP_TIMEOUT`  
  Timeout em segundos para requests HTTP (default: `45`).

- `FREEAPI_TEAM_IDS`  
  JSON com fallback manual de `{"Time": 123}` usado apenas se algum provider não conseguir identificar o time.

- `DRY_RUN`  
  Defina `DRY_RUN=1` para executar os scrapers e exibir o resumo **sem** enviar ao Odoo.

- `GE_CACHE` / `GE_OFFLINE`  
  `GE_CACHE=1` salva o HTML do GE em `.cache/ge_mineiro.html`.  
  `GE_OFFLINE=1` reutiliza o HTML em cache (útil para debug sem internet).

---

## Rodar localmente (teste rápido)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export ODOO_URL="https://..."
export ODOO_TOKEN="seu_token"
export DRY_RUN=1
python bot_agenda_futebol.py
```

O modo `DRY_RUN` imprime o total por provider/time e encerra sem POST no Odoo.

---

## Provider atual

- `providers/ge_globo_mineiro_provider.py`: faz scraping do site do GE e extrai todas as partidas do Campeonato Mineiro (rodada por rodada).  
  Se a estrutura do site mudar, ajuste apenas este provider. Execute `python providers/ge_globo_mineiro_provider.py` para um debug rápido (imprime os primeiros jogos).

---

## Scraper / Rate limit

O `ScraperClient` em `scraper.py` centraliza:

- `requests.Session` com `User-Agent` realista
- Retry/backoff automático em 429/5xx
- Rate limit simples (intervalo mínimo entre requisições)

Respeite robots/ToS dos sites ao adicionar novos providers.

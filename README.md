# BHZ Football Bot (GitHub Actions)

Bot externo para buscar a agenda de jogos do Cruzeiro, Atlético-MG e América-MG rastreando as páginas de *fixtures* no FlashScore e enviar para o módulo **Agenda de Futebol** do seu Odoo via endpoint REST:

`POST /bhz/football/api/matches`

## ✅ O que ele faz

1. Faz scraping das páginas de fixtures do FlashScore e coleta os próximos jogos oficiais dos três clubes mineiros.
2. Normaliza nomes, competições e datas para o formato aceito pelo Odoo (`YYYY-MM-DD HH:MM:SS` em America/Sao_Paulo).
3. Deduplica partidas (hash determinístico `flashscore|<data>|<mandante>|<visitante>`).
4. Envia as partidas em lote para o Odoo (ou roda em modo DRY-RUN apenas para inspeção).

---

## Requisitos

- Python 3.10+ (recomendado 3.11)
- Bibliotecas listadas em `requirements.txt` (instaladas via `pip install -r requirements.txt`)
- Endpoint Odoo funcional e token válido

---

## Variáveis de ambiente

O script carrega automaticamente um arquivo `.env` presente no diretório raiz do projeto. Ainda assim, você pode configurar as mesmas variáveis diretamente no ambiente (GitHub Actions, terminal, etc.); o resultado é equivalente.

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

- `DRY_RUN`  
  Defina `DRY_RUN=1` para executar o scraper e exibir o resumo **sem** enviar ao Odoo.

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

- `providers/flashscore_provider.py`: usa os *fixtures* estáticos do FlashScore (+ IDs fixos) e tenta manter os três times dentro da janela configurada.  
  O parsing identifica mandante/visitante, data/hora/estádio e envia um payload compatível com o Odoo. Configure os links em `providers/flashscore_provider.py::TEAM_PAGES` se um time mudar de URL.

---

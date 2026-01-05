# BHZ Football Bot (GitHub Actions)

Bot externo para buscar agenda de jogos (Cruzeiro, Atlético-MG e América-MG) e enviar para o módulo **Agenda de Futebol** no seu Odoo.sh via endpoint REST:

`POST /bhz/football/api/matches`

## ✅ O que ele faz

1. Coleta jogos de um provider (inicialmente: **API Futebol**, exemplo Mineiro 2026)
2. Normaliza os times-alvo (Cruzeiro / Atlético-MG / América-MG)
3. Gera um `external_id` estável (pra permitir **upsert**: cria/atualiza)
4. Envia em lote para o Odoo com `Authorization: Bearer <TOKEN>`

---

## Requisitos

- Python 3.10+ (recomendado 3.11)
- Conta/Token do provider (ex: API Futebol)
- Endpoint do seu Odoo funcionando

---

## Variáveis de ambiente

O script **não usa `.env`**. Tudo vem de **ENV VARS** (ideal para GitHub Actions).

Obrigatórias:

- `ODOO_URL`  
  Ex: `https://SEU_DB.dev.odoo.com/bhz/football/api/matches`

- `ODOO_TOKEN`  
  Token Bearer do seu módulo Odoo

- `APIFUT_TOKEN`  
  Token Bearer da API Futebol

Opcionais:

- `APIFUT_BASE` (default: `https://www.api-futebol.com.br`)
- `HTTP_TIMEOUT` (default: `60`)
- `RETRY_MAX` (default: `2`)
- `RETRY_SLEEP_SECONDS` (default: `10`)

---

## Rodar localmente (teste rápido)

### 1) Instale dependências
```bash
pip install -r requirements.txt

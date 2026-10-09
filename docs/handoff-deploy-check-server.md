# Handoff → SERVER — Scansia Manager: deploy-check dopo push su `main`

**Cosa è successo (SVILUPPO, 2026-07-09).** Ho pushato la feature **"Inizializza"** (cutover GUI + reconcile online-flag, primo write Shopify del tool) e portato il branch a `main` per il go-live:

- `origin/main`: `4f1eab4` → **`9270741`** (fast-forward, tutta F1 + Inizializza)
- `origin/feat/scansia-manager`: `d84b04f` → **`9270741`** (allineato — stesso tip di main)

Ale indica che **il deploy da `main` è automatico**. Ma il tuo ultimo assetto registrato era: service `scansia-manager` sul branch **`feat/scansia-manager`**, **autoDeploy=OFF**. Serve riconciliare.

## Check richiesti a SERVER

1. **Branch + autoDeploy del service `scansia-manager`.**
   - Se traccia già `main` con autoDeploy ON → il push ha già armato il deploy, conferma solo che è partito.
   - Se traccia ancora `feat/scansia-manager` con autoDeploy OFF → **ri-punta il service a `main` + abilita autoDeploy** (allineato all'aspettativa di Ale "deploy automatico da main"). In alternativa immediata: **Manual Deploy** — entrambi i branch sono su `9270741`, quindi qualunque dei due deploya lo stesso codice.

2. **Conferma che il deploy di `9270741` è andato a buon fine:** build OK, boot pulito, `/health`=200, Basic Auth attiva (`GET /` → 401). *(cold-start free ~30-60s atteso, ok.)*

3. **Env vars — NESSUNA nuova richiesta dall'Inizializza.** Usa il set M2 già presente: segreti `SHOPIFY_ADMIN_TOKEN`, `GOOGLE_CREDENTIALS_JSON` (o `GOOGLE_APPLICATION_CREDENTIALS`), `PROMO_LOCATION_ID`, `SHOPIFY_STORE`, `APP_PASSWORD`, `TOKEN_SIGNING_SECRET`; config `GSPREAD_SHEET_ID`, `GSPREAD_WORKSHEET_TITLE`, `SHOPIFY_API_VERSION=2025-07`. Conferma solo che il set è invariato e completo (senza `GSPREAD_WORKSHEET_TITLE` l'app fa `ConfigError` al boot).

## FYI (nessuna azione infra)

- Al **primo apply** dell'Inizializza l'app crea/scrive una **nuova tab GSheet `AUDIT_INIT`** (snapshot durevole before, abort-on-failure) sul foglio Scansia — comparirà una tab nuova, è previsto.
- **Nota autoDeploy**: se preferisci restare in deploy-gate (autoDeploy OFF anche su main), va bene — basta che tu faccia il Manual Deploy di `9270741` e me lo confermi; l'importante è che il codice live sia `9270741`.

**Ref:** `Racoon-GIT/Sync-Scansia@9270741` (branch `main` e `feat/scansia-manager`) · Render service `scansia-manager` (free) · https://scansia-manager.onrender.com

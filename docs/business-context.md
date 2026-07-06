# Contesto di business — Scansia resi e outlet Shopify

Racoon-LAB non è un rivenditore di calzature: è un fornitore di servizi di personalizzazione artigianale su calzature di terzi (Nike, Converse, Diadora, Birkenstock, Dr. Martens, Vans, Adidas, UGG). Questo documento spiega il razionale di business dietro la "scansia resi" e il loop di gestione degli outlet su Shopify, in modo che le scelte tecniche del tool (publish/delete/reorder/prezzi) risultino leggibili a chi riprende il progetto.

## Cos'è la scansia resi

La "scansia resi" è lo scaffale fisico dove finiscono i rientri: cambi taglia o resi da parte dei clienti. Quando un pezzo rientra, il risultato tipico è una **taglia singola** di un modello — non l'intera ladder di taglie del prodotto originale. Quel singolo pezzo viene rivenduto in saldo come prodotto Shopify di tipo "OUTLET".

## Il modello OUTLET (full-size)

Il prodotto outlet è un **duplicato** del prodotto sorgente full-price: eredita la ladder completa delle taglie (stessa struttura varianti del sorgente), ma lo **stock vive solo sulla taglia (o taglie) effettivamente rientrata/e**, sulla sell-location "Promo". Le altre taglie della ladder restano a stock zero: esistono come varianti ma non sono vendibili.

Il titolo dell'outlet segue la convenzione `"<titolo sorgente> - Outlet"`, dove il titolo base è quello del prodotto sorgente. Questa convenzione di naming è anche il meccanismo con cui le collezioni smart (vedi sotto) riconoscono un prodotto come outlet.

## Le tre collezioni

Tutte e tre sono collezioni **smart** (rule-driven), non manuali:

| Collezione | ID | Regola | Membri (ultima ricognizione) |
|---|---|---|---|
| SALDI | `95310381121` | `compareAt>0 AND inventory>0 AND type≠…` | 697 (670 ACTIVE / 20 DRAFT / 7 ARCHIVED) |
| OUTLET | `650952442188` | `TITLE contains "Outlet"` | 182 (131 ACTIVE / 51 DRAFT / 0 ARCHIVED) |
| BLACK FRIDAY 2025 | `262965428289` | `TITLE contains "Outlet"` (stessa regola di OUTLET) | 182 (di fatto duplicato di OUTLET) |

SALDI è la collezione vetrina dei saldi. OUTLET è l'enumerazione **autorevole** dei prodotti outlet: la membership di questa collezione è la fonte di verità, non la ricerca full-text. La ricerca `title:*Outlet*` (con wildcard) è un cross-check valido; `title:Outlet` senza wildcard restituisce 0 risultati e va evitata.

## Il loop publish → SALDI → reorder

Il punto chiave del sistema: un outlet pubblicato (cioè con `compareAt>0` e `stock>0`) **entra automaticamente in SALDI** tramite la smart-rule di quella collezione. Non c'è nessuna azione manuale di aggiunta a SALDI.

Da qui discende la divisione dei compiti del tool:
- il workflow **SYNC** (publish/delete) targetizza la collezione **OUTLET** — crea o rimuove il prodotto outlet, che a cascata entra/esce da SALDI in base a compareAt/stock;
- il workflow **REORDER** targetizza la collezione **SALDI** — riordina la vetrina che le due azioni precedenti alimentano.

Publish e reorder non sono quindi due funzioni scollegate: sono due stadi dello stesso loop che alimenta la stessa collezione vetrina finale.

## BLACK FRIDAY 2025: conseguenza storefront (Q-bf-storefront)

BLACK FRIDAY 2025 condivide **esattamente la stessa regola** di OUTLET (`TITLE contains "Outlet"`). Questo non è solo una ridondanza interna di collezioni: BLACK FRIDAY 2025 è una collezione **customer-facing** sullo storefront, quindi ogni nuovo outlet pubblicato **entra automaticamente anche in BLACK FRIDAY 2025**, una collezione fuori stagione ma visibile ai clienti.

La decisione owner attuale è **"skip"**: nessuna azione di cleanup ora. Questa decisione è stata presa consapevolmente contro il rischio di visibilità reale (non solo contro la duplicazione tecnica delle collezioni).

**Domanda owner aperta (Q-bf-storefront):** valutare una terza opzione oltre a "skip" e "cleanup pieno" — fare *unpublish* di BLACK FRIDAY 2025 dalla navigazione storefront (nasconderla ai clienti) lasciando intatta la collezione e la sua regola. In questo modo "skip cleanup" non equivarrebbe implicitamente a "tenerla visibile ai clienti": sono due decisioni distinte che oggi coincidono per inerzia.

## Stock, sell-location e oversell

La sell-location degli outlet è **Promo** (id `61184966721`). Delle location online totali (4: Promo, Magazzino, Prodotti Eliminati, Abbigliamento):
- 133 outlet hanno stock su Promo;
- 0 outlet sono solo-Magazzino.

**38 outlet risultano in policy di inventory CONTINUE (oversell)**, cioè vendibili anche a stock 0. Questo è un problema di business, non solo tecnico: un pezzo unico rientrato non deve poter essere overvenduto. Vanno normalizzati a policy **DENY**.

## Segnale di rientro

Uno scenario Make appende una riga al foglio Google `Scarpe_in_Scansia` a ogni rientro fisico (SKU, Size, qty). Quell'append è il segnale di rientro fresco e va trattato come tale: il tool lo consuma come **delta idempotente one-shot** — `row_uuid` + flag `reconciled` — sommando la qty a Promo esattamente una volta per riga. Il tool non deve mai dedurre lo stock corretto dalla colonna Qta statica del foglio, perché quella colonna non rappresenta lo stato reale accumulato.

Righe duplicate sulla stessa coppia (SKU, Size) sono rientri ripetuti legittimi (più cambi taglia sullo stesso modello) e vanno sommate, non deduplicate.

## Downstream (feed) — nessuna rottura

Gli outlet pubblicati confluiscono nel feed Google Shopping/Meta tramite la catena esistente già in produzione: `shopify-mysql-sync` (03:00) → `Feed-Exporter` (05:05). Un publish on-demand fatto da questo tool entra nel feed al ciclo giornaliero successivo: la latenza è di circa 24h, non una rottura del sistema.

Nessun sistema esterno legge il campo `Product_Id` scritto dal tool sul foglio `Scarpe_in_Scansia`: è uno stato interno del tool stesso.

---

**Nota sui numeri.** Tutti i conteggi e gli ID di questo documento provengono da 2 ricognizioni read-only sullo store (token custom-app), effettuate in un momento puntuale. Vanno riverificati live prima di qualsiasi decisione operativa: i conteggi di membership delle collezioni e lo stato delle policy di inventory cambiano nel tempo con l'attività ordinaria dello store.

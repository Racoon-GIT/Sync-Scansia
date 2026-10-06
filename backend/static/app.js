/**
 * Scansia Manager — MVP dashboard, vanilla JS (no framework, no CDN, no build).
 *
 * Every request is same-origin and gated by the server's HTTP Basic Auth
 * (backend.auth.basic_auth.require_basic_auth) — the BROWSER handles the auth
 * prompt (native 401 + WWW-Authenticate) and caches the credential for the
 * origin, so `fetch()` below carries no token/secret of any kind: there is
 * nothing to leak from this file.
 *
 * Load-bearing pattern used by publish / prices and the delete tab (cleanup,
 * single delete, multi-outlet exit, deny-normalize): a *preview* job
 * returns `{plan, plan_hash, confirm_token}`. The client stores plan_hash +
 * confirm_token verbatim and echoes them back — unmodified — in the body of
 * the matching *apply* call. The server re-verifies both against the live
 * state (TOCTOU) before mutating anything; a drifted plan_hash comes back as
 * job status `VERIFY_FAILED`, surfaced here as "lo stato è cambiato dal
 * preview: rifai il preview" ("piano cambiato: ricalcola l'anteprima" in the
 * delete tab).
 */
(function () {
  "use strict";

  // ===========================================================================
  // Generic helpers
  // ===========================================================================
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  async function api(method, path, body) {
    const opts = { method, headers: {} };
    if (body !== undefined) {
      opts.headers["Content-Type"] = "application/json";
      opts.body = JSON.stringify(body);
    }
    const res = await fetch(path, opts);
    let data = null;
    try {
      data = await res.json();
    } catch (e) {
      data = null;
    }
    if (!res.ok) {
      const err = (data && data.error) || { code: "unknown_error", message: "HTTP " + res.status };
      const e = new Error(err.message);
      e.code = err.code;
      e.reason = err.reason;
      e.status = res.status;
      throw e;
    }
    return data;
  }

  function sleep(ms) {
    return new Promise(function (r) { setTimeout(r, ms); });
  }

  /** Poll a job endpoint until it reaches a terminal state (done|failed). */
  async function pollJob(path, opts) {
    const intervalMs = (opts && opts.intervalMs) || 1500;
    const timeoutMs = (opts && opts.timeoutMs) || 120000;
    const start = Date.now();
    for (;;) {
      const rec = await api("GET", path);
      if (rec.status === "done" || rec.status === "failed") return rec;
      if (Date.now() - start > timeoutMs) {
        const e = new Error("Timeout in attesa del job.");
        e.code = "client_timeout";
        throw e;
      }
      await sleep(intervalMs);
    }
  }

  function toast(message, isError) {
    const el = document.getElementById("toast");
    el.textContent = message;
    el.classList.toggle("error", !!isError);
    el.classList.remove("hidden");
    clearTimeout(toast._t);
    toast._t = setTimeout(function () { el.classList.add("hidden"); }, 6000);
  }

  function setStatus(el, text, kind) {
    el.textContent = text || "";
    el.classList.remove("error", "ok");
    if (kind) el.classList.add(kind);
  }

  /** Standard "job failed" / "VERIFY_FAILED" / "gesture_required" -> human status text. */
  function jobFailureMessage(rec) {
    if (rec.status === "failed") {
      return "Operazione non riuscita (" + rec.error_code + ").";
    }
    if (rec.result && rec.result.status === "VERIFY_FAILED") {
      return "Lo stato è cambiato dal preview: rifai il preview.";
    }
    if (rec.result && rec.result.status === "gesture_required") {
      // The job itself succeeded (status "done") but the human-gesture gate
      // was not satisfied (missing/wrong CONFERMO, or missing second_confirm
      // above the demote-count threshold) — applied is false, no mutation ran.
      return "Digita CONFERMO e, se richiesto, spunta la conferma.";
    }
    return null;
  }

  // ===========================================================================
  // INIT banner ("Inizializza" on MySQL, plan §3.4) — preview -> the kept plan's
  // hash + token -> confirm with CONFERMO and the /dlqs precondition (the GET's
  // instant and dlqCount, typed). Hidden by default; shown only while
  // GET /init/status reports baseline_done === false.
  // ===========================================================================
  const initState = { pending: null };
  // The baseline reads every outlet (minutes, not seconds): poll patiently.
  const INIT_POLL = { timeoutMs: 30 * 60 * 1000, intervalMs: 3000 };

  function renderRows(tbodyId, rows, cells) {
    const tbody = document.getElementById(tbodyId);
    tbody.innerHTML = "";
    (rows || []).forEach(function (r) {
      const tr = document.createElement("tr");
      tr.innerHTML = cells(r).map(function (c) { return "<td>" + esc(c) + "</td>"; }).join("");
      tbody.appendChild(tr);
    });
  }

  function keysOf(plan, action) {
    return (plan.keys || []).filter(function (k) { return k.action === action; });
  }

  function renderInitPlan(plan) {
    document.getElementById("init-plan-panel").classList.remove("hidden");
    const c = plan.counts || {};
    document.getElementById("init-counts").textContent = plan.already_done
      ? "Inizializzazione già eseguita."
      : "Chiavi " + (c.keys || 0) + ": allineate " + (c.level || 0) + ", bozze azzerate " + (c.zero || 0) +
        ", senza outlet " + (c.none || 0) + ", solo timbro " + (c.stamp || 0) + ", unità a 0 " + (c.lower || 0) +
        ", trattenute " + (c.hold || 0) + ". Unità Promo +" + (c.units_up || 0) + " / -" + (c.units_down || 0) +
        ". Eventi da timbrare " + (c.events_to_stamp || 0) + ", trattenuti " + (c.events_held || 0) + ".";
    const notes = (plan.anomalies || []).concat(
      (plan.keyless_event_ids || []).map(function (i) { return "consume_miss:" + i; }));
    document.getElementById("init-anomalies").textContent = notes.length ? "Anomalie: " + notes.join(", ") : "";
    renderRows("init-level-tbody", keysOf(plan, "level"), function (k) {
      return [k.sku, k.size, k.n, k.delta, k.activate ? "sì" : ""]; });
    renderRows("init-zero-tbody", keysOf(plan, "zero"), function (k) { return [k.sku, k.size, k.n, k.delta]; });
    renderRows("init-none-tbody", keysOf(plan, "none"), function (k) { return [k.sku, k.size, k.n]; });
    renderRows("init-lower-tbody", keysOf(plan, "lower"), function (k) { return [k.sku, k.size, k.delta]; });
    renderRows("init-hold-tbody", keysOf(plan, "hold"), function (k) {
      return [k.sku, k.size, k.n, k.reason, (k.twins || []).join(" | "), k.delta]; });
    renderRows("init-products-tbody", plan.products, function (p) {
      return [p.product_id, p.title, p.status, (p.unsafe_variant_ids || []).length, (p.custom_lowerings || []).length]; });
    renderRows("init-prices-tbody", plan.prices, function (x) {
      return [x.sku, x.status, x.price, (x.values || []).join(" | ")]; });
    renderRows("init-partb-tbody", (plan.part_b || []).filter(function (r) { return r.bucket !== "kept-online"; }),
      function (r) { return [r.pair_id, r.sku, r.size, r.bucket]; });
    renderRows("init-catalogue-tbody", plan.catalogue, function (n) {
      return [n.sku, n.size, n.note, (n.twins || []).join(" | ")]; });
    document.getElementById("init-confirm-word").value = "";
    document.getElementById("init-dlq-at").value = "";
    document.getElementById("init-dlq-count").value = "";
    document.getElementById("btn-init-apply").disabled = !!plan.already_done;
    document.getElementById("init-outcome-panel").classList.add("hidden");
  }

  async function initPreview() {
    const statusEl = document.getElementById("init-status");
    const btn = document.getElementById("btn-init-preview");
    btn.disabled = true;
    setStatus(statusEl, "calcolo piano (alcuni minuti)...");
    try {
      const sub = await api("POST", "/init/preview", undefined);
      const rec = await pollJob("/init/preview/" + sub.job_id, INIT_POLL);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      initState.pending = { plan_hash: rec.result.plan_hash, confirm_token: rec.result.confirm_token };
      renderInitPlan(rec.result.plan);
      setStatus(statusEl, "piano pronto — verifica e conferma", "ok");
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  function renderInitOutcome(result) {
    const report = result.report;
    document.getElementById("init-outcome-panel").classList.remove("hidden");
    document.getElementById("init-outcome-summary").textContent =
      "Stato " + report.status + (report.baseline_row_id ? " — riga BASELINE #" + report.baseline_row_id : "") +
      (report.reason ? " (" + report.reason + ")" : "") + ". Eventi timbrati " + report.stamped +
      ", online +" + report.online_on + " / -" + report.online_off + ", prezzi seminati " +
      (report.prices_seeded || []).length + ", chiavi trattenute " + (report.held || []).length + ".";
    renderRows("init-outcome-tbody", report.outcomes, function (o) {
      return [o.sku, o.size, o.action, o.result, o.reason]; });
  }

  async function initApply() {
    if (!initState.pending) return;
    const statusEl = document.getElementById("init-apply-status");
    const btn = document.getElementById("btn-init-apply");
    const dlqCount = document.getElementById("init-dlq-count").value;
    const body = Object.assign({}, initState.pending, {
      confirm: document.getElementById("init-confirm-word").value.trim(),
      // datetime-local carries no offset: the server reads it as Europe/Rome
      dlq_checked_at: document.getElementById("init-dlq-at").value || null,
      dlq_count: dlqCount === "" ? null : Number(dlqCount),
    });
    btn.disabled = true;
    setStatus(statusEl, "applico (alcuni minuti)...");
    try {
      const sub = await api("POST", "/init/apply", body);
      const rec = await pollJob("/init/apply/" + sub.job_id, INIT_POLL);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      if (!rec.result || !rec.result.report) {
        setStatus(statusEl, "risposta inattesa dal server.", "error");
        return;
      }
      renderInitOutcome(rec.result);
      if (rec.result.applied) {
        setStatus(statusEl, "inizializzazione applicata", "ok");
        initState.pending = null;
        await checkInitStatus(); // hides the banner once baseline_done becomes true
      } else {
        setStatus(statusEl, "non applicata: " + rec.result.status, "error");
      }
    } catch (e) {
      // A synchronous 409, before any job: confirm_invalid, preview_expired, or
      // gesture_required (CONFERMO or the /dlqs precondition, with its reason).
      setStatus(statusEl, "errore: " + e.message + (e.reason ? " (" + e.reason + ")" : ""), "error");
    } finally {
      btn.disabled = false;
    }
  }

  async function checkInitStatus() {
    try {
      const data = await api("GET", "/init/status");
      document.getElementById("init-banner").classList.toggle("hidden", !!data.baseline_done);
    } catch (e) {
      // banner stays as-is on a transient error; not fatal to the rest of the app.
    }
  }

  // ===========================================================================
  // RE-LEVEL (per key, plan §3.4) — the held keys listed (or a key typed), the
  // plan previewed, then the same confirm as the baseline (CONFERMO + /dlqs).
  // ===========================================================================
  const relevelState = { pending: null };

  function addRelevelRow(key, h) {
    const tbody = document.getElementById("relevel-held-tbody");
    const tr = document.createElement("tr");
    const box = document.createElement("input");
    box.type = "checkbox";
    box.className = "relevel-key";
    box.dataset.sku = key[0];
    box.dataset.size = key[1];
    box.checked = !h;
    const td = document.createElement("td");
    td.appendChild(box);
    tr.appendChild(td);
    [key[0], key[1], h ? h.kinds.join(", ") : "", h ? h.reasons.join(", ") : "", h ? h.events : ""].forEach(function (c) {
      const cell = document.createElement("td");
      cell.textContent = String(c);
      tr.appendChild(cell);
    });
    tbody.appendChild(tr);
  }

  async function relevelHeld() {
    const statusEl = document.getElementById("relevel-status");
    try {
      const data = await api("GET", "/relevel/held");
      document.getElementById("relevel-held-tbody").innerHTML = "";
      (data.held || []).forEach(function (h) { addRelevelRow(h.key, h); });
      setStatus(statusEl, (data.held || []).length + " chiavi trattenute", "ok");
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    }
  }

  function relevelAdd() {
    const sku = document.getElementById("relevel-sku").value.trim();
    const size = document.getElementById("relevel-size").value.trim();
    if (sku && size) addRelevelRow([sku, size], null);
  }

  async function relevelPreview() {
    const statusEl = document.getElementById("relevel-status");
    const keys = Array.prototype.slice.call(document.querySelectorAll(".relevel-key:checked"))
      .map(function (b) { return [b.dataset.sku, b.dataset.size]; });
    if (!keys.length) {
      setStatus(statusEl, "seleziona almeno una chiave", "error");
      return;
    }
    setStatus(statusEl, "calcolo piano...");
    try {
      const sub = await api("POST", "/relevel/preview", { keys: keys });
      const rec = await pollJob("/relevel/preview/" + sub.job_id, INIT_POLL);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      relevelState.pending = { plan_hash: rec.result.plan_hash, confirm_token: rec.result.confirm_token };
      document.getElementById("relevel-plan-panel").classList.remove("hidden");
      document.getElementById("relevel-outcome-panel").classList.add("hidden");
      renderRows("relevel-plan-tbody", rec.result.plan.keys, function (k) {
        return [k.sku, k.size, k.n, k.branch, k.reason, k.delta, (k.held || []).join(", ")]; });
      setStatus(statusEl, "piano pronto — verifica e conferma", "ok");
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    }
  }

  async function relevelApply() {
    if (!relevelState.pending) return;
    const statusEl = document.getElementById("relevel-apply-status");
    const btn = document.getElementById("btn-relevel-apply");
    const dlqCount = document.getElementById("relevel-dlq-count").value;
    const body = Object.assign({}, relevelState.pending, {
      confirm: document.getElementById("relevel-confirm-word").value.trim(),
      dlq_checked_at: document.getElementById("relevel-dlq-at").value || null,
      dlq_count: dlqCount === "" ? null : Number(dlqCount),
    });
    btn.disabled = true;
    setStatus(statusEl, "applico...");
    try {
      const sub = await api("POST", "/relevel/apply", body);
      const rec = await pollJob("/relevel/apply/" + sub.job_id, INIT_POLL);
      if (rec.status === "failed" || !rec.result || !rec.result.report) {
        setStatus(statusEl, rec.status === "failed" ? jobFailureMessage(rec) : "risposta inattesa dal server.", "error");
        return;
      }
      document.getElementById("relevel-outcome-panel").classList.remove("hidden");
      renderRows("relevel-outcome-tbody", rec.result.report.outcomes, function (o) {
        return [o.sku, o.size, o.branch, o.result, o.reason]; });
      setStatus(statusEl, rec.result.applied ? "riallineamento eseguito" : "non applicato: " + rec.result.status,
        rec.result.applied ? "ok" : "error");
      if (rec.result.applied) relevelState.pending = null;
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message + (e.reason ? " (" + e.reason + ")" : ""), "error");
    } finally {
      btn.disabled = false;
    }
  }

  function initRelevel() {
    document.getElementById("btn-relevel-held").addEventListener("click", relevelHeld);
    document.getElementById("btn-relevel-add").addEventListener("click", relevelAdd);
    document.getElementById("btn-relevel-preview").addEventListener("click", relevelPreview);
    document.getElementById("btn-relevel-apply").addEventListener("click", relevelApply);
  }

  function initInitBanner() {
    document.getElementById("btn-init-preview").addEventListener("click", initPreview);
    document.getElementById("btn-init-apply").addEventListener("click", initApply);
    checkInitStatus();
  }

  // ===========================================================================
  // Tabs
  // ===========================================================================
  function initTabs() {
    const buttons = document.querySelectorAll(".tab-btn");
    buttons.forEach(function (btn) {
      btn.addEventListener("click", function () {
        buttons.forEach(function (b) { b.classList.remove("active"); });
        document.querySelectorAll(".tab-panel").forEach(function (p) { p.classList.remove("active"); });
        btn.classList.add("active");
        document.getElementById("tab-" + btn.dataset.tab).classList.add("active");
      });
    });
  }

  // ===========================================================================
  // SCANSIA tab
  // ===========================================================================
  const scansiaState = {
    pairs: [],          // the register's shelf: one row per pair (IN_SCANSIA)
    inventoryBySku: {}, // sku -> join result (chips + freshness)
  };

  // The live join's verdict on this pair's size: exact match (the SPs' rule), a twin
  // spelling to respell at the source, or no variant.
  function sizeCell(join, size) {
    if (!join || join.failed) return "";
    const s = (join.sizes || []).find(function (x) { return x.size === size; });
    if (!s) return "";
    if (s.matched) return "esatta (Promo " + esc(s.promo_available) + ")";
    if ((s.twins || []).length) return "da riscrivere: " + esc(s.twins.join(" | "));
    return "assente";
  }

  function renderScansiaTable() {
    const tbody = document.getElementById("scansia-tbody");
    tbody.innerHTML = "";
    scansiaState.pairs.forEach(function (p) {
      const tr = document.createElement("tr");
      const join = scansiaState.inventoryBySku[p.sku];
      tr.innerHTML =
        "<td>" + esc(p.pair_id) + "</td>" +
        "<td>" + esc(p.sku) + "</td>" +
        "<td>" + esc(p.size) + "</td>" +
        "<td>" + (p.online ? "sì" : "no") + "</td>" +
        "<td>" + esc(p.compare_at) + "</td>" +
        "<td>" + esc(p.price_outlet) + "</td>" +
        "<td>" + esc(p.discount) + "</td>" +
        "<td>" + esc(p.outlet_product_id) + "</td>" +
        "<td>" + sizeCell(join, p.size) + "</td>" +
        "<td>" + renderJoinCell(join) + "</td>";
      tbody.appendChild(tr);
    });
  }

  function renderJoinCell(join) {
    if (!join) return "<span class=\"freshness\">non aggiornato</span>";
    let html = (join.chips || []).map(function (c) {
      return "<span class=\"chip " + esc(c) + "\">" + esc(c) + "</span>";
    }).join("");
    if (join.failed) {
      html += "<span class=\"freshness failed\">join fallito — dato NON autoritativo</span>";
    } else if (join.stale) {
      html += "<span class=\"freshness stale\">parziale/stale — " + esc(join.fetched_at) + "</span>";
    } else {
      html += "<span class=\"freshness\">aggiornato: " + esc(join.fetched_at) + "</span>";
    }
    return html;
  }

  async function loadScansia() {
    try {
      const data = await api("GET", "/scansia");
      scansiaState.pairs = data.pairs || [];
      renderScansiaTable();
    } catch (e) {
      toast("Errore caricamento scaffale: " + e.message, true);
    }
  }

  async function runInventoryJoin() {
    const statusEl = document.getElementById("inventory-status");
    const btn = document.getElementById("btn-inventory-run");
    btn.disabled = true;
    setStatus(statusEl, "avvio join...");
    try {
      const sub = await api("POST", "/scansia/inventory", undefined);
      setStatus(statusEl, "in corso (job " + sub.job_id + ")...");
      const rec = await pollJob("/scansia/inventory/" + sub.job_id);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      const results = (rec.result && rec.result.results) || [];
      scansiaState.inventoryBySku = {};
      results.forEach(function (r) { scansiaState.inventoryBySku[r.sku] = r; });
      renderScansiaTable();
      const failedCount = (rec.result && rec.result.failed_count) || 0;
      setStatus(statusEl,
        "completato: " + results.length + " gruppi, " + failedCount + " non autoritativi",
        failedCount > 0 ? "error" : "ok");
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  function initScansiaTab() {
    document.getElementById("btn-scansia-refresh").addEventListener("click", loadScansia);
    document.getElementById("btn-inventory-run").addEventListener("click", runInventoryJoin);
    loadScansia();
  }

  // ===========================================================================
  // PUBLISH tab
  // ===========================================================================
  const publishState = { pending: null };

  function renderPublishPlan(plan) {
    const panel = document.getElementById("publish-plan-panel");
    panel.classList.remove("hidden");
    document.getElementById("publish-anomalies").textContent =
      (plan.anomalies && plan.anomalies.length) ? "Anomalie: " + plan.anomalies.join(", ") : "";
    const tbody = document.getElementById("publish-plan-tbody");
    tbody.innerHTML = "";
    (plan.actions || []).forEach(function (a) {
      const tr = document.createElement("tr");
      tr.innerHTML =
        "<td>" + esc(a.sku) + "</td>" +
        "<td>" + esc(a.branch) + "</td>" +
        "<td>" + (a.publishable ? "sì" : "no") + "</td>" +
        "<td>" + esc(a.reason) + "</td>" +
        "<td>" + esc(a.price) + "</td>" +
        "<td>" + esc(a.compare_at) + "</td>" +
        "<td>" + esc((a.warnings || []).join(", ")) + "</td>";
      tbody.appendChild(tr);
    });
    document.getElementById("publish-outcome-panel").classList.add("hidden");
  }

  async function publishPreview() {
    const statusEl = document.getElementById("publish-status");
    const btn = document.getElementById("btn-publish-preview");
    btn.disabled = true;
    setStatus(statusEl, "calcolo piano...");
    try {
      const sub = await api("POST", "/outlet/publish/preview", undefined);
      const rec = await pollJob("/outlet/publish/preview/" + sub.job_id);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      publishState.pending = { plan_hash: rec.result.plan_hash, confirm_token: rec.result.confirm_token };
      renderPublishPlan(rec.result.plan);
      setStatus(statusEl, "piano pronto — verifica e conferma", "ok");
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  function renderPublishOutcome(report) {
    const panel = document.getElementById("publish-outcome-panel");
    panel.classList.remove("hidden");
    const tbody = document.getElementById("publish-outcome-tbody");
    tbody.innerHTML = "";
    (report.outcomes || []).forEach(function (o) {
      const tr = document.createElement("tr");
      tr.innerHTML =
        "<td>" + esc(o.sku) + "</td>" + "<td>" + esc(o.branch) + "</td>" +
        "<td>" + esc(o.status) + "</td>" + "<td>" + esc(o.target_gid) + "</td>" +
        "<td>" + esc((o.warnings || []).join(", ")) + "</td>";
      tbody.appendChild(tr);
    });
  }

  async function publishConfirm() {
    if (!publishState.pending) return;
    const statusEl = document.getElementById("publish-apply-status");
    const btn = document.getElementById("btn-publish-confirm");
    btn.disabled = true;
    setStatus(statusEl, "applico...");
    try {
      const sub = await api("POST", "/outlet/publish/apply", publishState.pending);
      const rec = await pollJob("/outlet/publish/apply/" + sub.job_id);
      const failMsg = jobFailureMessage(rec);
      if (failMsg) {
        setStatus(statusEl, failMsg, "error");
        return;
      }
      renderPublishOutcome(rec.result.report);
      setStatus(statusEl, "applicato con successo", "ok");
      publishState.pending = null;
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  function initPublishTab() {
    document.getElementById("btn-publish-preview").addEventListener("click", publishPreview);
    document.getElementById("btn-publish-confirm").addEventListener("click", publishConfirm);
  }

  // ===========================================================================
  // PREZZI tab
  // ===========================================================================
  const pricesState = { pending: null };

  function currentPricesMode() {
    return document.getElementById("prices-mode").value;
  }

  function parseKeyValueLines(text, valueParser) {
    const out = {};
    (text || "").split("\n").forEach(function (line) {
      const t = line.trim();
      if (!t) return;
      const idx = t.indexOf("=");
      if (idx < 0) return;
      const k = t.slice(0, idx).trim();
      const v = t.slice(idx + 1).trim();
      if (!k) return;
      out[k] = valueParser(v);
    });
    return out;
  }

  function buildPriceParams() {
    const mode = currentPricesMode();
    const params = { percent_by_sku: {}, price_by_sku: {}, rules: [], override_percent_by_sku: {} };
    if (mode === "percent") {
      params.percent_by_sku = parseKeyValueLines(document.getElementById("percent-lines").value, parseFloat);
    } else if (mode === "direct") {
      params.price_by_sku = parseKeyValueLines(document.getElementById("direct-lines").value, function (v) { return v; });
    } else if (mode === "bulk") {
      const rulesRaw = document.getElementById("bulk-rules-json").value.trim();
      params.rules = rulesRaw ? JSON.parse(rulesRaw) : [];
      const overrideRaw = document.getElementById("bulk-override-json").value.trim();
      params.override_percent_by_sku = overrideRaw ? JSON.parse(overrideRaw) : {};
    }
    return params;
  }

  function updatePricesModeVisibility() {
    const mode = currentPricesMode();
    document.querySelector(".mode-percent").classList.toggle("hidden", mode !== "percent");
    document.querySelector(".mode-direct").classList.toggle("hidden", mode !== "direct");
    document.querySelector(".mode-bulk").classList.toggle("hidden", mode !== "bulk");
  }

  function renderPricePlan(plan) {
    const panel = document.getElementById("prices-plan-panel");
    panel.classList.remove("hidden");
    document.getElementById("prices-anomalies").textContent =
      (plan.anomalies && plan.anomalies.length) ? "Anomalie: " + plan.anomalies.join(", ") : "";
    const tbody = document.getElementById("prices-plan-tbody");
    tbody.innerHTML = "";
    (plan.diffs || []).forEach(function (d) {
      const tr = document.createElement("tr");
      tr.innerHTML =
        "<td>" + esc(d.sku) + "</td>" + "<td>" + esc(d.status) + "</td>" +
        "<td>" + (d.actionable ? "sì" : "no") + "</td>" +
        "<td>" + esc(d.price) + "</td>" + "<td>" + esc(d.compare_at) + "</td>" + "<td>" + esc(d.discount) + "</td>" +
        "<td>" + esc(d.register_price) + "</td>" + "<td>" + esc(d.live_price) + "</td>" +
        "<td>" + esc((d.warnings || []).join(", ")) + "</td>";
      tbody.appendChild(tr);
    });
    document.getElementById("prices-outcome-panel").classList.add("hidden");
  }

  async function pricesPreview(ev) {
    ev.preventDefault();
    const statusEl = document.getElementById("prices-status");
    const btn = document.getElementById("btn-prices-preview");
    btn.disabled = true;
    setStatus(statusEl, "calcolo piano...");
    try {
      const mode = currentPricesMode();
      const params = buildPriceParams();
      const status_override = document.getElementById("prices-status-override").checked;
      const sub = await api("POST", "/prices/preview", { mode: mode, params: params, status_override: status_override });
      const rec = await pollJob("/prices/preview/" + sub.job_id);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      pricesState.pending = {
        plan_hash: rec.result.plan_hash,
        confirm_token: rec.result.confirm_token,
        mode: mode, params: params, status_override: status_override,
      };
      renderPricePlan(rec.result.plan);
      setStatus(statusEl, "piano pronto — verifica e conferma", "ok");
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  function renderPricesOutcome(intentId, outcomes) {
    const panel = document.getElementById("prices-outcome-panel");
    panel.classList.remove("hidden");
    document.getElementById("prices-intent-id").textContent = "intent_id: " + intentId;
    const tbody = document.getElementById("prices-outcome-tbody");
    tbody.innerHTML = "";
    (outcomes || []).forEach(function (o) {
      const tr = document.createElement("tr");
      tr.innerHTML =
        "<td>" + esc(o.sku) + "</td>" + "<td>" + esc(o.product_gid) + "</td>" +
        "<td>" + esc(o.status) + "</td>" + "<td>" + esc((o.warnings || []).join(", ")) + "</td>";
      tbody.appendChild(tr);
    });
  }

  async function pricesConfirm() {
    if (!pricesState.pending) return;
    const statusEl = document.getElementById("prices-apply-status");
    const btn = document.getElementById("btn-prices-confirm");
    btn.disabled = true;
    setStatus(statusEl, "applico...");
    try {
      const sub = await api("POST", "/prices/apply", pricesState.pending);
      const rec = await pollJob("/prices/apply/" + sub.job_id);
      const failMsg = jobFailureMessage(rec);
      if (failMsg) {
        setStatus(statusEl, failMsg, "error");
        return;
      }
      renderPricesOutcome(rec.result.report.intent_id, rec.result.report.outcomes);
      setStatus(statusEl, "applicato con successo", "ok");
      pricesState.pending = null;
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  async function pricesRevert() {
    const statusEl = document.getElementById("revert-status");
    const btn = document.getElementById("btn-prices-revert");
    const intentId = document.getElementById("revert-intent-id").value.trim();
    const confirmWord = document.getElementById("revert-confirm-word").value.trim();
    if (!intentId) {
      setStatus(statusEl, "intent_id obbligatorio", "error");
      return;
    }
    btn.disabled = true;
    setStatus(statusEl, "revert in corso...");
    try {
      const sub = await api("POST", "/prices/revert", { intent_id: intentId, confirm: confirmWord });
      const rec = await pollJob("/prices/revert/" + sub.job_id);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      const report = rec.result;
      document.getElementById("revert-outcome-table").classList.remove("hidden");
      const tbody = document.getElementById("revert-outcome-tbody");
      tbody.innerHTML = "";
      (report.outcomes || []).forEach(function (o) {
        const tr = document.createElement("tr");
        tr.innerHTML =
          "<td>" + esc(o.sku) + "</td>" + "<td>" + esc(o.product_gid) + "</td>" +
          "<td>" + esc(o.status) + "</td>" + "<td>" + esc((o.warnings || []).join(", ")) + "</td>";
        tbody.appendChild(tr);
      });
      setStatus(statusEl,
        "revert applicato: " + report.reverted_pairs + " paia nel registro, " + report.reverted_products +
        " prodotti / " + report.reverted_variants + " varianti su Shopify",
        "ok");
    } catch (e) {
      // gesture_required / confirm errors surface here too (synchronous 409s
      // reach api() as a thrown Error with .code/.message from the stable envelope).
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  async function dischargeDebt() {
    const statusEl = document.getElementById("discharge-status");
    const btn = document.getElementById("btn-discharge-debt");
    btn.disabled = true;
    setStatus(statusEl, "calcolo...");
    try {
      const sub = await api("POST", "/prices/discharge-debt", undefined);
      const rec = await pollJob("/prices/discharge-debt/" + sub.job_id);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      const r = rec.result;
      document.getElementById("discharge-result").textContent =
        "Scansionati: " + r.scanned_products + " prodotti — non validi: " +
        r.broken_products + " prodotti / " + r.broken_variants + " varianti.";
      setStatus(statusEl, "completato", "ok");
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  function initPricesTab() {
    document.getElementById("prices-mode").addEventListener("change", updatePricesModeVisibility);
    document.getElementById("prices-form").addEventListener("submit", pricesPreview);
    document.getElementById("btn-prices-confirm").addEventListener("click", pricesConfirm);
    document.getElementById("btn-prices-revert").addEventListener("click", pricesRevert);
    document.getElementById("btn-discharge-debt").addEventListener("click", dischargeDebt);
    updatePricesModeVisibility();
  }

  // ===========================================================================
  // DELETE tab — zero-stock recon, bulk cleanup, single delete, multi-outlet
  // exit, deny-normalize (rule 7), the open RECONCILE rows and a person's close.
  // Every irreversible vertical is two steps: the preview keeps plan_hash +
  // confirm_token AND the inputs it was computed from, and the apply echoes all
  // of them with the CONFERMO gesture — what is shown is what is sent. A job
  // that comes back VERIFY_FAILED ran nothing: the plan moved, preview again.
  // ===========================================================================
  const cleanupState = { pending: null };
  const singleDeleteState = { pending: null };
  const exitState = { pending: null };
  const normalizeState = { pending: null };
  // Applies wait for the pass lease and touch Shopify once per outlet, and the
  // all-outlet normalize preview scans every outlet: poll as patiently as the
  // baseline does, not with the 2-minute default.
  const SLOW_POLL = INIT_POLL;
  const VERIFY_FAILED_TEXT = "piano cambiato: ricalcola l'anteprima";

  /** [[sku, size], ...] -> "SKU (taglia), ..." as inline text (a size can hold a "/": no slash separator). */
  function fmtKeys(keys) {
    return (keys || []).map(function (k) { return k[0] + " (" + k[1] + ")"; }).join(", ");
  }

  /** An ISO instant -> Europe/Rome wall-clock text (user-facing timestamps); "" for none. */
  function fmtRome(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    return isNaN(d.getTime()) ? String(iso) : d.toLocaleString("it-IT", { timeZone: "Europe/Rome" });
  }

  /** An apply job that is "done" but whose live re-plan no longer hashed as the preview: nothing ran. */
  function isVerifyFailed(rec) {
    return !!(rec.result && rec.result.status === "VERIFY_FAILED");
  }

  /**
   * Why an apply job has no usable `rec.result.report`: the job failed, the plan
   * moved (VERIFY_FAILED — checked before the report is read), or the answer is
   * not the contract's. null when the report is there.
   */
  function applyBlockedMessage(rec) {
    if (isVerifyFailed(rec)) return VERIFY_FAILED_TEXT;
    const failMsg = jobFailureMessage(rec);
    if (failMsg) return failMsg;
    return rec.result && rec.result.report ? null : "risposta inattesa dal server.";
  }

  /** A service report that is not APPLIED (lease unavailable/lost, ...): partial outcomes stay in the table. */
  function notCompletedText(report) {
    return "non completata: " + report.status + (report.reason ? " (" + report.reason + ")" : "") +
      " — controlla gli esiti";
  }

  /** Textarea -> trimmed, non-empty, de-duplicated lines (order kept). */
  function parseGidLines(text) {
    return (text || "").split("\n").map(function (l) { return l.trim(); })
      .filter(function (l, i, all) { return l && all.indexOf(l) === i; });
  }

  async function zeroStock() {
    const statusEl = document.getElementById("zero-stock-status");
    const btn = document.getElementById("btn-zero-stock");
    btn.disabled = true;
    setStatus(statusEl, "calcolo...");
    try {
      const sub = await api("POST", "/outlet/zero-stock", undefined);
      const rec = await pollJob("/outlet/zero-stock/" + sub.job_id);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      const r = rec.result;
      document.getElementById("zero-stock-table").classList.remove("hidden");
      document.getElementById("zero-stock-review-table").classList.remove("hidden");
      const tbody = document.getElementById("zero-stock-tbody");
      tbody.innerHTML = "";
      (r.candidates || []).forEach(function (c) {
        const tr = document.createElement("tr");
        tr.innerHTML =
          "<td>" + esc(c.product_gid) + "</td>" + "<td>" + esc(c.title) + "</td>" +
          "<td>" + esc(c.status) + "</td>" + "<td>" + esc(c.variant_count) + "</td>";
        tbody.appendChild(tr);
      });
      const reviewTbody = document.getElementById("zero-stock-review-tbody");
      reviewTbody.innerHTML = "";
      (r.review || []).forEach(function (c) {
        const tr = document.createElement("tr");
        tr.innerHTML =
          "<td>" + esc(c.product_gid) + "</td>" + "<td>" + esc(c.title) + "</td>" +
          "<td>" + esc(c.status) + "</td>" + "<td>" + esc((c.reasons || []).join(", ")) + "</td>";
        reviewTbody.appendChild(tr);
      });
      setStatus(statusEl, "scansionati " + r.scanned + ", candidati " + r.candidate_count, "ok");
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  function renderCleanupPlan(result) {
    const panel = document.getElementById("cleanup-plan-panel");
    panel.classList.remove("hidden");
    const plan = result.plan;
    document.getElementById("cleanup-count").textContent = result.count;
    document.getElementById("cleanup-second-confirm-flag").textContent =
      result.requires_second_confirm ? " (supera la soglia — richiede second_confirm)" : "";
    document.getElementById("cleanup-second-confirm-row").classList.toggle("hidden", !result.requires_second_confirm);
    document.getElementById("cleanup-second-confirm").checked = false;
    document.getElementById("cleanup-confirm-word").value = "";
    document.getElementById("cleanup-confirm-count").value = "";

    const tbody = document.getElementById("cleanup-candidates-tbody");
    tbody.innerHTML = "";
    (plan.candidates || []).forEach(function (c) {
      const tr = document.createElement("tr");
      tr.innerHTML = "<td>" + esc(c.product_gid) + "</td>" + "<td>" + esc(c.title) + "</td>" + "<td>" + esc(c.status) + "</td>";
      tbody.appendChild(tr);
    });
    const reviewTbody = document.getElementById("cleanup-review-tbody");
    reviewTbody.innerHTML = "";
    (plan.review || []).forEach(function (c) {
      const tr = document.createElement("tr");
      tr.innerHTML =
        "<td>" + esc(c.product_gid) + "</td>" + "<td>" + esc(c.title) + "</td>" +
        "<td>" + esc(c.status) + "</td>" + "<td>" + esc((c.reasons || []).join(", ")) + "</td>";
      reviewTbody.appendChild(tr);
    });
    // outlets whose SKU still has pairs on the shelf: listed apart, never deleted
    renderRows("cleanup-protected-tbody", plan.protected, function (c) {
      return [c.product_gid, c.title, c.status, (c.reasons || []).join(", ")]; });
    document.getElementById("cleanup-outcome-panel").classList.add("hidden");
  }

  async function cleanupPreview() {
    const statusEl = document.getElementById("cleanup-preview-status");
    const btn = document.getElementById("btn-cleanup-preview");
    btn.disabled = true;
    setStatus(statusEl, "calcolo piano...");
    try {
      const threshold = parseInt(document.getElementById("cleanup-threshold").value, 10) || 25;
      const archiveFirst = document.getElementById("cleanup-archive-first").checked;
      const sub = await api("POST", "/outlet/cleanup/preview", { threshold: threshold, archive_first: archiveFirst });
      const rec = await pollJob("/outlet/cleanup/preview/" + sub.job_id);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      cleanupState.pending = {
        plan_hash: rec.result.plan_hash,
        confirm_token: rec.result.confirm_token,
        threshold: threshold, archive_first: archiveFirst,
        count: rec.result.count, requires_second_confirm: rec.result.requires_second_confirm,
      };
      renderCleanupPlan(rec.result);
      setStatus(statusEl, "piano pronto — rivedi i candidati prima di confermare", "ok");
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  function renderCleanupOutcome(report) {
    const panel = document.getElementById("cleanup-outcome-panel");
    panel.classList.remove("hidden");
    const tbody = document.getElementById("cleanup-outcome-tbody");
    tbody.innerHTML = "";
    (report.outcomes || []).forEach(function (o) {
      const tr = document.createElement("tr");
      tr.innerHTML =
        "<td>" + esc(o.product_gid) + "</td>" + "<td>" + esc(o.status) + "</td>" +
        "<td>" + esc(o.deleted_id) + "</td>" + "<td>" + esc((o.warnings || []).join(", ")) + "</td>" +
        "<td>" + esc((o.closed_rows || []).join(", ")) + "</td>";
      tbody.appendChild(tr);
    });
  }

  async function cleanupApply() {
    if (!cleanupState.pending) return;
    const statusEl = document.getElementById("cleanup-apply-status");
    const btn = document.getElementById("btn-cleanup-apply");
    const confirmWord = document.getElementById("cleanup-confirm-word").value.trim();
    const confirmCountRaw = document.getElementById("cleanup-confirm-count").value;
    const secondConfirm = document.getElementById("cleanup-second-confirm").checked;
    if (confirmCountRaw === "") {
      setStatus(statusEl, "digita il conteggio esatto dei candidati per confermare", "error");
      return;
    }
    const confirmCount = parseInt(confirmCountRaw, 10);
    btn.disabled = true;
    setStatus(statusEl, "elimino...");
    try {
      const body = Object.assign({}, cleanupState.pending, {
        confirm: confirmWord, count: confirmCount, second_confirm: secondConfirm,
      });
      const sub = await api("POST", "/outlet/cleanup/apply", body);
      const rec = await pollJob("/outlet/cleanup/apply/" + sub.job_id, SLOW_POLL);
      const blocked = applyBlockedMessage(rec); // VERIFY_FAILED first, before any report is read
      if (blocked) {
        setStatus(statusEl, blocked, "error");
        return;
      }
      if (rec.result.report.verify_failed) {
        // the service's own re-verify under the lease: nothing was deleted
        setStatus(statusEl, VERIFY_FAILED_TEXT, "error");
        return;
      }
      renderCleanupOutcome(rec.result.report);
      setStatus(statusEl, "cleanup applicato: " + rec.result.report.deleted + " prodotti eliminati", "ok");
      cleanupState.pending = null;
    } catch (e) {
      // 409 gesture_required / confirm_invalid land here (sync response, no job).
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  // -- single delete: the step-(0) preview, then CONFERMO + apply -------------------
  /** The single delete's synchronous refusal: a non-outlet target (409 single_delete_not_outlet) has its own words. */
  function singleDeleteErrorText(e) {
    return e.code === "single_delete_not_outlet" ? "il GID non è un prodotto outlet" : "errore: " + e.message;
  }

  function renderSingleDeletePlan(plan) {
    document.getElementById("single-delete-plan-panel").classList.remove("hidden");
    document.getElementById("single-delete-title").textContent =
      (plan.title || "(senza titolo)") + " (" + plan.product_gid + ")";
    document.getElementById("single-delete-state").textContent = plan.status || "";
    document.getElementById("single-delete-skus").textContent = (plan.skus || []).join(", ") || "nessuno";
    document.getElementById("single-delete-keys").textContent =
      "Chiavi trattenute fino a un re-level: " + (fmtKeys(plan.keys) || "nessuna");
    document.getElementById("single-delete-shelf").textContent = (plan.shelf_pairs || []).join(", ") || "nessuna";
    document.getElementById("single-delete-open-rows").textContent = (plan.open_rows || []).join(", ") || "nessuna";
    document.getElementById("single-delete-confirm-word").value = "";
    setStatus(document.getElementById("single-delete-status"), "");
  }

  async function singleDeletePreview() {
    const statusEl = document.getElementById("single-delete-preview-status");
    const btn = document.getElementById("btn-single-delete-preview");
    const gid = document.getElementById("single-delete-gid").value.trim();
    if (!gid) {
      setStatus(statusEl, "product GID obbligatorio", "error");
      return;
    }
    singleDeleteState.pending = null;
    document.getElementById("single-delete-plan-panel").classList.add("hidden");
    btn.disabled = true;
    setStatus(statusEl, "calcolo anteprima...");
    try {
      const sub = await api("POST", "/outlet/delete/preview", { product_gid: gid });
      const rec = await pollJob("/outlet/delete/preview/" + sub.job_id);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      // the GID the plan was computed for is the one the apply echoes
      singleDeleteState.pending = {
        product_gid: gid, plan_hash: rec.result.plan_hash, confirm_token: rec.result.confirm_token,
      };
      renderSingleDeletePlan(rec.result.plan);
      setStatus(statusEl, "anteprima pronta — verifica e conferma", "ok");
    } catch (e) {
      setStatus(statusEl, singleDeleteErrorText(e), "error");
    } finally {
      btn.disabled = false;
    }
  }

  function singleDeleteReportText(report) {
    return report.status +
      (report.deleted_id ? " — " + report.deleted_id : "") +
      "; chiavi trattenute: " + (fmtKeys(report.held_keys) || "nessuna") +
      "; righe chiuse: " + ((report.closed_rows || []).join(", ") || "nessuna") +
      ((report.warnings || []).length ? "; warning: " + report.warnings.join(", ") : "");
  }

  async function singleDeleteApply() {
    const statusEl = document.getElementById("single-delete-status");
    const btn = document.getElementById("btn-single-delete");
    if (!singleDeleteState.pending) {
      setStatus(statusEl, "calcola prima l'anteprima", "error");
      return;
    }
    const confirmWord = document.getElementById("single-delete-confirm-word").value.trim();
    btn.disabled = true;
    setStatus(statusEl, "elimino...");
    try {
      const body = Object.assign({}, singleDeleteState.pending, { confirm: confirmWord, count: 1 });
      const sub = await api("POST", "/outlet/delete/apply", body);
      const rec = await pollJob("/outlet/delete/apply/" + sub.job_id, SLOW_POLL);
      const blocked = applyBlockedMessage(rec);
      if (blocked) {
        setStatus(statusEl, blocked, "error");
        if (isVerifyFailed(rec)) singleDeleteState.pending = null; // stale plan: preview again
        return;
      }
      const report = rec.result.report;
      if (report.status === "DELETED") {
        setStatus(statusEl, "eliminato: " + singleDeleteReportText(report), "ok");
        singleDeleteState.pending = null;
        document.getElementById("single-delete-confirm-word").value = "";
      } else if (report.status === "VERIFY_FAILED") {
        // the service's own re-verify under the lease: nothing was deleted
        setStatus(statusEl, VERIFY_FAILED_TEXT, "error");
        singleDeleteState.pending = null;
      } else {
        setStatus(statusEl, "non eliminato: " + singleDeleteReportText(report), "error");
      }
    } catch (e) {
      // 409 gesture_required / confirm_invalid / single_delete_not_outlet land here (sync, no job).
      setStatus(statusEl, singleDeleteErrorText(e), "error");
    } finally {
      btn.disabled = false;
    }
  }

  // -- multi-outlet exit: lower every other outlet of a SKU to 0 and delete it -------
  function renderExitPlan(plan) {
    document.getElementById("exit-plan-panel").classList.remove("hidden");
    document.getElementById("exit-keep-title").textContent =
      (plan.keep_title || "(senza titolo)") + " (" + plan.keep_gid + ")";
    renderRows("exit-outlets-tbody", plan.outlets, function (o) {
      return [o.product_gid, o.title, o.status,
        (o.lowerings || []).map(function (l) { return l.title + ":" + l.available; }).join(", "),
        (o.refusals || []).join(", ")];
    });
    document.getElementById("exit-keys").textContent =
      "Dopo l'uscita: re-level di " + (fmtKeys(plan.keys) || "nessuna chiave");
    document.getElementById("exit-confirm-word").value = "";
    setStatus(document.getElementById("exit-apply-status"), "");
    document.getElementById("exit-outcome-panel").classList.add("hidden");
  }

  async function exitPreview() {
    const statusEl = document.getElementById("exit-preview-status");
    const btn = document.getElementById("btn-exit-preview");
    const sku = document.getElementById("exit-sku").value.trim();
    const keepGid = document.getElementById("exit-keep-gid").value.trim();
    if (!sku || !keepGid) {
      setStatus(statusEl, "SKU e GID outlet da tenere obbligatori", "error");
      return;
    }
    exitState.pending = null;
    document.getElementById("exit-plan-panel").classList.add("hidden");
    document.getElementById("exit-outcome-panel").classList.add("hidden");
    btn.disabled = true;
    setStatus(statusEl, "calcolo anteprima...");
    try {
      const sub = await api("POST", "/outlet/exit/preview", { sku: sku, keep_product_gid: keepGid });
      const rec = await pollJob("/outlet/exit/preview/" + sub.job_id);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      const plan = rec.result.plan;
      if (plan.refused) {
        // a refused plan is never offered for apply
        setStatus(statusEl, "uscita rifiutata: " + plan.refused, "error");
        return;
      }
      exitState.pending = {
        sku: sku, keep_product_gid: keepGid,
        plan_hash: rec.result.plan_hash, confirm_token: rec.result.confirm_token,
      };
      renderExitPlan(plan);
      setStatus(statusEl, "anteprima pronta — verifica e conferma", "ok");
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  function renderExitOutcome(report) {
    document.getElementById("exit-outcome-panel").classList.remove("hidden");
    document.getElementById("exit-outcome-summary").textContent =
      "Stato " + report.status + (report.reason ? " (" + report.reason + ")" : "") +
      ". Chiavi da riallineare con un re-level: " + (fmtKeys(report.keys_to_relevel) || "nessuna") + ".";
    renderRows("exit-outcome-tbody", report.outcomes, function (o) {
      return [o.product_gid, o.status, (o.warnings || []).join(", "),
        (o.lowered_rows || []).join(", "), (o.closed_rows || []).join(", ")];
    });
  }

  async function exitApply() {
    const statusEl = document.getElementById("exit-apply-status");
    const btn = document.getElementById("btn-exit-apply");
    if (!exitState.pending) {
      setStatus(statusEl, "calcola prima l'anteprima", "error");
      return;
    }
    const confirmWord = document.getElementById("exit-confirm-word").value.trim();
    btn.disabled = true;
    setStatus(statusEl, "eseguo l'uscita (alcuni minuti)...");
    try {
      const body = Object.assign({}, exitState.pending, { confirm: confirmWord });
      const sub = await api("POST", "/outlet/exit/apply", body);
      const rec = await pollJob("/outlet/exit/apply/" + sub.job_id, SLOW_POLL);
      const blocked = applyBlockedMessage(rec);
      if (blocked) {
        setStatus(statusEl, blocked, "error");
        if (isVerifyFailed(rec)) exitState.pending = null; // stale plan: preview again
        return;
      }
      const report = rec.result.report;
      if (report.status === "VERIFY_FAILED") {
        // the service's own re-verify under the lease: nothing ran
        setStatus(statusEl, VERIFY_FAILED_TEXT, "error");
        exitState.pending = null;
        return;
      }
      renderExitOutcome(report);
      if (report.status === "APPLIED") {
        setStatus(statusEl, "uscita eseguita", "ok");
        exitState.pending = null;
        document.getElementById("exit-confirm-word").value = "";
      } else {
        setStatus(statusEl, notCompletedText(report), "error");
      }
    } catch (e) {
      // 409 gesture_required / confirm_invalid land here (sync response, no job).
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  // -- deny-normalize (rule 7) on the named outlets, or on every outlet --------------
  function renderNormalizePlan(plan) {
    const products = plan.products || [];
    document.getElementById("normalize-plan-panel").classList.remove("hidden");
    document.getElementById("normalize-summary").textContent =
      "Outlet scansionati " + plan.scanned + ", da normalizzare " + products.length +
      ", da rivedere " + (plan.review || []).length + ".";
    renderRows("normalize-products-tbody", products, function (p) {
      return [p.product_gid, p.title, p.status, (p.unsafe_variant_ids || []).length, (p.custom_lowerings || []).length];
    });
    renderRows("normalize-review-tbody", plan.review, function (r) { return [r[0], r[1]]; });
    document.getElementById("normalize-confirm-word").value = "";
    setStatus(document.getElementById("normalize-apply-status"), "");
    document.getElementById("btn-normalize-apply").disabled = !products.length; // nothing planned: nothing to apply
    document.getElementById("normalize-outcome-panel").classList.add("hidden");
  }

  async function normalizePreview() {
    const statusEl = document.getElementById("normalize-preview-status");
    const btn = document.getElementById("btn-normalize-preview");
    const gids = parseGidLines(document.getElementById("normalize-gids").value);
    normalizeState.pending = null;
    document.getElementById("normalize-plan-panel").classList.add("hidden");
    document.getElementById("normalize-outcome-panel").classList.add("hidden");
    btn.disabled = true;
    setStatus(statusEl, gids.length ? "calcolo anteprima..." : "calcolo anteprima su tutti gli outlet (alcuni minuti)...");
    try {
      const sub = await api("POST", "/outlet/deny-normalize/preview", { product_gids: gids });
      const rec = await pollJob("/outlet/deny-normalize/preview/" + sub.job_id, SLOW_POLL);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      // the list the plan was computed for is the list the apply echoes
      normalizeState.pending = {
        product_gids: gids, plan_hash: rec.result.plan_hash, confirm_token: rec.result.confirm_token,
      };
      renderNormalizePlan(rec.result.plan);
      setStatus(statusEl, "anteprima pronta — verifica e conferma", "ok");
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  function renderNormalizeOutcome(report) {
    document.getElementById("normalize-outcome-panel").classList.remove("hidden");
    renderRows("normalize-outcome-tbody", report.outcomes, function (o) {
      return [o.product_gid, o.status, (o.warnings || []).join(", "), (o.rows || []).join(", ")];
    });
  }

  async function normalizeApply() {
    const statusEl = document.getElementById("normalize-apply-status");
    const btn = document.getElementById("btn-normalize-apply");
    if (!normalizeState.pending) {
      setStatus(statusEl, "calcola prima l'anteprima", "error");
      return;
    }
    const confirmWord = document.getElementById("normalize-confirm-word").value.trim();
    btn.disabled = true;
    setStatus(statusEl, "applico (alcuni minuti)...");
    try {
      const body = Object.assign({}, normalizeState.pending, { confirm: confirmWord });
      const sub = await api("POST", "/outlet/deny-normalize/apply", body);
      const rec = await pollJob("/outlet/deny-normalize/apply/" + sub.job_id, SLOW_POLL);
      const blocked = applyBlockedMessage(rec);
      if (blocked) {
        setStatus(statusEl, blocked, "error");
        if (isVerifyFailed(rec)) normalizeState.pending = null; // stale plan: preview again
        return;
      }
      const report = rec.result.report;
      renderNormalizeOutcome(report);
      if (report.status === "APPLIED") {
        setStatus(statusEl, "normalizzazione applicata: " + (report.outcomes || []).length + " prodotti", "ok");
        normalizeState.pending = null;
        document.getElementById("normalize-confirm-word").value = "";
      } else {
        setStatus(statusEl, notCompletedText(report), "error");
      }
    } catch (e) {
      // 409 gesture_required / confirm_invalid land here (sync response, no job).
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  // -- the open RECONCILE rows, and a person's close of one --------------------------
  async function reconcileOpen() {
    const statusEl = document.getElementById("reconcile-open-status");
    const btn = document.getElementById("btn-reconcile-open");
    btn.disabled = true;
    setStatus(statusEl, "leggo...");
    try {
      const data = await api("GET", "/outlet/reconcile/open"); // synchronous read, no job
      const rows = data.rows || [];
      renderRows("reconcile-open-tbody", rows, function (r) {
        return [r.row_id, r.action, r.purpose, r.status, fmtKeys(r.keys), r.product_id, fmtRome(r.sent_at), r.closes_as];
      });
      setStatus(statusEl, rows.length + " righe aperte", "ok");
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  async function reconcileClose() {
    const statusEl = document.getElementById("reconcile-close-status");
    const btn = document.getElementById("btn-reconcile-close");
    const rowId = Number(document.getElementById("close-row-id").value); // "" -> 0, "12.5" -> refused: never truncated
    const why = document.getElementById("close-why").value.trim();
    if (!Number.isInteger(rowId) || rowId < 1) {
      setStatus(statusEl, "ID riga obbligatorio (numero intero)", "error");
      return;
    }
    if (!why) {
      setStatus(statusEl, "motivo obbligatorio", "error");
      return;
    }
    btn.disabled = true;
    setStatus(statusEl, "chiudo...");
    try {
      const sub = await api("POST", "/outlet/reconcile/close", {
        row_id: rowId,
        expect_status: document.getElementById("close-expect-status").value,
        why: why,
        confirm: document.getElementById("close-confirm-word").value.trim(),
      });
      const rec = await pollJob("/outlet/reconcile/close/" + sub.job_id, SLOW_POLL);
      document.getElementById("close-confirm-word").value = "";
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
      } else {
        const r = rec.result || {}; // the close report itself, not wrapped
        if (r.status === "CLOSED") {
          setStatus(statusEl, "chiusa come " + r.closed_as, "ok");
          document.getElementById("close-row-id").value = "";
          document.getElementById("close-why").value = "";
        } else {
          setStatus(statusEl, (r.status || "risposta inattesa dal server") + (r.reason ? " (" + r.reason + ")" : ""), "error");
        }
      }
      await reconcileOpen(); // the table now shows what is still open
    } catch (e) {
      // 409 gesture_required (CONFERMO, a reason, a closable status) lands here (sync, no job).
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  function initDeleteTab() {
    document.getElementById("btn-zero-stock").addEventListener("click", zeroStock);
    document.getElementById("btn-cleanup-preview").addEventListener("click", cleanupPreview);
    document.getElementById("btn-cleanup-apply").addEventListener("click", cleanupApply);
    document.getElementById("btn-single-delete-preview").addEventListener("click", singleDeletePreview);
    document.getElementById("btn-single-delete").addEventListener("click", singleDeleteApply);
    document.getElementById("btn-exit-preview").addEventListener("click", exitPreview);
    document.getElementById("btn-exit-apply").addEventListener("click", exitApply);
    document.getElementById("btn-normalize-preview").addEventListener("click", normalizePreview);
    document.getElementById("btn-normalize-apply").addEventListener("click", normalizeApply);
    document.getElementById("btn-reconcile-open").addEventListener("click", reconcileOpen);
    document.getElementById("btn-reconcile-close").addEventListener("click", reconcileClose);
  }

  // ===========================================================================
  // AUDIT tab
  // ===========================================================================
  async function loadAudit() {
    const limit = parseInt(document.getElementById("audit-limit").value, 10) || 50;
    try {
      const data = await api("GET", "/audit?limit=" + encodeURIComponent(limit));
      const events = data.events || [];
      const table = document.getElementById("audit-table");
      const thead = table.querySelector("thead");
      const tbody = document.getElementById("audit-tbody");
      thead.innerHTML = "";
      tbody.innerHTML = "";
      if (!events.length) return;
      const cols = Object.keys(events[0]);
      const headRow = document.createElement("tr");
      cols.forEach(function (c) {
        const th = document.createElement("th");
        th.textContent = c;
        headRow.appendChild(th);
      });
      thead.appendChild(headRow);
      events.forEach(function (ev) {
        const tr = document.createElement("tr");
        cols.forEach(function (c) {
          const td = document.createElement("td");
          td.textContent = ev[c] == null ? "" : ev[c];
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
    } catch (e) {
      toast("Errore caricamento audit: " + e.message, true);
    }
  }

  function initAuditTab() {
    document.getElementById("btn-audit-refresh").addEventListener("click", loadAudit);
  }

  // ===========================================================================
  // Boot
  // ===========================================================================
  document.addEventListener("DOMContentLoaded", function () {
    initInitBanner();
    initRelevel();
    initTabs();
    initScansiaTab();
    initPublishTab();
    initPricesTab();
    initDeleteTab();
    initAuditTab();
  });
})();

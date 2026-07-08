/**
 * Scansia Manager — MVP dashboard, vanilla JS (no framework, no CDN, no build).
 *
 * Every request is same-origin and gated by the server's HTTP Basic Auth
 * (backend.auth.basic_auth.require_basic_auth) — the BROWSER handles the auth
 * prompt (native 401 + WWW-Authenticate) and caches the credential for the
 * origin, so `fetch()` below carries no token/secret of any kind: there is
 * nothing to leak from this file.
 *
 * Load-bearing pattern used by publish / prices / cleanup: a *preview* job
 * returns `{plan, plan_hash, confirm_token}`. The client stores plan_hash +
 * confirm_token verbatim and echoes them back — unmodified — in the body of
 * the matching *apply* call. The server re-verifies both against the live
 * state (TOCTOU) before mutating anything; a drifted plan_hash comes back as
 * job status `VERIFY_FAILED`, surfaced here as "lo stato è cambiato dal
 * preview: rifai il preview".
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
  // INIT banner (first-run "Inizializza") — same preview->store token->echo-on-
  // apply flow as publish/cleanup. Hidden by default; shown only when
  // GET /init/status reports cutover_done === false.
  // ===========================================================================
  const initState = { pending: null };

  function renderInitDecisionRows(tbodyId, rows, cols) {
    const tbody = document.getElementById(tbodyId);
    tbody.innerHTML = "";
    (rows || []).forEach(function (d) {
      const tr = document.createElement("tr");
      tr.innerHTML = cols.map(function (c) { return "<td>" + esc(d[c]) + "</td>"; }).join("");
      tbody.appendChild(tr);
    });
  }

  function renderInitPlan(plan) {
    const panel = document.getElementById("init-plan-panel");
    panel.classList.remove("hidden");
    document.getElementById("init-backfill-count").textContent = plan.backfill_pending_rows;
    document.getElementById("init-anomalies").textContent =
      (plan.anomalies && plan.anomalies.length) ? "Anomalie: " + plan.anomalies.join(", ") : "";
    renderInitDecisionRows("init-kept-online-tbody", plan.kept_online, ["sku", "size", "target_gid"]);
    renderInitDecisionRows("init-demote-missing-tbody", plan.demote_missing, ["sku", "size"]);
    renderInitDecisionRows("init-demote-draft-tbody", plan.demote_draft, ["sku", "size", "target_gid", "live_status"]);
    renderInitDecisionRows("init-demote-soldout-tbody", plan.demote_sold_out_size, ["sku", "size", "target_gid"]);
    renderInitDecisionRows("init-review-tbody", plan.review_multi_match, ["sku", "size"]);
    const demoteCount = (plan.demote_missing || []).length + (plan.demote_draft || []).length + (plan.demote_sold_out_size || []).length;
    document.getElementById("init-second-confirm-row").classList.toggle("hidden", demoteCount <= 25);
    document.getElementById("init-second-confirm").checked = false;
    document.getElementById("init-confirm-word").value = "";
    document.getElementById("init-outcome-panel").classList.add("hidden");
  }

  async function initPreview() {
    const statusEl = document.getElementById("init-status");
    const btn = document.getElementById("btn-init-preview");
    btn.disabled = true;
    setStatus(statusEl, "calcolo piano...");
    try {
      const sub = await api("POST", "/init/preview", undefined);
      const rec = await pollJob("/init/preview/" + sub.job_id);
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

  function renderInitOutcome(report) {
    const panel = document.getElementById("init-outcome-panel");
    panel.classList.remove("hidden");
    const tbody = document.getElementById("init-outcome-tbody");
    tbody.innerHTML = "";
    (report.outcomes || []).forEach(function (o) {
      const tr = document.createElement("tr");
      tr.innerHTML =
        "<td>" + esc(o.sku) + "</td>" + "<td>" + esc(o.size) + "</td>" +
        "<td>" + esc(o.bucket) + "</td>" + "<td>" + esc(o.status) + "</td>" +
        "<td>" + esc(o.target_gid) + "</td>" + "<td>" + esc((o.warnings || []).join(", ")) + "</td>";
      tbody.appendChild(tr);
    });
  }

  async function initApply() {
    if (!initState.pending) return;
    const statusEl = document.getElementById("init-apply-status");
    const btn = document.getElementById("btn-init-apply");
    const confirmWord = document.getElementById("init-confirm-word").value.trim();
    const secondConfirm = document.getElementById("init-second-confirm").checked;
    btn.disabled = true;
    setStatus(statusEl, "applico...");
    try {
      const body = Object.assign({}, initState.pending, { confirm: confirmWord, second_confirm: secondConfirm });
      const sub = await api("POST", "/init/apply", body);
      const rec = await pollJob("/init/apply/" + sub.job_id);
      const failMsg = jobFailureMessage(rec);
      if (failMsg) {
        setStatus(statusEl, failMsg, "error");
        return;
      }
      // Defense-in-depth (LOW-c): jobFailureMessage() already maps every known
      // non-applied result (VERIFY_FAILED, gesture_required) above, but never
      // call the renderer on an unexpected/missing report — a raw JS crash is
      // worse than a generic error message.
      if (!rec.result || !rec.result.report) {
        setStatus(statusEl, "risposta inattesa dal server.", "error");
        return;
      }
      renderInitOutcome(rec.result.report);
      setStatus(statusEl, "applicato con successo", "ok");
      initState.pending = null;
      await checkInitStatus(); // hides the banner once cutover_done becomes true
    } catch (e) {
      // A thrown/rejected `api()` call means the HTTP response itself was non-2xx
      // (see `api()` above) — in practice ONLY confirm_invalid (bad/expired
      // confirm_token), which the server answers with a SYNCHRONOUS 409 before
      // any job exists. A missing/wrong CONFERMO (gesture_required) is NOT a
      // 409 — the job is created (202) and completes as "done" with
      // result.status === "gesture_required"; that case is handled above via
      // jobFailureMessage(), never reaches this catch.
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  async function checkInitStatus() {
    try {
      const data = await api("GET", "/init/status");
      document.getElementById("init-banner").classList.toggle("hidden", !!data.cutover_done);
    } catch (e) {
      // banner stays as-is on a transient error; not fatal to the rest of the app.
    }
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
    rows: [],
    inventoryBySku: {}, // sku -> join result (chips + freshness)
  };

  function renderScansiaTable() {
    const tbody = document.getElementById("scansia-tbody");
    tbody.innerHTML = "";
    scansiaState.rows.forEach(function (row) {
      const tr = document.createElement("tr");
      const join = scansiaState.inventoryBySku[row.sku];
      tr.innerHTML =
        "<td>" + esc(row.sku) + "</td>" +
        "<td>" + esc(row.size) + "</td>" +
        "<td>" + esc(row.qta) + "</td>" +
        "<td>" + esc(row.online) + "</td>" +
        "<td>" + esc(row.prezzo_high) + "</td>" +
        "<td>" + esc(row.prezzo_outlet) + "</td>" +
        "<td>" + esc(row.sconto) + "</td>" +
        "<td>" + (row.reconciled ? "sì" : "no") + "</td>" +
        "<td>" + esc((row.anomalies || []).join(", ")) + "</td>" +
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
      scansiaState.rows = data.rows || [];
      renderScansiaTable();
    } catch (e) {
      toast("Errore caricamento righe scansia: " + e.message, true);
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
        "<td>" + esc(d.price) + "</td>" + "<td>" + esc(d.compare_at) + "</td>" +
        "<td>" + esc(d.sheet_price) + "</td>" + "<td>" + esc(d.live_price) + "</td>" +
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
      const row_override = document.getElementById("prices-row-override").checked;
      const status_override = document.getElementById("prices-status-override").checked;
      const sub = await api("POST", "/prices/preview", { mode: mode, params: params, row_override: row_override, status_override: status_override });
      const rec = await pollJob("/prices/preview/" + sub.job_id);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      pricesState.pending = {
        plan_hash: rec.result.plan_hash,
        confirm_token: rec.result.confirm_token,
        mode: mode, params: params, row_override: row_override, status_override: status_override,
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
        "revert applicato: " + report.reverted_products + " prodotti / " + report.reverted_variants + " varianti",
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
  // DELETE tab
  // ===========================================================================
  const cleanupState = { pending: null };

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
        "<td>" + esc(o.deleted_id) + "</td>" + "<td>" + esc((o.warnings || []).join(", ")) + "</td>";
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
      const rec = await pollJob("/outlet/cleanup/apply/" + sub.job_id);
      const failMsg = jobFailureMessage(rec);
      if (failMsg) {
        setStatus(statusEl, failMsg, "error");
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

  async function singleDelete() {
    const statusEl = document.getElementById("single-delete-status");
    const btn = document.getElementById("btn-single-delete");
    const gid = document.getElementById("single-delete-gid").value.trim();
    const confirmWord = document.getElementById("single-delete-confirm-word").value.trim();
    if (!gid) {
      setStatus(statusEl, "product GID obbligatorio", "error");
      return;
    }
    btn.disabled = true;
    setStatus(statusEl, "elimino...");
    try {
      const sub = await api("POST", "/outlet/delete/apply", { product_gid: gid, confirm: confirmWord, count: 1 });
      const rec = await pollJob("/outlet/delete/apply/" + sub.job_id);
      if (rec.status === "failed") {
        setStatus(statusEl, jobFailureMessage(rec), "error");
        return;
      }
      setStatus(statusEl, "eliminato: " + JSON.stringify(rec.result), "ok");
      document.getElementById("single-delete-gid").value = "";
      document.getElementById("single-delete-confirm-word").value = "";
    } catch (e) {
      setStatus(statusEl, "errore: " + e.message, "error");
    } finally {
      btn.disabled = false;
    }
  }

  function initDeleteTab() {
    document.getElementById("btn-zero-stock").addEventListener("click", zeroStock);
    document.getElementById("btn-cleanup-preview").addEventListener("click", cleanupPreview);
    document.getElementById("btn-cleanup-apply").addEventListener("click", cleanupApply);
    document.getElementById("btn-single-delete").addEventListener("click", singleDelete);
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
    initTabs();
    initScansiaTab();
    initPublishTab();
    initPricesTab();
    initDeleteTab();
    initAuditTab();
  });
})();

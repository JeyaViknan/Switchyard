/* Switchyard console.
 *
 * One EventSource feeds everything. Each snapshot is handed to whichever page
 * is showing; the others do no work. Rendering is capped and pauses when the
 * tab is hidden, because this runs beside the gateway on a 1 GB box and a
 * console that steals CPU from the thing it is measuring is lying about it.
 *
 * No framework and no build step on purpose: the whole UI is three files the
 * browser loads directly, which is one less thing to install, pin, audit and
 * explain. The strict CSP means no inline styles or scripts, so anything
 * dynamic is set through the DOM rather than written into markup.
 */

import { drawMap } from "/static/map.js";

const $ = (id) => document.getElementById(id);

const PAGES = {
  "/": "page-map",
  "/tenants": "page-tenants",
  "/providers": "page-providers",
  "/demo": "page-demo",
  "/system": "page-system",
  "/api": "page-api",
};

const state = {
  snapshot: null,
  route: "/",
  history: [],
  breakerHistory: new Map(),   // provider -> [{at, state}]
  authed: false,
};

/* ------------------------------------------------------------------ utils */

const num = (v, digits = 1) =>
  v === null || v === undefined || Number.isNaN(v) ? "–" : Number(v).toFixed(digits);

const pct = (v) =>
  v === null || v === undefined ? "–" : `${(Number(v) * 100).toFixed(1)}%`;

const int = (v) => (v === null || v === undefined ? "–" : String(Math.round(v)));

function localTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  return Number.isNaN(d.getTime())
    ? ""
    : d.toLocaleTimeString(undefined, { hour12: false });
}

function setText(id, text) {
  const el = $(id);
  if (el) el.textContent = text;
}

/** Build an element. Never takes HTML: everything goes in as text. */
function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === null || v === undefined || v === false) continue;
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else node.setAttribute(k, v === true ? "" : String(v));
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined) continue;
    node.append(child);
  }
  return node;
}

/* ------------------------------------------------------------------ router */

function routeFromHash() {
  const raw = (location.hash || "#/").replace(/^#/, "");
  return PAGES[raw] ? raw : "/";
}

function showRoute() {
  state.route = routeFromHash();
  for (const [route, pageId] of Object.entries(PAGES)) {
    const page = $(pageId);
    if (page) page.hidden = route !== state.route;
  }
  for (const link of document.querySelectorAll(".tabs a")) {
    const current = link.dataset.route === state.route;
    if (current) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  }
  if (state.snapshot) render(state.snapshot);
  if (state.route === "/providers") loadHistory();
}

/* -------------------------------------------------------------- connection */

function setConnection(stateName, text) {
  const conn = $("conn");
  if (conn) conn.dataset.state = stateName;
  setText("conn-text", text);
}

let source = null;
let retryDelay = 1000;

function connect() {
  if (source) source.close();
  setConnection("connecting", "connecting");
  source = new EventSource("/api/stream");

  source.addEventListener("snapshot", (event) => {
    retryDelay = 1000;
    setConnection("live", "live");
    try {
      const snapshot = JSON.parse(event.data);
      state.snapshot = snapshot;
      // A stream opened before sign-in keeps reporting the session it was
      // opened with, so it may only ever downgrade this flag wrongly. Trust
      // it to grant access, never to revoke it mid-stream; `connect()` after
      // a login replaces the stream with one that carries the cookie.
      if (snapshot.authenticated) state.authed = true;
      recordBreakers(snapshot);
      render(snapshot);
    } catch (err) {
      console.error("bad snapshot", err);
    }
  });

  source.onerror = () => {
    setConnection("down", "reconnecting");
    source.close();
    // Backs off to 10s. The console reconnecting every 200ms against a
    // gateway that is down would add load to an already unhappy box.
    setTimeout(connect, retryDelay);
    retryDelay = Math.min(retryDelay * 2, 10000);
  };
}

async function loadHistory() {
  try {
    const response = await fetch("/api/history?minutes=10");
    if (response.ok) state.history = (await response.json()).points || [];
  } catch { /* a missing sparkline is not worth an error banner */ }
}

/* --------------------------------------------------------------- rendering */

let lastPaint = 0;
const MIN_FRAME_MS = 1000 / 30;     // 30fps ceiling

function render(snapshot) {
  if (document.hidden) return;
  const now = performance.now();
  if (now - lastPaint < MIN_FRAME_MS) return;
  lastPaint = now;

  renderChrome(snapshot);
  if (!snapshot.ready) return;

  switch (state.route) {
    case "/": renderMap(snapshot); break;
    case "/tenants": renderTenants(snapshot); break;
    case "/providers": renderProviders(snapshot); break;
    case "/demo": renderDemo(snapshot); break;
    case "/system": renderSystem(snapshot); break;
    case "/api": renderApi(snapshot); break;
  }
}

function renderChrome(snapshot) {
  const banner = $("mode-banner");
  const live = snapshot.mode === "live-providers";
  if (banner) banner.dataset.mode = snapshot.mode || "simulation";
  setText("mode-tag", live ? "LIVE PROVIDERS" : "SIMULATION");
  setText(
    "mode-text",
    live
      ? "at least one real provider is configured — requests cost money"
      : "synthetic providers · no real LLM calls · $0",
  );

  const kpis = snapshot.kpis || {};
  setText("kpi-rps", num(kpis.rps, 1));
  setText("kpi-success", pct(kpis.success_ratio_60s));
  setText("kpi-failovers", int(kpis.failovers_5m));
  setText("kpi-capacity", pct(kpis.capacity_utilisation));
  const build = snapshot.build || {};
  setText("kpi-build", build.version ? `${build.version} ${build.short_sha || ""}`.trim() : "–");

  const links = $("foot-links");
  if (links) {
    links.replaceChildren();
    const l = snapshot.links || {};
    if (l.grafana) links.append(el("a", { href: l.grafana, text: "Grafana" }), " · ");
    links.append(el("a", {
      href: "https://github.com/JeyaViknan/Switchyard", text: "Source",
    }));
  }
  setText("foot-note", snapshot.at ? `Updated ${localTime(snapshot.at)}` : "Switchyard");
}

function renderMap(snapshot) {
  drawMap($("map"), snapshot);
  renderEvents(snapshot.events || []);
}

const SEVERITY_ICON = { info: "•", warning: "▲", critical: "✕" };

function renderEvents(events) {
  const list = $("events");
  if (!list) return;
  list.replaceChildren(
    ...events.slice(0, 40).map((e) =>
      el("li", { "data-severity": e.severity },
        el("span", { class: "ev-icon", "aria-hidden": "true",
                     text: SEVERITY_ICON[e.severity] || "•" }),
        el("time", { datetime: e.at, text: localTime(e.at) }),
        el("span", { text: e.text }))),
  );
  if (!events.length) {
    list.replaceChildren(el("li", { "data-severity": "info" },
      el("span", { class: "ev-icon", "aria-hidden": "true", text: "•" }),
      el("span", { text: "Nothing has happened yet. Start a demo to make something happen." })));
  }
}

function renderTenants(snapshot) {
  const body = $("tenants-table")?.querySelector("tbody");
  if (!body) return;
  body.replaceChildren(...(snapshot.tenants || []).map((t) => {
    const budget = t.budget || {};
    const share = budget.limit ? `${pct((budget.available || 0) / budget.limit)} left` : "unlimited";
    return el("tr", {},
      el("th", { scope: "row", text: t.id }),
      el("td", { class: "num", text: num(t.weight, 1) }),
      el("td", { class: "num", text: int(t.floor) }),
      el("td", { class: "num", text: t.ceiling === null ? "–" : int(t.ceiling) }),
      el("td", { class: "num", text: int(t.inflight) }),
      el("td", { class: "num", text: int(t.queued) }),
      el("td", { class: "num", text: num(t.offered_rps, 1) }),
      el("td", { class: "num", text: num(t.admitted_rps, 1) }),
      el("td", { class: "num", text: num(t.rejected_rps, 1) }),
      el("td", { class: "num", text: t.queue_wait_p95_ms === null ? "no data"
                                      : `${int(t.queue_wait_p95_ms)} ms` }),
      el("td", { class: "num", text: num(t.tokens_per_s, 0) }),
      el("td", { text: share }));
  }));
}

const BREAKER_ICON = { closed: "✓", half_open: "◐", open: "✕" };
const BREAKER_WORD = { closed: "Closed", half_open: "Half-open", open: "Open" };

function renderProviders(snapshot) {
  const body = $("providers-table")?.querySelector("tbody");
  if (body) {
    body.replaceChildren(...(snapshot.providers || []).map((p) => {
      const reopen = p.state === "open" && p.reopens_in_s
        ? ` · retries in ${num(p.reopens_in_s, 0)}s` : "";
      const fault = p.fault
        ? Object.entries(p.fault).map(([k, v]) => `${k} ${v}`).join(", ")
        : "none";
      return el("tr", {},
        el("th", { scope: "row", text: p.name }),
        el("td", {}, el("span", { class: "badge", "data-kind": p.kind, text: p.kind })),
        el("td", {}, el("span", { class: "badge", "data-state": p.state,
                                  text: `${BREAKER_ICON[p.state] || "?"} ${BREAKER_WORD[p.state] || p.state}${reopen}` })),
        el("td", { class: "num", text: p.ttft_ewma_ms ? `${int(p.ttft_ewma_ms)} ms` : "–" }),
        el("td", { class: "num", text: num(p.ok_rps, 1) }),
        el("td", { class: "num", text: num(p.failed_rps, 1) }),
        el("td", { text: (p.routes_serving || []).join(", ") || "–" }),
        el("td", { text: fault }));
    }));
  }
  renderBreakerTimeline();
}

function recordBreakers(snapshot) {
  const now = Date.now();
  for (const p of snapshot.providers || []) {
    const series = state.breakerHistory.get(p.name) || [];
    const last = series[series.length - 1];
    if (!last || last.state !== p.state) series.push({ at: now, state: p.state });
    const cutoff = now - 10 * 60 * 1000;
    while (series.length > 1 && series[1].at < cutoff) series.shift();
    state.breakerHistory.set(p.name, series);
  }
}

const BREAKER_VAR = { closed: "--ok", half_open: "--warn", open: "--bad" };

function renderBreakerTimeline() {
  const wrap = $("breaker-timeline");
  if (!wrap) return;
  const now = Date.now();
  const window = 10 * 60 * 1000;
  wrap.replaceChildren(...[...state.breakerHistory.entries()].map(([name, series]) => {
    const bar = el("div", { class: "timeline-bar" });
    series.forEach((point, i) => {
      const end = i + 1 < series.length ? series[i + 1].at : now;
      const width = Math.max(0, ((end - point.at) / window) * 100);
      const seg = el("div", {
        class: "timeline-seg",
        title: `${BREAKER_WORD[point.state] || point.state} from ${localTime(new Date(point.at).toISOString())}`,
      });
      seg.style.width = `${width}%`;
      seg.style.background = `var(${BREAKER_VAR[point.state] || "--line"})`;
      bar.append(seg);
    });
    return el("div", { class: "timeline-row" },
      el("div", { class: "timeline-name", text: `${name} — last 10 minutes` }), bar);
  }));
}

/* -------------------------------------------------------------------- demo */

function renderDemo(snapshot) {
  const available = snapshot.demo_controls !== false;
  $("demo-disabled").hidden = available;
  $("demo-locked").hidden = !available || state.authed;
  $("demo-unlocked").hidden = !available || !state.authed;
  if (!available || !state.authed) return;

  renderDemoButtons(snapshot);
  renderFaultButtons(snapshot);
  renderScenario(snapshot.scenario);
}

/** Called when the session changes, so the next render rebuilds the controls. */
function invalidateControls() {
  rebuilt.clear();
}

const DEMO_ACTIONS = [
  { id: "noisy-neighbour", kind: "scenario", label: "Noisy neighbour",
    blurb: "One tenant floods at 25 req/s. Watch the other keep its lane." },
  { id: "provider-outage", kind: "scenario", label: "Provider outage",
    blurb: "Break 'fast' completely. Watch traffic arc to 'slow' and come back." },
  { id: "steady", kind: "traffic", label: "Steady traffic",
    blurb: "Normal load from three tenants, nothing going wrong." },
  { id: "stop", kind: "reset", label: "Stop and reset",
    blurb: "Stop traffic, end any demo, clear every injected fault.", danger: true },
];

/* Buttons are rebuilt only when something about them changes.
 * Re-creating them on every snapshot replaces the node under the pointer
 * once a second, which loses clicks and makes the page jump for anyone
 * using a keyboard. The signature is what the markup actually depends on. */
const rebuilt = new Map();

function changed(key, signature) {
  if (rebuilt.get(key) === signature) return false;
  rebuilt.set(key, signature);
  return true;
}

function renderDemoButtons(snapshot) {
  const wrap = $("demo-buttons");
  if (!wrap) return;
  const busy = Boolean(snapshot.scenario && !snapshot.scenario.finished);
  if (!changed("demo", String(busy))) return;
  wrap.replaceChildren(...DEMO_ACTIONS.map((action) => {
    const button = el("button", {
      class: `demo-btn${action.danger ? " danger" : ""}`,
      type: "button",
      disabled: busy && action.kind !== "reset",
    }, el("strong", { text: action.label }), el("span", { text: action.blurb }));
    button.addEventListener("click", () => runAction(action));
    return button;
  }));
}

function renderFaultButtons(snapshot) {
  const wrap = $("fault-buttons");
  if (!wrap) return;
  const providers = snapshot.providers || [];
  const signature = providers.map((p) => `${p.name}:${p.fault ? 1 : 0}`).join("|");
  if (!changed("faults", signature)) return;
  wrap.replaceChildren(...providers.map((p) => {
    const broken = Boolean(p.fault);
    const button = el("button", { class: `demo-btn${broken ? " danger" : ""}`, type: "button" },
      el("strong", { text: broken ? `Repair '${p.name}'` : `Break '${p.name}'` }),
      el("span", { text: broken
        ? "Clear the injected fault and let the breaker close again."
        : "Make every request to this provider fail, right now." }));
    button.addEventListener("click", () => post(
      `/api/faults/${encodeURIComponent(p.name)}`,
      broken ? { clear: true } : { error_rate: 1.0 }));
    return button;
  }));
}

function renderScenario(scenario) {
  const card = $("scenario-card");
  if (!card) return;
  card.hidden = !scenario;
  if (!scenario) return;
  setText("scenario-name", scenario.name);
  setText("scenario-headline", scenario.headline || "");
  setText("scenario-phase", scenario.phase);
  setText("scenario-elapsed", `· ${num(scenario.elapsed_s, 0)}s elapsed`);
  setText("scenario-summary", scenario.summary || "");

  const icons = { pass: "✓", fail: "✕", pending: "…" };
  $("scenario-checks").replaceChildren(...(scenario.checks || []).map((check) =>
    el("li", { "data-status": check.status },
      el("span", { class: "check-icon", "aria-hidden": "true", text: icons[check.status] }),
      el("span", {},
        el("span", { text: check.claim }),
        el("span", { class: "check-detail",
                     text: check.detail || `${check.status}…` })))));
}

async function runAction(action) {
  if (action.kind === "scenario") {
    await post(`/api/scenarios/${encodeURIComponent(action.id)}/start`, {});
  } else if (action.kind === "traffic") {
    await post("/api/traffic", { profile: action.id });
  } else {
    await post("/api/reset", {});
  }
}

async function post(path, body) {
  try {
    const response = await fetch(path, {
      method: "POST",
      headers: { "content-type": "application/json", "x-console": "1" },
      body: JSON.stringify(body),
    });
    if (response.status === 401) state.authed = false;
    return response.ok;
  } catch {
    return false;
  }
}

function wireLogin() {
  const form = $("login-form");
  if (!form) return;
  form.addEventListener("submit", async (submitEvent) => {
    submitEvent.preventDefault();
    const error = $("login-error");
    error.hidden = true;
    const response = await fetch("/api/login", {
      method: "POST",
      headers: { "content-type": "application/json", "x-console": "1" },
      body: JSON.stringify({ password: $("password").value }),
    });
    if (response.ok) {
      state.authed = true;
      $("password").value = "";
      invalidateControls();
      if (state.snapshot) renderDemo(state.snapshot);
      // The stream was opened before the cookie existed, so every snapshot
      // on it still reports authenticated:false -- which would re-hide the
      // controls a second after signing in. Reconnect so the stream carries
      // the session.
      connect();
    } else {
      error.textContent = response.status === 429
        ? "Too many attempts. Wait a minute."
        : "Wrong password.";
      error.hidden = false;
    }
  });
}

/* ------------------------------------------------------------------ system */

function renderSystem(snapshot) {
  const tiles = $("service-tiles");
  if (tiles) {
    tiles.replaceChildren(...Object.entries(snapshot.services || {}).map(([name, value]) =>
      el("div", { class: "tile", "data-state": value },
        el("div", { class: "tile-name", text: name }),
        el("div", { class: "tile-state", text: value }))));
  }

  const build = snapshot.build || {};
  const facts = [
    ["Version", build.version || "–"],
    ["Commit", build.git_sha || "–"],
    ["Gateway uptime", build.uptime_s ? `${int(build.uptime_s)}s` : "–"],
    ["Deployed at", build.deployed_at || "not set (running locally)"],
    ["Mode", snapshot.mode],
    ["Cost", "$0 — synthetic providers, free-tier infrastructure"],
  ];
  const list = $("build-facts");
  if (list) {
    list.replaceChildren(...facts.flatMap(([term, value]) =>
      [el("dt", { text: term }), el("dd", { text: String(value) })]));
    if (build.run_url) {
      list.append(el("dt", { text: "Deploy run" }),
                  el("dd", {}, el("a", { href: build.run_url, text: "GitHub Actions" })));
    }
  }

  setText("real-provider-snippet", [
    "[providers.openai]",
    'base_url = "https://api.openai.com/v1"',
    'api_key_env = "OPENAI_API_KEY"   # read from the environment, never stored',
    'upstream_model = "gpt-4o-mini"',
    "",
    "[routes]",
    'fast = ["openai", "local"]       # fall back to a self-hosted model',
  ].join("\n"));
}

function renderApi(snapshot) {
  const base = (snapshot.links && snapshot.links.api) || location.origin.replace(/:\d+$/, ":8000");
  setText("curl-snippet", [
    `curl -sN ${base}/v1/chat/completions \\`,
    '  -H "Authorization: Bearer <your-tenant-key>" \\',
    "  -H 'Content-Type: application/json' \\",
    `  -d '{"model":"fast","messages":[{"role":"user","content":"hello"}],"stream":true}'`,
  ].join("\n"));
  setText("sdk-snippet", [
    "from openai import OpenAI",
    "",
    `client = OpenAI(base_url="${base}/v1", api_key="<your-tenant-key>")`,
    'stream = client.chat.completions.create(',
    '    model="fast",',
    '    messages=[{"role": "user", "content": "hello"}],',
    "    stream=True,",
    ")",
  ].join("\n"));
}

/* -------------------------------------------------------------------- boot */

window.addEventListener("hashchange", showRoute);
document.addEventListener("visibilitychange", () => {
  // Nothing to draw for a tab nobody is looking at, and this shares a CPU
  // with the gateway.
  if (!document.hidden && state.snapshot) render(state.snapshot);
});

wireLogin();
showRoute();
connect();
loadHistory();

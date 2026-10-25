/* The live system map.
 *
 * An SVG diagram of tenants -> gateway capacity -> providers, driven entirely
 * by measured numbers. Nothing here is scripted: every bar length, slot fill
 * and particle rate comes from the snapshot, so if the gateway stops
 * protecting a tenant the picture shows it.
 *
 * Honesty rules this file follows:
 *   - A particle is not a request. It stands for a measured rate, sampled
 *     once a second, and the legend says so. Animating one dot per real
 *     request would be a lie at 25 rps and unreadable at 300.
 *   - Status is never colour alone. Every breaker state carries an icon and
 *     a word, because colour does not survive a projector or colour blindness.
 *   - Motion is decoration. Every number is also present as text, so
 *     prefers-reduced-motion turns the particles off and loses nothing.
 */

const SVG = "http://www.w3.org/2000/svg";
const TENANT_VARS = ["--t0", "--t1", "--t2", "--t3"];

const WIDE = { w: 1000, h: 560, stacked: false };
const NARROW = { w: 460, h: 940, stacked: true };

let particles = [];
let flows = [];
let rafHandle = null;
let lastFrame = 0;
let reducedMotion = false;

try {
  const query = window.matchMedia("(prefers-reduced-motion: reduce)");
  reducedMotion = query.matches;
  query.addEventListener("change", (e) => { reducedMotion = e.matches; });
} catch { /* ancient browser: keep the animation */ }

function node(tag, attrs = {}) {
  const element = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === null || v === undefined) continue;
    element.setAttribute(k, String(v));
  }
  return element;
}

function text(x, y, value, opts = {}) {
  const t = node("text", {
    x, y,
    "font-size": opts.size || 12,
    "font-weight": opts.weight || 400,
    fill: opts.fill || "currentColor",
    "text-anchor": opts.anchor || "start",
    "font-family": opts.mono ? "ui-monospace, Menlo, monospace" : "inherit",
    opacity: opts.opacity,
  });
  t.textContent = value;
  return t;
}

const tenantColour = (i) => `var(${TENANT_VARS[i % TENANT_VARS.length]})`;

const BREAKER = {
  closed: { icon: "✓", word: "Closed", colour: "var(--ok)" },
  half_open: { icon: "◐", word: "Half-open", colour: "var(--warn)" },
  open: { icon: "✕", word: "Open", colour: "var(--bad)" },
};

/* ------------------------------------------------------------------ layout */

/** Height the gateway panel needs: slot grid + legend + one queue bar per
 *  tenant. Measured rather than guessed, so adding a fourth tenant does not
 *  push its queue bar out through the bottom of the box. */
function gatewayHeight(snapshot, tenants) {
  const total = (snapshot.gateway && snapshot.gateway.max_concurrency) || 0;
  const perRow = total > 24 ? 8 : 6;
  const cell = Math.min(26, (360 - 40) / perRow - 6);
  const rows = Math.ceil(total / perRow);
  return 44 + rows * (cell + 6) + 50 + tenants.length * 16 + 24;
}

function geometry(snapshot, stacked) {
  const tenants = snapshot.tenants || [];
  const providers = snapshot.providers || [];

  if (stacked) {
    const cardW = 420, cardH = 54, gap = 8;
    return {
      box: NARROW,
      tenants: tenants.map((t, i) => ({
        data: t, i, x: 20, y: 40 + i * (cardH + gap), w: cardW, h: cardH,
        anchor: { x: 20 + cardW / 2, y: 40 + i * (cardH + gap) + cardH },
      })),
      gateway: { x: 20, y: 300, w: cardW, h: 250 },
      providers: providers.map((p, i) => ({
        data: p, i, x: 20, y: 600 + i * (cardH + 26), w: cardW, h: cardH + 18,
        anchor: { x: 20 + cardW / 2, y: 600 + i * (cardH + 26) },
      })),
      gatewayIn: { x: 230, y: 300 },
      gatewayOut: { x: 230, y: 550 },
    };
  }

  const cardW = 236, cardH = 104, gap = 12;
  return {
    box: WIDE,
    tenants: tenants.map((t, i) => ({
      data: t, i, x: 16, y: 52 + i * (cardH + gap), w: cardW, h: cardH,
      anchor: { x: 16 + cardW, y: 52 + i * (cardH + gap) + cardH / 2 },
    })),
    gateway: { x: 318, y: 40, w: 360, h: gatewayHeight(snapshot, tenants) },
    providers: providers.map((p, i) => ({
      data: p, i, x: 740, y: 60 + i * (cardH + 22), w: 244, h: cardH + 10,
      anchor: { x: 740, y: 60 + i * (cardH + 22) + (cardH + 10) / 2 },
    })),
    gatewayIn: { x: 318, y: 275 },
    gatewayOut: { x: 678, y: 275 },
  };
}

/* ------------------------------------------------------------------- cards */

function tenantCard(slot) {
  const t = slot.data;
  const g = node("g");
  const colour = tenantColour(slot.i);

  g.append(node("rect", {
    x: slot.x, y: slot.y, width: slot.w, height: slot.h, rx: 9,
    fill: "var(--panel)", stroke: "var(--line)",
  }));
  g.append(node("rect", {
    x: slot.x, y: slot.y, width: 4, height: slot.h, rx: 2, fill: colour,
  }));
  g.append(text(slot.x + 14, slot.y + 20, t.id, { weight: 600, size: 14 }));
  g.append(text(slot.x + slot.w - 12, slot.y + 20,
    `weight ${t.weight ?? "–"} · floor ${t.floor ?? 0}`,
    { anchor: "end", size: 11, fill: "var(--muted)" }));

  // Offered against served: the gap *is* the story when a tenant is refused.
  const barX = slot.x + 14, barW = slot.w - 28;
  const offered = Math.max(t.offered_rps || 0, 0.001);
  const servedFrac = Math.min(1, (t.admitted_rps || 0) / offered);
  const barY = slot.y + 32;
  g.append(node("rect", { x: barX, y: barY, width: barW, height: 8, rx: 4,
                          fill: "var(--line)" }));
  g.append(node("rect", { x: barX, y: barY, width: Math.max(2, barW * servedFrac),
                          height: 8, rx: 4, fill: colour }));
  if (servedFrac < 0.999 && (t.rejected_rps || 0) > 0) {
    g.append(node("rect", {
      x: barX + barW * servedFrac, y: barY,
      width: Math.max(2, barW * (1 - servedFrac)), height: 8, rx: 4,
      fill: "var(--bad)", opacity: 0.85,
    }));
  }

  // One fact per line. Two right-aligned strings on the same baseline fit
  // at 420px and collide at 236px, which is the width that matters.
  let line = barY + 23;
  g.append(text(barX, line,
    `${(t.admitted_rps || 0).toFixed(1)} of ${(t.offered_rps || 0).toFixed(1)} req/s served`,
    { size: 11, fill: "var(--muted)" }));

  line += 15;
  const wait = t.queue_wait_p95_ms;
  g.append(text(barX, line,
    `${t.inflight ?? 0} running · ${t.queued ?? 0} queued`
    + (wait === null || wait === undefined ? "" : ` · ${Math.round(wait)}ms wait`),
    { size: 11, fill: "var(--muted)" }));

  const budget = t.budget || {};
  if (budget.limit) {
    const left = (budget.available || 0) / budget.limit;
    g.append(text(barX, line + 15, `budget ${(left * 100).toFixed(0)}% left`,
      { size: 10.5, fill: left < 0.1 ? "var(--bad)" : "var(--muted)" }));
  }
  if ((t.rejected_rps || 0) > 0) {
    g.append(text(slot.x + slot.w - 12, line + 15,
      `▲ ${(t.rejected_rps).toFixed(0)}/s refused`,
      { anchor: "end", size: 10.5, fill: "var(--bad)", weight: 600 }));
  }
  return g;
}

function gatewayPanel(snapshot, geo) {
  const g = node("g");
  const box = geo.gateway;
  const gw = snapshot.gateway || {};
  const tenants = snapshot.tenants || [];

  g.append(node("rect", {
    x: box.x, y: box.y, width: box.w, height: box.h, rx: 11,
    fill: "var(--panel)", stroke: "var(--line)",
  }));
  g.append(text(box.x + 16, box.y + 24, "Gateway", { weight: 600, size: 15 }));
  g.append(text(box.x + box.w - 16, box.y + 24,
    `policy ${gw.policy || "–"}`, { anchor: "end", size: 11, fill: "var(--muted)" }));

  // Capacity as individual slots. The reserved floors are drawn first, in
  // tenant order, so you can see that a tenant's guarantee is a physical
  // block of the grid rather than a promise in a config file.
  const total = gw.max_concurrency || 0;
  const perRow = total > 24 ? 8 : 6;
  const cell = Math.min(26, (box.w - 40) / perRow - 6);
  const gridX = box.x + 18, gridY = box.y + 44;

  const owners = [];
  tenants.forEach((t, i) => {
    for (let n = 0; n < (t.floor || 0); n += 1) owners.push(i);
  });
  while (owners.length < total) owners.push(-1);     // the shared pool

  const filled = [];
  tenants.forEach((t, i) => {
    for (let n = 0; n < (t.inflight || 0); n += 1) filled.push(i);
  });

  for (let s = 0; s < total; s += 1) {
    const col = s % perRow, row = Math.floor(s / perRow);
    const x = gridX + col * (cell + 6), y = gridY + row * (cell + 6);
    const owner = owners[s];
    const occupant = filled[s];
    g.append(node("rect", {
      x, y, width: cell, height: cell, rx: 5,
      fill: occupant === undefined ? "var(--bg)" : tenantColour(occupant),
      stroke: owner === -1 ? "var(--line)" : tenantColour(owner),
      "stroke-width": owner === -1 ? 1 : 2,
      "stroke-dasharray": owner === -1 ? "3 3" : null,
    }));
  }

  const rows = Math.ceil(total / perRow);
  let y = gridY + rows * (cell + 6) + 14;
  g.append(text(gridX, y,
    `${gw.inflight ?? 0} of ${total} slots in use`, { size: 12, weight: 600 }));
  y += 15;
  g.append(text(gridX, y,
    "solid outline = reserved floor · dashed = shared pool",
    { size: 10.5, fill: "var(--muted)" }));

  // Queue bars, one per tenant, feeding the grid.
  y += 20;
  const maxQueue = Math.max(1, ...tenants.map((t) => t.queued || 0));
  tenants.forEach((t, i) => {
    const barW = box.w - 36;
    g.append(text(gridX, y + 9, t.id, { size: 11, fill: "var(--muted)" }));
    g.append(node("rect", { x: gridX + 62, y, width: barW - 62, height: 9, rx: 4,
                            fill: "var(--line)" }));
    const frac = (t.queued || 0) / maxQueue;
    if (frac > 0) {
      g.append(node("rect", {
        x: gridX + 62, y, width: Math.max(3, (barW - 62) * frac), height: 9, rx: 4,
        fill: tenantColour(i),
      }));
    }
    g.append(text(gridX + barW, y + 9, `${t.queued || 0} queued`,
      { anchor: "end", size: 10.5, fill: "var(--muted)" }));
    y += 16;
  });

  if (gw.draining) {
    g.append(text(gridX, y + 14, "▲ Draining — refusing new work, finishing what is running",
      { size: 11.5, fill: "var(--warn)", weight: 600 }));
  }
  return g;
}

function providerCard(slot) {
  const p = slot.data;
  const g = node("g");
  const look = BREAKER[p.state] || BREAKER.closed;

  g.append(node("rect", {
    x: slot.x, y: slot.y, width: slot.w, height: slot.h, rx: 9,
    fill: "var(--panel)", stroke: look.colour, "stroke-width": 1.5,
  }));
  g.append(text(slot.x + 14, slot.y + 21, p.name, { weight: 600, size: 14 }));
  g.append(text(slot.x + slot.w - 12, slot.y + 21, p.kind,
    { anchor: "end", size: 10.5, fill: p.kind === "external" ? "var(--warn)" : "var(--muted)" }));

  const reopen = p.state === "open" && p.reopens_in_s
    ? ` · retries in ${Math.round(p.reopens_in_s)}s` : "";
  g.append(text(slot.x + 14, slot.y + 41,
    `${look.icon} ${look.word}${reopen}`, { size: 12, fill: look.colour, weight: 600 }));

  g.append(text(slot.x + 14, slot.y + 60,
    `${(p.ok_rps || 0).toFixed(1)}/s ok · ${(p.failed_rps || 0).toFixed(1)}/s failed`,
    { size: 11, fill: "var(--muted)" }));
  if (p.ttft_ewma_ms) {
    g.append(text(slot.x + slot.w - 12, slot.y + 60, `${Math.round(p.ttft_ewma_ms)}ms TTFT`,
      { anchor: "end", size: 11, fill: "var(--muted)" }));
  }
  if (p.fault) {
    const label = Object.entries(p.fault)
      .map(([k, v]) => `${k.replace("_", " ")} ${typeof v === "number" ? `${v * 100}%` : v}`)
      .join(", ");
    g.append(node("rect", { x: slot.x + 12, y: slot.y + slot.h - 24, width: slot.w - 24,
                            height: 18, rx: 5, fill: "var(--bad)", opacity: 0.16 }));
    g.append(text(slot.x + 18, slot.y + slot.h - 11, `▲ injected: ${label}`,
      { size: 10.5, fill: "var(--bad)", weight: 600 }));
  }
  return g;
}

/* ---------------------------------------------------------------- particles */

function buildFlows(snapshot, geo) {
  const out = [];
  geo.tenants.forEach((slot) => {
    const t = slot.data;
    if ((t.admitted_rps || 0) > 0) {
      out.push({ from: slot.anchor, to: geo.gatewayIn,
                 rate: t.admitted_rps, colour: tenantColour(slot.i), kind: "admitted" });
    }
    if ((t.rejected_rps || 0) > 0) {
      // Rejected traffic bounces back off the gateway edge: it reached the
      // door and was turned around, which is a different picture from never
      // having been sent.
      out.push({ from: geo.gatewayIn, to: slot.anchor,
                 rate: t.rejected_rps, colour: "var(--bad)", kind: "rejected" });
    }
  });
  geo.providers.forEach((slot) => {
    const p = slot.data;
    if ((p.ok_rps || 0) > 0) {
      out.push({ from: geo.gatewayOut, to: slot.anchor,
                 rate: p.ok_rps, colour: "var(--ok)", kind: "served" });
    }
    if ((p.failed_rps || 0) > 0) {
      out.push({ from: geo.gatewayOut, to: slot.anchor,
                 rate: p.failed_rps, colour: "var(--bad)", kind: "failed" });
    }
  });
  return out;
}

function failoverArcs(snapshot, geo, layer) {
  const byName = new Map(geo.providers.map((s) => [s.data.name, s]));
  for (const f of snapshot.failovers || []) {
    const from = byName.get(f.from), to = byName.get(f.to);
    if (!from || !to) continue;
    const x = from.x + from.w / 2;
    const y1 = from.y + from.h / 2, y2 = to.y + to.h / 2;
    const bow = Math.max(40, Math.abs(y2 - y1) / 2);
    const path = node("path", {
      d: `M ${x} ${y1} Q ${x + bow} ${(y1 + y2) / 2} ${x} ${y2}`,
      fill: "none", stroke: "var(--warn)", "stroke-width": 2,
      "stroke-dasharray": "5 4", opacity: 0.9,
    });
    layer.append(path);
    layer.append(text(x + bow * 0.7, (y1 + y2) / 2,
      `failing over ${f.rps.toFixed(1)}/s`,
      { size: 10.5, fill: "var(--warn)", weight: 600 }));
  }
}

function step(timestamp) {
  rafHandle = null;
  const layer = document.getElementById("map-flows");
  if (!layer || reducedMotion || document.hidden) return;

  const dt = Math.min(0.1, (timestamp - lastFrame) / 1000 || 0.033);
  lastFrame = timestamp;

  // One particle per request-per-second, capped: past about 40 dots the
  // picture stops conveying anything extra and starts costing frames.
  for (const flow of flows) {
    flow.carry = (flow.carry || 0) + flow.rate * dt;
    while (flow.carry >= 1 && particles.length < 80) {
      flow.carry -= 1;
      particles.push({ flow, t: 0 });
    }
    if (flow.carry > 3) flow.carry = 3;
  }

  const keep = [];
  const frag = document.createDocumentFragment();
  for (const particle of particles) {
    particle.t += dt * 0.9;
    if (particle.t >= 1) continue;
    const { from, to } = particle.flow;
    const x = from.x + (to.x - from.x) * particle.t;
    const y = from.y + (to.y - from.y) * particle.t;
    frag.append(node("circle", {
      cx: x, cy: y, r: particle.flow.kind === "rejected" ? 2.6 : 3.2,
      fill: particle.flow.colour,
      opacity: particle.flow.kind === "rejected" ? 0.75 : 0.9,
    }));
    keep.push(particle);
  }
  particles = keep;
  layer.replaceChildren(frag);
  rafHandle = requestAnimationFrame(step);
}

/* -------------------------------------------------------------------- draw */

export function drawMap(svg, snapshot) {
  if (!svg || !snapshot || !snapshot.ready) return;

  const stacked = window.innerWidth < 720;
  const geo = geometry(snapshot, stacked);
  svg.setAttribute("viewBox", `0 0 ${geo.box.w} ${geo.box.h}`);

  const tenantLayer = document.getElementById("map-tenants");
  const gatewayLayer = document.getElementById("map-gateway");
  const providerLayer = document.getElementById("map-providers");
  if (!tenantLayer || !gatewayLayer || !providerLayer) return;

  const heading = (x, y, label) =>
    text(x, y, label, { size: 11, fill: "var(--muted)", weight: 600 });

  tenantLayer.replaceChildren(
    heading(16, 30, "TENANTS"),
    ...geo.tenants.map(tenantCard),
  );
  gatewayLayer.replaceChildren(gatewayPanel(snapshot, geo));
  const providerHeading = stacked
    ? heading(20, 586, "PROVIDERS")
    : heading(740, 40, "PROVIDERS");
  providerLayer.replaceChildren(
    providerHeading,
    ...geo.providers.map(providerCard),
  );
  failoverArcs(snapshot, geo, providerLayer);

  flows = buildFlows(snapshot, geo);
  if (reducedMotion) {
    const layer = document.getElementById("map-flows");
    if (layer) layer.replaceChildren();
  } else if (rafHandle === null) {
    lastFrame = performance.now();
    rafHandle = requestAnimationFrame(step);
  }
}

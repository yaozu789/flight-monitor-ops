"use strict";
const $ = (tag, attrs = {}, text) => {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  if (text !== undefined) el.textContent = text;  // never innerHTML: data is untrusted
  return el;
};
const css = name => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

// Real links are a few KB; a crafted one must not exhaust memory.
const MAX_LINK = 64 * 1024, MAX_JSON = 2 * 1024 * 1024;

async function decode(fragment) {
  if (!fragment.startsWith("v1.")) throw new Error("unknown link format");
  if (fragment.length > MAX_LINK) throw new Error("link too long");
  const b64 = fragment.slice(3).replace(/-/g, "+").replace(/_/g, "/");
  const bin = Uint8Array.from(atob(b64 + "=".repeat((4 - b64.length % 4) % 4)),
                              c => c.charCodeAt(0));
  const reader = new Blob([bin]).stream()
    .pipeThrough(new DecompressionStream("deflate-raw")).getReader();
  const parts = [];
  let size = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.length;
    if (size > MAX_JSON) { reader.cancel(); throw new Error("link data too large"); }
    parts.push(value);
  }
  const d = JSON.parse(await new Blob(parts).text());
  if (d.v !== 1 || !Array.isArray(d.watches) || !Array.isArray(d.runs)
      || typeof d.prices !== "object" || typeof d.spend !== "object"
      || !Array.isArray(d.events)) throw new Error("unexpected link data");
  return d;
}

const DAY = 86400000;
function rangeButtons(chart, spans, dataMin, dataMax) {
  const box = $("div", { class: "btns" });
  for (const [label, days] of spans) {
    const b = $("button", { type: "button" }, label);
    b.onclick = () => {
      box.querySelectorAll("button").forEach(x => x.classList.remove("on"));
      b.classList.add("on");
      const min = days ? Math.max(dataMin, dataMax - days * DAY) : dataMin;
      chart.zoomScale("x", { min, max: dataMax }, "default");
    };
    box.append(b);
  }
  box.lastChild.classList.add("on");
  return box;
}

const zoomOpts = {
  pan: { enabled: true, mode: "x" },
  zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: "x" },
};

function render(d) {
  const app = document.getElementById("app");
  const at = new Date(d.at);
  document.getElementById("sub").textContent =
    `Data as of ${at.toLocaleString()} · stored fares and run stats, no live search`;
  const palette = [css("--accent"), css("--accent2"), "#8a5cd6", "#1aa39a", "#c24d8f"];
  const grid = css("--line"), muted = css("--muted");
  Chart.defaults.color = muted;
  Chart.defaults.borderColor = grid;

  // Runs: decode slot deltas.
  let slot = d.run0;
  const runs = d.runs.map(([delta, status, attempts, stalled]) => {
    slot += delta;
    return { t: slot * 1800000, status, attempts, stalled };
  });
  const last = runs[runs.length - 1];
  const since24 = Date.now() - DAY;
  const runs24 = runs.filter(r => r.t >= since24);
  const today = new Date().toISOString().slice(0, 10);
  const spentToday = Object.values(d.spend[today] || {}).reduce((a, b) => a + b, 0);
  const stalls = runs.filter(r => r.stalled).length;

  const cards = $("div", { class: "cards" });
  const card = (k, v, cls = "") => {
    const c = $("div", { class: "card" });
    c.append($("div", { class: "k" }, k), $("div", { class: "v " + cls }, v));
    cards.append(c);
  };
  for (const [id, cap] of d.watches) {
    const pts = d.prices[id] || [];
    const lastLow = pts.length ? pts[pts.length - 1][1] : null;
    card(`${id} latest daily low (cap $${cap})`,
         lastLow === null ? "—" : `$${lastLow}`, lastLow !== null && lastLow <= cap ? "ok" : "");
  }
  card("Last run", last ? `${last.status}, ${last.attempts ?? "?"} req` : "—",
       last && last.status !== "completed" ? "bad" : "");
  card("Runs, last 24h", String(runs24.length));
  card("Requests today (UTC)", `${spentToday} / 150`);
  card(`Stalled runs, ${Math.round((Date.now() - d.run0 * 1800000) / DAY)}d`,
       String(stalls), stalls ? "bad" : "ok");
  app.append(cards);

  // Prices.
  const ps = $("section");
  const head = $("div", { class: "row" });
  head.append($("h2", {}, "Daily lowest fare (USD)"));
  ps.append(head);
  const pc = $("div", { class: "chart" }); const pcv = $("canvas"); pc.append(pcv); ps.append(pc);
  ps.append($("div", { class: "hint" },
    "Lowest stored fare per day, any dates and trip lengths in the window. " +
    "Drag or pinch to zoom, swipe to slide; tap for values. Dashed = cap."));
  app.append(ps);
  const priceSets = [];
  let pMin = Infinity, pMax = -Infinity;
  d.watches.forEach(([id, cap], i) => {
    // Stored days are UTC calendar days: place each at LOCAL midnight so
    // the axis shows the same date (not the previous day in Pacific time).
    const pts = (d.prices[id] || []).map(([day, low, n]) => ({ x: new Date(day + "T00:00").getTime(), y: low, n }));
    pts.forEach(p => { pMin = Math.min(pMin, p.x); pMax = Math.max(pMax, p.x); });
    const color = palette[i % palette.length];
    priceSets.push({ label: id, data: pts, borderColor: color, backgroundColor: color,
                     pointRadius: 2, tension: 0.2 });
    if (pts.length) priceSets.push({ label: `${id} cap`, borderColor: color, borderDash: [6, 4],
      borderWidth: 1, pointRadius: 0,
      data: [{ x: pts[0].x, y: cap }, { x: pts[pts.length - 1].x, y: cap }] });
  });
  if (!Number.isFinite(pMin)) { pMin = Date.now() - 7 * DAY; pMax = Date.now(); }
  const priceChart = new Chart(pcv, {
    type: "line", data: { datasets: priceSets },
    options: {
      maintainAspectRatio: false, parsing: true, interaction: { mode: "nearest", axis: "x", intersect: false },
      scales: { x: { type: "time", time: { unit: "day" } }, y: { ticks: { callback: v => "$" + v } } },
      plugins: {
        zoom: { ...zoomOpts, limits: { x: { min: pMin - DAY, max: pMax + DAY } } },
        tooltip: { callbacks: { label: c => c.dataset.label.endsWith("cap")
          ? `${c.dataset.label} $${c.parsed.y}`
          : `${c.dataset.label} $${c.parsed.y} (${c.raw.n} fares)` } },
      },
    },
  });
  head.append(rangeButtons(priceChart, [["7d", 7], ["30d", 30], ["90d", 90], ["All", 0]], pMin, pMax));

  // Requests per run.
  const rs = $("section");
  const rhead = $("div", { class: "row" });
  rhead.append($("h2", {}, "Requests per run"));
  rs.append(rhead);
  const rc = $("div", { class: "chart small" }); const rcv = $("canvas"); rc.append(rcv); rs.append(rc);
  rs.append($("div", { class: "hint" },
    "Each bar is one scheduled run. Red = stalled (sent nothing although a check was affordable); " +
    "yellow = stopped (deadline, budget, throttle or crash)."));
  app.append(rs);
  const color = r => r.stalled ? css("--bad")
    : r.status === "completed" ? css("--accent") : css("--warn");
  const rMin = runs.length ? runs[0].t : Date.now() - DAY, rMax = runs.length ? last.t : Date.now();
  const runChart = new Chart(rcv, {
    type: "bar",
    data: { datasets: [{ label: "requests", data: runs.map(r => ({ x: r.t, y: r.attempts ?? 0, r })),
      backgroundColor: runs.map(color), barThickness: 3, minBarLength: 3 }] },
    options: {
      maintainAspectRatio: false,
      scales: { x: { type: "time" }, y: { beginAtZero: true } },
      plugins: { legend: { display: false },
        zoom: { ...zoomOpts, limits: { x: { min: rMin - DAY, max: rMax + DAY } } },
        tooltip: { callbacks: { label: c => {
          const r = c.raw.r;
          return `${r.status}${r.stalled ? " (stalled)" : ""}: ${r.attempts ?? "no stats"} requests`;
        } } } },
    },
  });
  rhead.append(rangeButtons(runChart, [["1d", 1], ["7d", 7], ["All", 0]], rMin, rMax));

  // Daily spend by category.
  const ss = $("section");
  ss.append($("h2", {}, "Google requests per day (UTC), by category"));
  const sc = $("div", { class: "chart small" }); const scv = $("canvas"); sc.append(scv); ss.append(sc);
  app.append(ss);
  const days = Object.keys(d.spend).sort();
  const cats = [...new Set(days.flatMap(x => Object.keys(d.spend[x])))].sort();
  new Chart(scv, {
    type: "bar",
    data: { labels: days.map(x => x.slice(5)),
      datasets: cats.map((c, i) => ({ label: c, data: days.map(x => d.spend[x][c] || 0),
        backgroundColor: palette[i % palette.length], stack: "s" })) },
    options: { maintainAspectRatio: false,
      scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true, suggestedMax: 150 } } },
  });

  // Events.
  const es = $("section");
  es.append($("h2", {}, "Problems logged (latest 20)"));
  const ul = $("ul", { class: "events" });
  if (!d.events.length) ul.append($("li", {}, "None."));
  for (const [when, name, detail] of d.events)
    ul.append($("li", {}, `${when.replace("T", " ")} UTC · ${name}: ${detail}`));
  es.append(ul);
  app.append(es);
}

(async () => {
  const frag = location.hash.slice(1);
  if (!frag) {
    document.getElementById("sub").textContent = "";
    const e = $("div", { class: "empty" });
    e.append($("p", {}, "No data in this link."),
             $("p", {}, "Open the dashboard link from a Flight Monitor report. The numbers travel " +
                        "after the # in the link and never reach this server."));
    document.getElementById("app").append(e);
    return;
  }
  try { render(await decode(frag)); }
  catch (err) {
    document.getElementById("sub").textContent = "Could not read this link: " + err.message;
  }
})();

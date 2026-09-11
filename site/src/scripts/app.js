import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

// Frozen July 2025 snapshot: 8,339 Kingfisher County wells, each with
// three vendor positions and pairwise spheroid distances (meters).
// wells.json rows: {u, n, e:[lon,lat], o:[..], s:[..], d:[eo,es,os]}
const VENDORS = [
  { key: "o", label: "OCC", color: "#2a78d6" },
  { key: "e", label: "Enverus", color: "#eb6834" },
  { key: "s", label: "S&P Global", color: "#1baf7a" },
];
const fmt = (n) => Number(n).toLocaleString("en-US", { maximumFractionDigits: 0 });

let wells = [];
let threshold = 500;
let prefix = "";

const map = new maplibregl.Map({
  container: "map",
  style: "https://tiles.openfreemap.org/styles/positron",
  bounds: [
    [-98.3, 35.6],
    [-97.3, 36.25],
  ],
  fitBoundsOptions: { padding: 20 },
  attributionControl: { compact: true },
  cooperativeGestures: true,
});
map.addControl(new maplibregl.NavigationControl({ showCompass: false }));
map.on("error", (e) => console.error("[map]", e.error ?? e));
window._map = map;

const maxDist = (w) => Math.max(...w.d);

function filtered() {
  return wells.filter(
    (w) => maxDist(w) > threshold && (!prefix || w.u.startsWith(prefix)),
  );
}

function render() {
  const shown = filtered();
  const pts = [];
  const lines = [];
  for (const w of shown) {
    for (const v of VENDORS) {
      pts.push({
        type: "Feature",
        geometry: { type: "Point", coordinates: w[v.key] },
        properties: {
          vendor: v.label, color: v.color, u: w.u, n: w.n,
          deo: w.d[0], des: w.d[1], dos: w.d[2],
        },
      });
    }
    lines.push({
      type: "Feature",
      geometry: {
        type: "LineString",
        coordinates: [w.e, w.o, w.s, w.e],
      },
      properties: {},
    });
  }
  map.getSource("well-lines")?.setData(
    { type: "FeatureCollection", features: lines });
  map.getSource("well-pts")?.setData(
    { type: "FeatureCollection", features: pts });

  const pct = wells.length
    ? ((shown.length / wells.length) * 100).toFixed(1) : "0";
  document.getElementById("counts").textContent =
    `${fmt(shown.length)} of ${fmt(wells.length)} wells (${pct}%)`;

  const top = shown.slice().sort((a, b) => maxDist(b) - maxDist(a))
    .slice(0, 25);
  document.querySelector("#offenders tbody").innerHTML = top
    .map((w) => `<tr data-i="${wells.indexOf(w)}">
      <td><span class="fac">${w.n ?? w.u}</span>
        <span class="sub">${w.u}</span></td>
      <td class="num">${fmt(maxDist(w))}</td></tr>`)
    .join("");

  drawHistMarker();
}

// ---- histogram of max pairwise distance (all wells, log bins) ----
const BINS = [0, 10, 25, 50, 100, 250, 500, 1000, 2500, 10000, Infinity];
const BIN_LABELS = ["<10", "10", "25", "50", "100", "250", "500",
                    "1k", "2.5k", ">10k"];

function drawHist() {
  const counts = new Array(BINS.length - 1).fill(0);
  for (const w of wells) {
    const d = maxDist(w);
    for (let i = 0; i < BINS.length - 1; i++)
      if (d >= BINS[i] && d < BINS[i + 1]) { counts[i]++; break; }
  }
  const svg = document.getElementById("hist");
  const W = 640, H = 180, m = { t: 12, r: 10, b: 26, l: 8 };
  const bw = (W - m.l - m.r) / counts.length;
  const maxC = Math.max(...counts);
  let s = "";
  counts.forEach((c, i) => {
    const h = c ? Math.max(2, ((H - m.t - m.b) * c) / maxC) : 0;
    const x = m.l + i * bw;
    s += `<rect x="${x + 2}" y="${H - m.b - h}" width="${bw - 4}"
      height="${h}" rx="3" fill="#2a78d6"><title>${BIN_LABELS[i]}m:
      ${fmt(c)} wells</title></rect>`;
    if (c) s += `<text x="${x + bw / 2}" y="${H - m.b - h - 4}"
      text-anchor="middle" font-size="10" fill="#52514e">${fmt(c)}</text>`;
    s += `<text x="${x + bw / 2}" y="${H - 8}" text-anchor="middle"
      font-size="10" fill="#898781">${BIN_LABELS[i]}</text>`;
  });
  s += `<line id="hist-marker" x1="0" x2="0" y1="${m.t}" y2="${H - m.b}"
    stroke="#d95f00" stroke-width="2" stroke-dasharray="4 3"/>`;
  svg.innerHTML = s;
  drawHistMarker();
}

function drawHistMarker() {
  const marker = document.getElementById("hist-marker");
  if (!marker) return;
  const W = 640, m = { l: 8, r: 10 };
  const bw = (W - m.l - m.r) / (BINS.length - 1);
  let i = 0;
  while (i < BINS.length - 2 && BINS[i + 1] <= threshold) i++;
  const span = (BINS[i + 1] === Infinity ? BINS[i] * 4 : BINS[i + 1]) - BINS[i];
  const frac = Math.min(1, (threshold - BINS[i]) / span);
  const x = m.l + (i + frac) * bw;
  marker.setAttribute("x1", x);
  marker.setAttribute("x2", x);
}

// ---- map layers, tooltip, table clicks, controls ----
map.on("load", () => {
  map.addSource("well-lines", {
    type: "geojson",
    data: { type: "FeatureCollection", features: [] },
  });
  map.addLayer({
    id: "well-lines",
    type: "line",
    source: "well-lines",
    paint: { "line-color": "#898781", "line-width": 1.2, "line-opacity": 0.7 },
  });
  map.addSource("well-pts", {
    type: "geojson",
    data: { type: "FeatureCollection", features: [] },
  });
  map.addLayer({
    id: "well-pts",
    type: "circle",
    source: "well-pts",
    paint: {
      "circle-color": ["get", "color"],
      "circle-radius": 5,
      "circle-opacity": 0.85,
      "circle-stroke-color": "#fcfcfb",
      "circle-stroke-width": 0.8,
    },
  });

  const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false });
  map.on("mousemove", "well-pts", (e) => {
    map.getCanvas().style.cursor = "pointer";
    const p = e.features[0].properties;
    popup.setLngLat(e.features[0].geometry.coordinates).setHTML(`
      <div class="pop-name">${p.n ?? p.u}</div>
      <div class="pop-sub">API ${p.u} &middot; this point: ${p.vendor}</div>
      <div class="pop-row"><span>Enverus ↔ OCC</span>
        <span class="v">${fmt(p.deo)} m</span></div>
      <div class="pop-row"><span>Enverus ↔ S&amp;P</span>
        <span class="v">${fmt(p.des)} m</span></div>
      <div class="pop-row"><span>OCC ↔ S&amp;P</span>
        <span class="v">${fmt(p.dos)} m</span></div>
    `).addTo(map);
  });
  map.on("mouseleave", "well-pts", () => {
    map.getCanvas().style.cursor = "";
    popup.remove();
  });

  dataReady.then(render); // sync sources once both exist
});

// Data, histogram, table and counts do not wait on the map.
const dataReady = fetch("/wells.json")
  .then((r) => r.json())
  .then((data) => {
    wells = data;
    drawHist();
    render();
  });

document.getElementById("offenders").addEventListener("click", (e) => {
  const i = e.target.closest("tr[data-i]")?.dataset.i;
  if (i != null && wells[i])
    map.flyTo({ center: wells[i].o, zoom: 13.5 });
});

const slider = document.getElementById("threshold");
const label = document.getElementById("threshold-label");
slider.addEventListener("input", () => {
  threshold = Math.round(10 ** +slider.value);
  label.textContent = `disagreement > ${
    threshold >= 1000 ? (threshold / 1000).toFixed(1) + " km" : threshold + " m"}`;
  render();
});

document.getElementById("api-filter").addEventListener("input", (e) => {
  prefix = e.target.value.trim();
  render();
});

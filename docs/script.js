// Single source of truth for the sidebar. Adding a chapter = one entry.
const CHAPTERS = [
  {
    label: "Ch. 1 — Foundation",
    page: "chapters/chapter1.html",
    sections: [
      { id: "c1-goals", label: "Goals & Scope" },
      { id: "c1-layout", label: "Module & Layout" },
      { id: "c1-types", label: "Domain Types" },
      { id: "c1-tooling", label: "Tooling & CI" },
      { id: "c1-decisions", label: "Key Decisions" },
    ],
  },
  {
    label: "Ch. 2 — Storage Engine",
    page: "chapters/chapter2.html",
    sections: [
      { id: "c2-approach", label: "Approach" },
      { id: "c2-interface", label: "Store Interface" },
      { id: "c2-memory", label: "In-Memory Store" },
      { id: "c2-wal", label: "Write-Ahead Log" },
      { id: "c2-format", label: "Record Format" },
      { id: "c2-failures", label: "Failure Modes" },
      { id: "c2-realworld", label: "In the Wild & Growth" },
      { id: "c2-recovery", label: "Crash Recovery" },
      { id: "c2-compaction", label: "Compaction" },
      { id: "c2-testing", label: "Testing" },
      { id: "c2-fuzz", label: "Property & Fuzz Testing" },
    ],
  },
  {
    label: "Ch. 3 — Network Layer",
    page: "chapters/chapter3.html",
    sections: [
      { id: "c3-approach", label: "Approach & Transport" },
      { id: "c3-contract", label: "API Contract" },
      { id: "c3-routing", label: "Routing" },
      { id: "c3-handlers", label: "Handlers & the Store" },
      { id: "c3-errors", label: "Error Mapping" },
      { id: "c3-testing", label: "Testing" },
      { id: "c3-daemon", label: "Daemon & Lifecycle" },
    ],
  },
];

const CHEVRON =
  '<svg class="chevron" width="13" height="13" viewBox="0 0 24 24" fill="none" ' +
  'stroke="currentColor" stroke-width="2.5" stroke-linecap="round" ' +
  'stroke-linejoin="round" aria-hidden="true"><polyline points="6 9 12 15 18 9"></polyline></svg>';

const inChapters = location.pathname.includes("/chapters/");
const base = inChapters ? "../" : "";
const currentFile = location.pathname.split("/").pop() || "index.html";

function renderSidebar() {
  const brand =
    `<a class="brand" href="${base}index.html">` +
    `<div class="logo">K</div>` +
    `<div><h1>Keva</h1><div class="tag">Design Documentation</div></div>` +
    `</a>`;

  let groups =
    `<div class="group">` +
    `<a class="overview-link ${currentFile === "index.html" ? "active" : ""}" ` +
    `href="${base}index.html">Overview</a></div>`;

  for (const ch of CHAPTERS) {
    const onThisPage = ch.page.split("/").pop() === currentFile;
    const collapsed = !onThisPage; // expand only the chapter you're viewing
    const links = ch.sections
      .map((s) => {
        const href = onThisPage ? `#${s.id}` : `${base}${ch.page}#${s.id}`;
        return `<a class="sub" href="${href}">${s.label}</a>`;
      })
      .join("");
    groups +=
      `<div class="group${collapsed ? " collapsed" : ""}">` +
      `<button class="group-toggle" aria-expanded="${!collapsed}">` +
      `<span class="gt-label">${ch.label}</span>${CHEVRON}</button>` +
      `<div class="group-links">${links}</div></div>`;
  }

  document.getElementById("sidebar").innerHTML =
    brand + `<nav class="toc">${groups}</nav>`;
}

function wireTheme() {
  const root = document.documentElement;
  const saved = localStorage.getItem("keva-theme");
  if (saved) root.setAttribute("data-theme", saved);
  const btn = document.getElementById("themeBtn");
  if (!btn) return;
  btn.addEventListener("click", () => {
    const next = root.getAttribute("data-theme") === "dark" ? "light" : "dark";
    root.setAttribute("data-theme", next);
    localStorage.setItem("keva-theme", next);
  });
}

function wireCollapse() {
  document.querySelectorAll("nav.toc .group-toggle").forEach((btn) => {
    btn.addEventListener("click", () => {
      const group = btn.closest(".group");
      const collapsed = group.classList.toggle("collapsed");
      group.dataset.userCollapsed = collapsed ? "1" : "";
      btn.setAttribute("aria-expanded", String(!collapsed));
    });
  });
}

function wireScrollspy() {
  const links = [...document.querySelectorAll('nav.toc a[href^="#"]')];
  if (links.length === 0) return; // overview page has no in-page section links

  const byId = new Map(links.map((a) => [a.getAttribute("href").slice(1), a]));
  const sections = [...document.querySelectorAll("h3[id]")].filter((h) => byId.has(h.id));
  const visible = new Set();
  let paused = false;
  let pauseTimer = null;

  function setActive(id) {
    links.forEach((l) => l.classList.remove("active"));
    const a = byId.get(id);
    if (a) a.classList.add("active");
  }

  // The current section is the topmost one whose heading has scrolled past
  // the sticky top bar.
  function syncFromScroll() {
    const first = sections.find((s) => visible.has(s.id));
    if (first) setActive(first.id);
  }

  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((e) => {
        if (e.isIntersecting) visible.add(e.target.id);
        else visible.delete(e.target.id);
      });
      if (!paused) syncFromScroll();
    },
    { rootMargin: "-72px 0px -70% 0px", threshold: 0 }
  );
  sections.forEach((s) => observer.observe(s));

  // A click wins immediately, and the spy is paused during the smooth
  // scroll so it can't flicker to a section passing through mid-flight.
  links.forEach((a) => {
    a.addEventListener("click", () => {
      setActive(a.getAttribute("href").slice(1));
      paused = true;
      clearTimeout(pauseTimer);
      pauseTimer = setTimeout(() => {
        paused = false;
        syncFromScroll();
      }, 700);
    });
  });
}

renderSidebar();
wireTheme();
wireCollapse();
wireScrollspy();

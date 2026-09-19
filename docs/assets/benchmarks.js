/*
 * Renders the Benchmarks page from this crate's own published results.
 *
 * The numbers are fetched in the reader's browser instead of being written into the page: a run
 * rewrites `docs/benchmarks/results.json` and nothing else, so the table a reader sees, the
 * document the framework's site reads, and the run that produced them cannot drift apart. The
 * same document also feeds three translations, which is why no figure is ever typed into one.
 *
 * Prose is never written here. The page carries every label as JSON on the container, so each
 * translated page controls its own wording, and it carries the path to its own document, because
 * a translated page sits one directory deeper than the document it reads.
 */

(() => {
  "use strict";

  // Schema 2 added a section this page does not render; both still carry `scenarios`. A document
  // declaring anything else is not rendered as if it were one of these: printing wrong numbers is
  // worse than printing none.
  const SCHEMAS = [1, 2];
  const TIMEOUT_MS = 8000;

  // The environment fields, in the order the core's schema documents them. A field the run could
  // not determine is absent from the document and absent from the table; nothing is guessed here.
  const MACHINE = [
    "cpu",
    "architecture",
    "cpu_frequency",
    "cores",
    "memory",
    "memory_speed",
    "os",
    "broker",
    "rustc",
    "profile",
    "features",
    "rustflags",
  ];

  const text = (tag, value) => {
    const node = document.createElement(tag);
    node.textContent = value;
    return node;
  };

  async function load(url) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
    try {
      const response = await fetch(url, { signal: controller.signal });
      if (!response.ok) {
        return null;
      }
      const results = await response.json();
      return results && SCHEMAS.includes(results.schema) ? results : null;
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
  }

  const number = (value, lang) =>
    typeof value === "number" ? value.toLocaleString(lang, { maximumFractionDigits: 1 }) : "-";

  function side(measurement, unit, lang) {
    if (!measurement) {
      return "-";
    }
    const median = number(measurement.median, lang) + " " + unit;
    if (typeof measurement.min !== "number" || typeof measurement.max !== "number") {
      return median;
    }
    return median + " (" + number(measurement.min, lang) + "-" + number(measurement.max, lang) + ")";
  }

  function overhead(scenario, labels) {
    // The honesty rule of the methodology, enforced where it is read: a difference smaller than
    // the run-to-run spread is a verdict, never a percentage.
    let value =
      scenario.verdict === "indistinguishable"
        ? labels.indistinguishable
        : (scenario.overhead_percent >= 0 ? "+" : "") + scenario.overhead_percent + "%";
    if (scenario.broker_bound) {
      value += " (" + labels.brokerBound + ")";
    }
    return value;
  }

  function table(results, labels, lang) {
    const element = document.createElement("table");
    const head = element.createTHead().insertRow();
    for (const column of [labels.scenario, labels.raw, labels.framework, labels.overhead]) {
      head.appendChild(text("th", column));
    }
    const body = element.createTBody();
    for (const scenario of results.scenarios) {
      const row = body.insertRow();
      row.appendChild(text("td", scenario.name));
      row.appendChild(text("td", side(scenario.raw, scenario.unit, lang)));
      row.appendChild(text("td", side(scenario.framework, scenario.unit, lang)));
      row.appendChild(text("td", overhead(scenario, labels)));
    }
    return element;
  }

  function machine(results, labels) {
    const environment = results.environment || {};
    const rows = MACHINE.filter((field) => environment[field]).map((field) => [
      labels[field] || field,
      environment[field],
    ]);
    rows.push([
      labels.versions,
      results.crate + " " + results.crate_version + " on ruststream " + results.core_version,
    ]);
    rows.push([labels.measured, results.measured_at]);

    const element = document.createElement("table");
    const body = element.createTBody();
    for (const [label, value] of rows) {
      const row = body.insertRow();
      row.appendChild(text("th", label));
      row.appendChild(text("td", value));
    }
    return element;
  }

  async function main() {
    const container = document.getElementById("benchmark-results");
    const machineContainer = document.getElementById("benchmark-machine");
    if (!container) {
      return;
    }
    const labels = JSON.parse(container.dataset.benchmarkLabels);
    const lang = document.documentElement.lang || "en";
    for (const element of [container, machineContainer]) {
      element?.replaceChildren(text("p", labels.loading));
    }

    // A translated page is served one directory deeper than the document, which lives beside the
    // English page, so each page states where its own document is rather than guessing.
    const source = new URL(container.dataset.benchmarkResults || "results.json", location.href);
    const results = await load(source);
    if (!results) {
      // A broken publish is visible instead of silently missing.
      for (const element of [container, machineContainer]) {
        element?.replaceChildren(text("p", labels.unavailable));
      }
      return;
    }
    container.replaceChildren(table(results, labels, lang));
    machineContainer?.replaceChildren(machine(results, labels));
  }

  // Material swaps page content without a reload, so the tables are built on every navigation
  // rather than once per document.
  if (window.document$) {
    window.document$.subscribe(main);
  } else {
    document.addEventListener("DOMContentLoaded", main);
  }
})();

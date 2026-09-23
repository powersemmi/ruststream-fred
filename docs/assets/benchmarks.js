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
 *
 * A row carries three measurements - the client driven by hand, this crate's consumer driven by
 * hand, and the service a user writes - and the two differences between them, each with its own
 * verdict. The code table is the crate's own cost per message in instructions and allocations,
 * with what starting the service cost once.
 */

(() => {
  "use strict";

  // Schema 1 carried each loop as a median with its extremes, schema 2 added the `code` section,
  // and schema 3 reports each loop as its best, median and worst round. All three render. A
  // document declaring anything else is not rendered as if it were one of these: printing wrong
  // numbers is worse than printing none.
  const SCHEMAS = [1, 2, 3];
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
    "valgrind",
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
    if (typeof measurement.best === "number") {
      const best = number(measurement.best, lang) + " " + unit;
      // The parenthesis is the typical round: the median where the document carries one, and the
      // worst round where it does not. The worst round stays out of the cell otherwise, because
      // what it is there for is the spread the verdict rule reads.
      const typical =
        typeof measurement.median === "number" ? measurement.median : measurement.worst;
      if (typeof typical !== "number") {
        return best;
      }
      return best + " (" + number(typical, lang) + ")";
    }
    const median = number(measurement.median, lang) + " " + unit;
    if (typeof measurement.min !== "number" || typeof measurement.max !== "number") {
      return median;
    }
    return median + " (" + number(measurement.min, lang) + "-" + number(measurement.max, lang) + ")";
  }

  // The honesty rule of the methodology, enforced where it is read: a difference smaller than the
  // run-to-run spread is a verdict, never a percentage.
  function overhead(percent, verdict, labels) {
    if (verdict !== "measured" || typeof percent !== "number") {
      return labels.indistinguishable;
    }
    return (percent >= 0 ? "+" : "") + percent + "%";
  }

  function scenarioName(scenario, labels) {
    return scenario.broker_bound ? scenario.name + " (" + labels.brokerBound + ")" : scenario.name;
  }

  function table(results, labels, lang) {
    const element = document.createElement("table");
    const head = element.createTHead().insertRow();
    const columns = [
      labels.scenario,
      labels.raw,
      labels.adapter,
      labels.framework,
      labels.adapterOverhead,
      labels.overhead,
    ];
    for (const column of columns) {
      head.appendChild(text("th", column));
    }
    const body = element.createTBody();
    for (const scenario of results.scenarios) {
      const row = body.insertRow();
      row.appendChild(text("td", scenarioName(scenario, labels)));
      row.appendChild(text("td", side(scenario.raw, scenario.unit, lang)));
      row.appendChild(text("td", side(scenario.adapter, scenario.unit, lang)));
      row.appendChild(text("td", side(scenario.framework, scenario.unit, lang)));
      row.appendChild(
        text("td", overhead(scenario.adapter_overhead_percent, scenario.adapter_verdict, labels)),
      );
      row.appendChild(text("td", overhead(scenario.overhead_percent, scenario.verdict, labels)));
    }
    return element;
  }

  function code(results, labels, lang) {
    const element = document.createElement("table");
    const head = element.createTHead().insertRow();
    for (const column of [labels.scenario, labels.instructions, labels.allocations, labels.cold]) {
      head.appendChild(text("th", column));
    }
    const body = element.createTBody();
    for (const scenario of results.code) {
      const row = body.insertRow();
      row.appendChild(text("td", scenario.name));
      row.appendChild(text("td", number(scenario.framework?.instructions, lang)));
      row.appendChild(text("td", number(scenario.framework?.allocations, lang)));
      // Two numbers in one cell: what starting cost in instructions, and in allocations.
      row.appendChild(
        text(
          "td",
          scenario.cold
            ? number(scenario.cold.instructions, lang) +
                " / " +
                number(scenario.cold.allocations, lang)
            : "-",
        ),
      );
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
    const codeContainer = document.getElementById("benchmark-code");
    if (!container) {
      return;
    }
    const labels = JSON.parse(container.dataset.benchmarkLabels);
    const lang = document.documentElement.lang || "en";
    for (const element of [container, machineContainer, codeContainer]) {
      element?.replaceChildren(text("p", labels.loading));
    }

    // A translated page is served one directory deeper than the document, which lives beside the
    // English page, so each page states where its own document is rather than guessing.
    const source = new URL(container.dataset.benchmarkResults || "results.json", location.href);
    const results = await load(source);
    if (!results) {
      // A broken publish is visible instead of silently missing.
      for (const element of [container, machineContainer, codeContainer]) {
        element?.replaceChildren(text("p", labels.unavailable));
      }
      return;
    }
    container.replaceChildren(table(results, labels, lang));
    machineContainer?.replaceChildren(machine(results, labels));
    codeContainer?.replaceChildren(
      results.code?.length ? code(results, labels, lang) : text("p", labels.unavailable),
    );
  }

  // Material swaps page content without a reload, so the tables are built on every navigation
  // rather than once per document.
  if (window.document$) {
    window.document$.subscribe(main);
  } else {
    document.addEventListener("DOMContentLoaded", main);
  }
})();

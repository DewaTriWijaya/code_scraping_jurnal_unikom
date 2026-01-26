/**
 * Crossref Works Harvester (Gabungan Lengkap + Exact Matching + FILTER_UNIKOM + Batching)
 * Output CSV: Excel-safe, lengkap (~22 kolom)
 */

const fs = require("fs");
const fetch = require("node-fetch");
const csvParser = require("csv-parser");

// =========================
// CONFIG
// =========================
const INPUT_CSV = "author/openalex/dosen_openalex_by_name_only_cleaned.csv";
const OUTPUT_CSV = "jurnal/crossref/crossref_works_full.csv";

const YEAR_START = 2021;
const YEAR_END = 2026;

const START_INDEX = 203;

const ROWS_PER_REQUEST = 100;
const MAX_OFFSET = 10000;
const REQUEST_DELAY_MS = 500;
const AUTHOR_BATCH_SIZE = 8;

/** aktifkan jika ingin filter afiliasi UNIKOM */
const FILTER_UNIKOM = false;

// =========================
// HELPERS
// =========================
function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

function csvSafe(val) {
  if (val === null || val === undefined) return "";
  return `"${String(val).replace(/"/g, '""')}"`;
}

function normalizeName(name) {
  return name
    .toLowerCase()
    .replace(/[^a-z\s]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

// =========================
// AUTHOR MATCHING
// =========================
function isExactAuthorMatch(work, queryAuthor) {
  const target = normalizeName(queryAuthor);

  return (work.author || []).some(a => {
    const fullName = normalizeName(`${a.given || ""} ${a.family || ""}`);
    return fullName === target;
  });
}

function isExactAuthorFromUNIKOM(work, queryAuthor) {
  const target = normalizeName(queryAuthor);

  return (work.author || []).some(a => {
    const fullName = normalizeName(`${a.given || ""} ${a.family || ""}`);
    if (fullName !== target) return false;

    return (a.affiliation || []).some(aff => {
      const affName = normalizeName(aff.name || "");
      return affName.includes("universitas komputer indonesia") || affName.includes("unikom");
    });
  });
}

// =========================
// CSV INPUT
// =========================
function readAuthorsFromCSV(filePath) {
  return new Promise((resolve, reject) => {
    const authors = [];
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on("data", row => {
        const keys = Object.keys(row);
        const name = row[keys[1]]; // kolom ke-2 = nama author
        if (name) authors.push(name.trim());
      })
      .on("end", () => resolve(authors))
      .on("error", reject);
  });
}

// =========================
// CROSSREF FETCH
// =========================
async function fetchWorksByAuthorYear(authorName, year) {
  const works = [];
  let offset = 0;

  while (offset < MAX_OFFSET) {
    const url =
      `https://api.crossref.org/works` +
      `?query.author=${encodeURIComponent(authorName)}` +
      `&filter=from-pub-date:${year}-01-01,until-pub-date:${year}-12-31` +
      `&rows=${ROWS_PER_REQUEST}&offset=${offset}`;

    try {
      const res = await fetch(url);
      if (!res.ok) {
        console.error("❌ Fetch error:", res.status, url);
        break;
      }

      const data = await res.json();
      const items = data.message.items || [];
      if (items.length === 0) break;

      works.push(...items);
      offset += ROWS_PER_REQUEST;

      if (offset >= data.message["total-results"]) break;

      await sleep(REQUEST_DELAY_MS);

    } catch (err) {
      console.error("❌ Error fetch:", err.message, url);
      break;
    }
  }

  return works;
}

// =========================
// FLATTEN WORK TO CSV
// =========================
function workToCSVRow(work, authorQuery) {
  const authorsStr = (work.author || [])
    .map(a => `${a.given || ""} ${a.family || ""}`.trim())
    .join("; ");

  const containerTitle = work["container-title"]?.[0] || "";
  const shortContainerTitle = work["short-container-title"]?.[0] || "";
  const publishedDate = work["published-print"]?.["date-parts"]?.[0]?.join("-") ||
                        work["published-online"]?.["date-parts"]?.[0]?.join("-") ||
                        "";
  const indexedDateTime = work.indexed?.["date-time"] || "";
  const indexedDateParts = work.indexed?.["date-parts"]?.map(p => p.join("-")).join("; ") || "";
  const pdfLink = work.link?.find(l => l["content-type"] === "application/pdf")?.URL || "";
  const linkUrls = (work.link || []).map(l => l.URL).join("; ");
  const abstractStr = work.abstract || "";
  const issnStr = (work.ISSN || []).join("; ");
  const issnTypeStr = (work["issn-type"] || []).map(i => `${i.value}(${i.type})`).join("; ");

  return [
    authorQuery,
    work.DOI || "",
    work.title?.[0] || "",
    authorsStr,
    containerTitle,
    shortContainerTitle,
    work.publisher || "",
    work.issue || "",
    work.volume || "",
    work.page || "",
    publishedDate,
    work.type || "",
    work.source || "",
    pdfLink,
    linkUrls,
    abstractStr,
    work.score || "",
    issnStr,
    issnTypeStr,
    indexedDateTime,
    indexedDateParts,
    work.URL || ""
  ].map(csvSafe).join(",");
}

// =========================
// MAIN
// =========================
async function run() {
  const authors = await readAuthorsFromCSV(INPUT_CSV);

  const headers = [
    "author_query","doi","title","authors","container_title","short_container_title","publisher",
    "issue","volume","page","published","type","source","pdf_link","all_links","abstract",
    "score","issn","issn_type","indexed_date_time","indexed_date_parts","url"
  ];

  if (!fs.existsSync(OUTPUT_CSV)) {
    fs.writeFileSync(OUTPUT_CSV, headers.map(csvSafe).join(",") + "\n", "utf8");
  }

  // for (let i = 0; i < authors.length; i += AUTHOR_BATCH_SIZE) {
  for (let i = START_INDEX; i < authors.length; i += AUTHOR_BATCH_SIZE) {
    const batch = authors.slice(i, i + AUTHOR_BATCH_SIZE);
    console.log(`\n🚀 Batch ${i + 1} - ${i + batch.length}`);

    const stream = fs.createWriteStream(OUTPUT_CSV, { flags: "a" });

    for (const author of batch) {
      console.log(`🔍 Author: ${author}`);

      for (let year = YEAR_START; year <= YEAR_END; year++) {
        const works = await fetchWorksByAuthorYear(author, year);

        for (const w of works) {
          const ok = FILTER_UNIKOM
            ? isExactAuthorFromUNIKOM(w, author)
            : isExactAuthorMatch(w, author);

          if (!ok) continue;
          stream.write(workToCSVRow(w, author) + "\n");
        }
      }
    }

    await new Promise(res => stream.end(res));
    console.log("✅ Batch selesai");
  }

  console.log(`\n🎉 DONE → ${OUTPUT_CSV}`);
}

run().catch(console.error);

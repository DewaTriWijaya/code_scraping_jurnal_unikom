/**
 * Node.js Script: Ambil semua data works Crossref untuk list author
 * Filter publication_year 2020-2026
 * Menyimpan semua field penting
 * Output CSV Excel-safe
 */

const fs = require("fs");
const fetch = require("node-fetch");
const csvParser = require("csv-parser");

// =========================
// CONFIG
// =========================
const INPUT_CSV = "author/openalex/dosen_openalex_by_name_only_cleaned.csv"; // CSV input, kolom ke-2 = nama author
const OUTPUT_CSV = "jurnal/crossref/crossref_works_full.csv";
const YEAR_START = 2021;
const YEAR_END = 2026;
const ROWS_PER_REQUEST = 100;
const DELAY_MS = 500; // delay antar request

// =========================
// Excel-safe helper
// =========================
function csvSafe(val) {
  if (val === null || val === undefined) return "";
  const escaped = String(val).replace(/"/g, '""');
  return `"${escaped}"`;
}

// =========================
// Delay helper
// =========================
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// =========================
// Baca CSV input
// =========================
function readAuthorsFromCSV(filePath) {
  return new Promise((resolve, reject) => {
    const authors = [];
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on("data", (row) => {
        const keys = Object.keys(row);
        authors.push(row[keys[1]]); // kolom ke-2 = nama author
      })
      .on("end", () => resolve(authors))
      .on("error", reject);
  });
}

// =========================
// Ambil semua works per author per tahun
// =========================
async function getWorksByAuthorYear(authorName, year) {
  let allWorks = [];
  let offset = 0;
  while (offset < 10000) { // Crossref max offset = 10000
    const url = `https://api.crossref.org/works?query.author=${encodeURIComponent(authorName)}&filter=from-pub-date:${year}-01-01,until-pub-date:${year}-12-31&rows=${ROWS_PER_REQUEST}&offset=${offset}`;
    try {
      const res = await fetch(url);
      if (!res.ok) {
        console.error("❌ Fetch error:", res.status, url);
        break;
      }
      const data = await res.json();
      const items = data.message.items;
      if (!items || items.length === 0) break;

      allWorks.push(...items);
      offset += ROWS_PER_REQUEST;

      if (offset >= data.message["total-results"]) break;

      await sleep(DELAY_MS); // delay aman
    } catch (err) {
      console.error("❌ Error fetch:", err.message, url);
      break;
    }
  }
  return allWorks;
}

// =========================
// Flatten work ke CSV row
// =========================
function workToCSVRow(work, authorName) {
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
    authorName,
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
  const authors = ["LILIS PUSPITAWATI"];

  // Header CSV
  const headers = [
    "author_query","doi","title","authors","container_title","short_container_title","publisher",
    "issue","volume","page","published","type","source","pdf_link","all_links","abstract",
    "score","issn","issn_type","indexed_date_time","indexed_date_parts","url"
  ];
  let csv = headers.map(csvSafe).join(",") + "\n";

  for (const author of authors) {
    console.log(`🔍 Mengambil works untuk author: ${author}`);
    for (let year = YEAR_START; year <= YEAR_END; year++) {
      console.log(`  📅 Tahun: ${year}`);
      let works = await getWorksByAuthorYear(author, year);
      console.log(`  📄 Total works tahun ${year}: ${works.length}`);
      for (const w of works) {
        csv += workToCSVRow(w, author) + "\n";
      }
    }
  }

  fs.writeFileSync(OUTPUT_CSV, csv, "utf8");
  console.log(`\n🎉 SELESAI! Output: ${OUTPUT_CSV}`);
}

run().catch(console.error);

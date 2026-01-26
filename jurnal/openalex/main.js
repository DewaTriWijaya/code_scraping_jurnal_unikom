/**
 * Node.js Script: Ambil semua data works OpenAlex untuk list author
 * Filter publication_year 2020-2026
 * Output CSV Excel-safe
 * Semua atribut OpenAlex termasuk indexed_in, ISSN, topics, keywords, concepts, OA, dll
 */

const fs = require("fs");
const fetch = require("node-fetch");

// =========================
// CONFIG
// =========================
const INPUT_CSV = "author/openalex/dosen_openalex_by_name_only_cleaned.csv"; 
const OUTPUT_CSV = "jurnal/openalex/openalex_works_full.csv";
const PER_PAGE = 200;
const START_YEAR = 2020;
const END_YEAR = 2026;

// =========================
// Excel-safe helper
// =========================
function csvSafe(val) {
  if (val === null || val === undefined) return "";
  const escaped = String(val).replace(/"/g, '""');
  return `"${escaped}"`;
}

// =========================
// Baca CSV input
// =========================
function readAuthorsCSV(path) {
  const lines = fs.readFileSync(path, "utf8").trim().split("\n");
  const dataLines = lines.slice(1); // skip header
  return dataLines.map(line => {
    const parts = line.split(",").map(s => s.replace(/"/g, "").trim());
    return {
      author_name: parts[1],
      author_id: parts[2],
    };
  });
}

// =========================
// Ambil works per author (dengan pagination)
// =========================
async function getWorksByAuthor(authorId) {
  let works = [];
  let page = 1;
  let hasMore = true;

  while (hasMore) {
    const url = `https://api.openalex.org/works?filter=author.id:${authorId},publication_year:${START_YEAR}-${END_YEAR}&per_page=${PER_PAGE}&page=${page}`;
    const res = await fetch(url);
    const json = await res.json();
    works.push(...(json.results || []));

    const totalPages = Math.ceil((json.meta?.count || 0) / PER_PAGE);
    page++;
    hasMore = page <= totalPages;
  }

  return works;
}

// =========================
// Flatten work ke CSV row
// =========================
function workToCSVRow(work, author) {
  const loc = work.primary_location || {};
  const source = loc.source || {};

  // authors
  const authorsStr = (work.authorships || [])
    .map(a => {
      const name = a.author?.display_name || a.raw_author_name || "";
      const aff = (a.affiliations || [])
        .map(af => af.raw_affiliation_string)
        .join("; ");
      return `${name} [${aff}]`;
    })
    .join(" | ");

  // topics, keywords, concepts
  const topicsStr = (work.topics || []).map(t => t.display_name).join("; ");
  const keywordsStr = (work.keywords || []).map(k => k.display_name).join("; ");
  const conceptsStr = (work.concepts || []).map(c => c.display_name).join("; ");

  // abstract
  const abstractStr = work.abstract_inverted_index
    ? Object.keys(work.abstract_inverted_index).join(" ")
    : "";

  // counts by year
  const countsByYear = (work.counts_by_year || [])
    .map(c => `${c.year}:${c.cited_by_count}`)
    .join("; ");

  // referenced & related works
  const refWorks = (work.referenced_works || []).join("; ");
  const relatedWorks = (work.related_works || []).join("; ");

  // bibliographic info
  const biblio = work.biblio || {};
  const volume = biblio.volume || "";
  const issue = biblio.issue || "";
  const firstPage = biblio.first_page || "";
  const lastPage = biblio.last_page || "";

  // ISSN
  const issnStr = source.issn ? source.issn.join("; ") : "";
  const issnL = source.issn_l || "";

  // indexed_in
  const indexedIn = work.indexed_in ? work.indexed_in.join("; ") : "";

  // OA & license
  const oaStatus = loc.is_oa ? "yes" : "no";
  const license = loc.license_id || work.open_access?.oa_status || "";
  const bestOA = work.best_oa_location?.landing_page_url || "";

  // corresponding author & institution
  const corrAuthors = work.corresponding_author_ids?.join("; ") || "";
  const corrInsts = work.corresponding_institution_ids?.join("; ") || "";

  // citation percentile
  const citationPercentile = work.citation_normalized_percentile?.value || "";
  const top1 = work.citation_normalized_percentile?.is_in_top_1_percent ? "yes" : "no";
  const top10 = work.citation_normalized_percentile?.is_in_top_10_percent ? "yes" : "no";

  // cited year range
  const citedYearMin = work.cited_by_percentile_year?.min || "";
  const citedYearMax = work.cited_by_percentile_year?.max || "";

  // primary topic
  const primaryTopic = work.primary_topic || {};

  return [
    author.author_name,
    author.author_id,
    work.id,
    work.doi,
    work.title,
    work.display_name,
    work.publication_year,
    work.publication_date,
    work.type,
    loc.id || "",
    loc.landing_page_url || "",
    loc.pdf_url || "",
    source.display_name || "",
    source.host_organization_name || "",
    issnStr,
    issnL,
    oaStatus,
    license,
    bestOA,
    work.cited_by_count,
    indexedIn,
    authorsStr,
    topicsStr,
    keywordsStr,
    conceptsStr,
    abstractStr,
    countsByYear,
    refWorks,
    relatedWorks,
    volume,
    issue,
    firstPage,
    lastPage,
    corrAuthors,
    corrInsts,
    citationPercentile,
    top1,
    top10,
    citedYearMin,
    citedYearMax,
    primaryTopic.display_name || "",
    primaryTopic.id || "",
    primaryTopic.subfield?.display_name || "",
    primaryTopic.field?.display_name || "",
    primaryTopic.domain?.display_name || ""
  ].map(csvSafe).join(",");
}

// =========================
// MAIN
// =========================
async function run() {
  const authors = readAuthorsCSV(INPUT_CSV);

  // Header CSV
  const headers = [
    "author_name","author_id","work_id","doi","title","display_name",
    "publication_year","publication_date","type","primary_location_id",
    "primary_location_url","primary_location_pdf","venue_name","publisher",
    "issn_all","issn_l","open_access","license","best_oa_location",
    "cited_by_count","indexed_in","authors","topics","keywords","concepts",
    "abstract","counts_by_year","referenced_works","related_works",
    "volume","issue","first_page","last_page",
    "corresponding_authors","corresponding_insts","citation_percentile",
    "top_1","top_10","cited_year_min","cited_year_max",
    "primary_topic_name","primary_topic_id","primary_topic_subfield",
    "primary_topic_field","primary_topic_domain"
  ];
  let csv = headers.map(csvSafe).join(",") + "\n";

  for (const author of authors) {
    console.log(`🔍 Mengambil works untuk author: ${author.author_name} (${author.author_id})`);
    let works = [];
    try {
      works = await getWorksByAuthor(author.author_id);
    } catch (err) {
      console.error("❌ Error fetch works:", err.message);
      continue;
    }
    console.log(`📄 Total works: ${works.length}`);

    for (const w of works) {
      csv += workToCSVRow(w, author) + "\n";
    }
  }

  fs.writeFileSync(OUTPUT_CSV, csv, "utf8");
  console.log(`\n🎉 SELESAI! Output: ${OUTPUT_CSV}`);
}

run().catch(console.error);

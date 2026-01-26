/**
 * Matching author OpenAlex berdasarkan NAMA SAJA
 * TANPA afiliasi, TANPA institusi, TANPA NIDN
 * SEMUA ANGKA disimpan sebagai TEXT (AMAN EXCEL)
 */

const fs = require("fs");
const fetch = require("node-fetch");

// =========================
// FILE PATH
// =========================
const INPUT_CSV = "author/authors_cleaned.csv";
const OUTPUT_CSV = "author/openalex/dosen_openalex_by_name_only.csv";

// =========================
// GELAR
// =========================
const TITLE_PREFIX = [
  "prof", "prof.",
  "dr", "dr.",
  "ir", "ir.",
  "hj", "h.", "h"
];

const TITLE_SUFFIX = [
  "s.t", "st",
  "s.kom", "skom",
  "m.kom", "mkom",
  "m.t", "mt",
  "m.sc", "msc",
  "ph.d", "phd",
  "m.si", "msi",
  "m.m", "mm",
  "s.si", "ssi",
  "s.pd", "spd"
];

// =========================
// NORMALISASI NAMA
// =========================
function normalizeName(nama) {
  let n = nama.toLowerCase().replace(/,/g, " ");

  let removed = true;
  while (removed) {
    removed = false;
    TITLE_PREFIX.forEach(t => {
      const r = new RegExp(`^${t}\\s+`, "i");
      if (r.test(n)) {
        n = n.replace(r, "");
        removed = true;
      }
    });
  }

  let removedSuffix = true;
  while (removedSuffix) {
    removedSuffix = false;
    TITLE_SUFFIX.forEach(t => {
      const r = new RegExp(`[\\s,]+${t}$`, "i");
      if (r.test(n)) {
        n = n.replace(r, "");
        removedSuffix = true;
      }
    });
  }

  return n.replace(/\s+/g, " ").trim();
}

// =========================
// CSV READER
// =========================
function readCSV(path) {
  const lines = fs.readFileSync(path, "utf8").trim().split("\n");
  const dataLines = lines.slice(1);

  return dataLines.map(line => {
    const parts = line.split(",").map(s => s.replace(/"/g, "").trim());
    return {
      id_author: parts[0],
      nama: parts[2]
    };
  });
}

// =========================
// HELPER: EXCEL SAFE TEXT
// =========================
function asText(val) {
  if (val === null || val === undefined || val === "") return "";
  return `="${val}"`;
}

// =========================
// SEARCH OPENALEX (NAME ONLY)
// =========================
async function searchAuthor(nama) {
  const url =
    `https://api.openalex.org/authors` +
    `?search=${encodeURIComponent(nama)}` +
    `&per_page=10`;

  const res = await fetch(url);
  const json = await res.json();
  return json.results || [];
}

// =========================
// MAIN
// =========================
async function run() {
  const dosenList = readCSV(INPUT_CSV);

  let csv =
    "id_author,nama,openalex_id,orcid,relevance_score,works_count,cited_by_count,h_index,i10_index,2yr_mean_citedness\n";

  for (const d of dosenList) {
    const cleanName = normalizeName(d.nama);
    console.log(`🔍 ${d.nama} → ${cleanName}`);

    let match = null;

    try {
      const candidates = await searchAuthor(cleanName);
      match = candidates[0]; // hasil paling relevan
    } catch (err) {
      console.error("❌ Fetch error:", err.message);
    }

    if (!match) {
      csv += `"${d.nama}",,,,,,,,\n`;
      console.log("   ❌ Tidak ditemukan");
      continue;
    }

    const stats = match.summary_stats || {};

    csv += `"${d.id_author}",` +
           `"${d.nama}",` +
           `"${match.id}",` +
           `"${match.orcid || ""}",` +
           `${asText(match.relevance_score)},` +
           `${asText(match.works_count)},` +
           `${asText(match.cited_by_count)},` +
           `${asText(stats.h_index)},` +
           `${asText(stats.i10_index)},` +
           `${asText(stats["2yr_mean_citedness"])}\n`;

    console.log("   ✅ Match:", match.display_name);
  }

  fs.writeFileSync(OUTPUT_CSV, csv, "utf8");
  console.log(`\n🎉 SELESAI! Output: ${OUTPUT_CSV}`);
}

run().catch(console.error);

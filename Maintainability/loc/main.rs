use walkdir::WalkDir;
use csv::Writer;
use std::fs;

fn main() {
    let root_path = r"C:\Users\dewat\Desktop\Project Skripsi Agrimate\agrimate-android";
    let output_csv = "loc_mi.csv";

    let mut writer = Writer::from_path(output_csv).unwrap();
    writer
        .write_record(&["file", "loc_mi"])
        .unwrap();

    for entry in WalkDir::new(root_path)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "kt"))
    {
        let code = fs::read_to_string(entry.path()).unwrap();
        let loc = count_loc_mi(&code);

        writer
            .write_record(&[
                entry.file_name().to_string_lossy().as_ref(),
                &loc.to_string(),
            ])
            .unwrap();
    }

    writer.flush().unwrap();
    println!("✅ LOC (MI) exported → {}", output_csv);
}

/// LOC untuk Maintainability Index:
/// - non-blank
/// - non-comment
fn count_loc_mi(code: &str) -> usize {
    let mut loc = 0;
    let mut in_block_comment = false;

    for line in code.lines() {
        let mut line = line.trim();

        if line.is_empty() {
            continue;
        }

        // block comment start
        if line.starts_with("/*") {
            in_block_comment = true;
        }

        // skip block comment content
        if in_block_comment {
            if line.ends_with("*/") {
                in_block_comment = false;
            }
            continue;
        }

        // skip single-line comment
        if line.starts_with("//") {
            continue;
        }

        // count as LOC
        loc += 1;
    }

    loc
}

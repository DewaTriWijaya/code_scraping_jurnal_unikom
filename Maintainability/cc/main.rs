use std::fs;
use std::path::Path;


use csv::Writer;
use tree_sitter::{Node, Parser};
use tree_sitter_kotlin::language;
use walkdir::WalkDir;

fn main() {
    let root_path = r"C:\Users\dewat\Desktop\Project Skripsi Agrimate\agrimate-android";
    let output_csv = "cc_per_file.csv";

    let mut parser = Parser::new();
    parser
        .set_language(&language())
        .expect("Failed to load Kotlin grammar");

    let mut writer = Writer::from_path(output_csv).expect("Failed to create CSV");
    writer
        .write_record(["file_name", "file_path", "cyclomatic_complexity"])
        .unwrap();

    for entry in WalkDir::new(root_path)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| is_kotlin_file(e.path()))
    {
        let path = entry.path();
        let source = fs::read_to_string(path).unwrap();

        let tree = parser.parse(&source, None).unwrap();
        let root = tree.root_node();

        let mut cc = 1; // baseline McCabe
        count_cc(root, source.as_bytes(), &mut cc);

        writer
            .write_record(&[
                path.file_name().unwrap().to_string_lossy().as_ref(),
                path.to_string_lossy().as_ref(),
                cc.to_string().as_ref(),
            ])
            .unwrap();
    }

    writer.flush().unwrap();
    println!("✅ CC per file exported → {}", output_csv);
}

fn is_kotlin_file(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| e == "kt")
        .unwrap_or(false)
}

/// Count decision points ONLY
fn count_cc(node: Node, source: &[u8], cc: &mut i32) {
    match node.kind() {
        // branching
        "if_expression"
        | "when_entry"
        | "for_statement"
        | "while_statement"
        | "do_while_statement"
        | "catch_block" => {
            *cc += 1;
        }

        // logical operators
        "binary_expression" => {
            if let Some(op) = node.child_by_field_name("operator") {
                if let Ok(op_text) = op.utf8_text(source) {
                    if op_text == "&&" || op_text == "||" {
                        *cc += 1;
                    }
                }
            }
        }

        _ => {}
    }

    // IMPORTANT:
    // We still traverse lambda body,
    // but we DO NOT add +1 for lambda itself
    for i in 0..node.child_count() {
        if let Some(child) = node.child(i) {
            count_cc(child, source, cc);
        }
    }
}

use std::fs;
use std::path::Path;

use tree_sitter::{Parser, Query, QueryCursor};
use tree_sitter_kotlin::language;
use walkdir::WalkDir;

fn main() {
    let root_path = r"C:\Users\dewat\Desktop\Project Skripsi Agrimate\agrimate-android\app\src\main\java\com\codelabs\agrimate";
    let output_csv = "cloc_per_file.csv";

    let mut parser = Parser::new();
    parser.set_language(&language()).unwrap();

    let mut writer = csv::Writer::from_path(output_csv).unwrap();
    writer.write_record(["file_name", "file_path", "cloc"]).unwrap();

    // QUERY KHUSUS COMMENT
    let query = Query::new(
        &language(),
        r#"
        (line_comment) @comment
        (multiline_comment) @comment
        "#,
    )
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

        let mut cursor = QueryCursor::new();
        let mut cloc = 0;

        for m in cursor.matches(&query, root, source.as_bytes()) {
            for capture in m.captures {
                let node = capture.node;
                let text = &source[node.byte_range()];
                cloc += text.lines().count();
            }
        }

        writer
            .write_record(&[
                path.file_name().unwrap().to_string_lossy().as_ref(),
                path.to_string_lossy().as_ref(),
                &cloc.to_string(),
            ])
            .unwrap();
    }

    writer.flush().unwrap();
    println!("CLOC exported → {}", output_csv);
}

fn is_kotlin_file(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| e == "kt")
        .unwrap_or(false)
}

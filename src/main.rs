use std::fs;
use tree_sitter::{Parser, Node};
use tree_sitter_kotlin::language;

fn main() {
    // 1. Baca file Kotlin
    let source_code = fs::read_to_string("C:\\Users\\dewat\\Desktop\\Project Skripsi Agrimate\\script-pengujian\\kotlin_mi_analysis\\src\\HelpPlanScreen.kt")
        .expect("Gagal membaca file Kotlin");

    // 2. Setup parser
    let mut parser = Parser::new();
    parser
        .set_language(&language())
        .expect("Gagal load grammar Kotlin");

    // 3. Parse source code → Tree
    let tree = parser
        .parse(&source_code, None)
        .expect("Gagal parsing");

    // 4. Ambil root node
    let root_node = tree.root_node();

    println!("Root node: {}", root_node.kind());

    // 5. Traversal AST
    print_tree(root_node, &source_code, 0);
}

// Fungsi rekursif untuk cetak AST
fn print_tree(node: Node, source: &str, indent: usize) {
    let padding = "  ".repeat(indent);

    println!(
        "{}{} [{}..{}]",
        padding,
        node.kind(),
        node.start_byte(),
        node.end_byte()
    );

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        print_tree(child, source, indent + 1);
    }
}

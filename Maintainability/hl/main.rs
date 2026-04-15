use walkdir::WalkDir;
use regex::Regex;
use csv::Writer;
use std::{fs, collections::HashSet};

fn main() {
    let root_path = r"C:\Users\dewat\Desktop\Project Skripsi Agrimate\agrimate-android";
    let output_csv = "halstead_defensible.csv";

    let mut writer = Writer::from_path(output_csv).unwrap();
    writer.write_record(&[
        "file",
        "N1", "N2",
        "n1", "n2",
        "length",
        "vocabulary",
        "volume",
        "difficulty",
        "effort",
        "bugs"
    ]).unwrap();

    for entry in WalkDir::new(root_path)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "kt"))
    {
        let mut code = fs::read_to_string(entry.path()).unwrap();

        // 🔴 REMOVE package & import lines
        code = code
            .lines()
            .filter(|l| {
                let t = l.trim();
                !t.starts_with("package ") && !t.starts_with("import ")
            })
            .collect::<Vec<_>>()
            .join("\n");

        // 🔴 REMOVE annotations completely
        code = Regex::new(r"@\w+").unwrap().replace_all(&code, "").to_string();

        let tokens = tokenize_kotlin(&code);

        let mut operators = Vec::new();
        let mut operands = Vec::new();

        for t in tokens {
            classify_token(&t, &mut operators, &mut operands);
        }

        if operators.is_empty() && operands.is_empty() {
            continue;
        }

        let N1 = operators.len();
        let N2 = operands.len();

        let n1: HashSet<_> = operators.iter().cloned().collect();
        let n2: HashSet<_> = operands.iter().cloned().collect();

        let length = N1 + N2;
        let vocabulary = n1.len() + n2.len();
        let volume = (length as f64) * (vocabulary as f64).log2();
        let difficulty = if n2.is_empty() {
            0.0
        } else {
            (n1.len() as f64 / 2.0) * (N2 as f64 / n2.len() as f64)
        };
        let effort = difficulty * volume;
        let bugs = volume / 3000.0;

        writer.write_record(&[
            entry.file_name().to_string_lossy().as_ref(),
            &N1.to_string(),
            &N2.to_string(),
            &n1.len().to_string(),
            &n2.len().to_string(),
            &length.to_string(),
            &vocabulary.to_string(),
            &format!("{:.2}", volume),
            &format!("{:.2}", difficulty),
            &format!("{:.2}", effort),
            &format!("{:.4}", bugs),
        ]).unwrap();
    }

    writer.flush().unwrap();
    println!("✅ Halstead FINAL (academically defensible) completed");
}

fn tokenize_kotlin(code: &str) -> Vec<String> {
    let re = Regex::new(
        r#"==|!=|<=|>=|\|\||&&|[=+\-*/<>:]|\b\d+(\.\d+)?\b|\b[A-Za-z_][A-Za-z0-9_]*\b"#
    ).unwrap();

    re.find_iter(code)
        .map(|m| m.as_str().to_string())
        .collect()
}

fn classify_token(token: &str, operators: &mut Vec<String>, operands: &mut Vec<String>) {
    // Operators = computational + control
    let operator_re = Regex::new(
        r"^(if|else|when|for|while|return|\+|-|\*|/|==|!=|<=|>=|=|&&|\|\||:)$"
    ).unwrap();

    // Declarative keywords → ignore
    let ignore_re = Regex::new(
        r"^(class|data|object|interface|sealed|fun|val|var)$"
    ).unwrap();

    let literal_re = Regex::new(
        r#"^(".*"|'.*'|\d+(\.\d+)?|true|false|null)$"#
    ).unwrap();

    let identifier_re = Regex::new(
        r"^[A-Za-z_][A-Za-z0-9_]*$"
    ).unwrap();

    if operator_re.is_match(token) {
        operators.push(token.to_string());
    } else if ignore_re.is_match(token) {
        // skip
    } else if literal_re.is_match(token) || identifier_re.is_match(token) {
        operands.push(token.to_string());
    }
}

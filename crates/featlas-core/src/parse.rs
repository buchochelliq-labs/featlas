//! Restricted YAML/JSON profile (SEM-PARSE-001).
//!
//! The reader builds a JSON value directly from parser events so that every
//! forbidden construct is rejected instead of being silently interpreted:
//! multiple documents, anchors, aliases, merge keys, explicit/custom tags,
//! duplicate or non-string keys, ambiguous plain scalars, implicit timestamps,
//! non-finite or out-of-range numbers and `${...}` interpolation markers.
//! JSON input must first be syntactically valid JSON and is then read through
//! the same event builder (JSON is a YAML 1.2 flow subset), so the two formats
//! share one set of rules and source locations.

use crate::diag::{Diagnostic, SourceLocation, Stage};
use regex::Regex;
use serde_json::{Map, Number, Value};
use std::collections::HashMap;
use std::sync::OnceLock;
use yaml_rust2::parser::{Event, Parser};
use yaml_rust2::scanner::{Marker, TScalarStyle};

pub const MAX_DOCUMENT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_DEPTH: usize = 128;
pub const MAX_NODES: usize = 2_000_000;

const RULE: &str = "SEM-PARSE-001";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Yaml,
    Json,
}

impl Format {
    pub fn from_path(path: &str) -> Format {
        if path.to_ascii_lowercase().ends_with(".json") {
            Format::Json
        } else {
            Format::Yaml
        }
    }
}

/// A parsed document plus a JSON-pointer → source-location index.
#[derive(Debug, Clone)]
pub struct ParsedDocument {
    pub value: Value,
    pub locations: HashMap<String, SourceLocation>,
}

impl ParsedDocument {
    /// Location of the nearest ancestor of `pointer` that has one.
    pub fn locate(&self, pointer: &str) -> Option<SourceLocation> {
        let mut p = pointer.to_string();
        loop {
            if let Some(l) = self.locations.get(&p) {
                return Some(l.clone());
            }
            match p.rfind('/') {
                Some(i) => p.truncate(i),
                None => return self.locations.get("").cloned(),
            }
        }
    }
}

fn re(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).expect("static regex"))
}

static INT_RE: OnceLock<Regex> = OnceLock::new();
static FLOAT_RE: OnceLock<Regex> = OnceLock::new();
static TS_RE: OnceLock<Regex> = OnceLock::new();
static AMBIG_RE: OnceLock<Regex> = OnceLock::new();
static INTERP_RE: OnceLock<Regex> = OnceLock::new();

fn loc(m: &Marker) -> SourceLocation {
    SourceLocation {
        line: m.line(),
        column: m.col() + 1,
    }
}

fn escape_pointer(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

fn perr(
    code: &str,
    path: &str,
    location: Option<SourceLocation>,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::new(code, Stage::Parse, RULE, path, message).at(location)
}

enum Frame {
    Seq {
        path: String,
        items: Vec<Value>,
    },
    Map {
        path: String,
        map: Map<String, Value>,
        key: Option<(String, SourceLocation)>,
        key_locs: HashMap<String, SourceLocation>,
    },
}

struct Builder {
    stack: Vec<Frame>,
    root: Option<Value>,
    locations: HashMap<String, SourceLocation>,
    nodes: usize,
}

/// Resolve a plain (unquoted) scalar under the JSON-compatible profile.
fn resolve_plain(text: &str, path: &str, location: &SourceLocation) -> Result<Value, Diagnostic> {
    let at = Some(location.clone());
    if text == "null" {
        return Ok(Value::Null);
    }
    if text == "true" {
        return Ok(Value::Bool(true));
    }
    if text == "false" {
        return Ok(Value::Bool(false));
    }
    if re(&INT_RE, r"^-?(?:0|[1-9][0-9]*)$").is_match(text) {
        return text
            .parse::<i64>()
            .map(|i| Value::Number(Number::from(i)))
            .map_err(|_| {
                perr(
                    "PARSE_INTEGER_RANGE",
                    path,
                    at,
                    format!("Integer {text} is outside signed 64-bit range"),
                )
            });
    }
    if re(
        &FLOAT_RE,
        r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+(?:[eE][+-]?[0-9]+)?|[eE][+-]?[0-9]+)$",
    )
    .is_match(text)
    {
        let f: f64 = text.parse().map_err(|_| {
            perr(
                "PARSE_NUMBER",
                path,
                at.clone(),
                format!("Invalid number {text}"),
            )
        })?;
        if !f.is_finite() {
            return Err(perr(
                "PARSE_NON_FINITE",
                path,
                at,
                format!("Number {text} is not finite"),
            ));
        }
        if f == 0.0
            && text
                .trim_start_matches('-')
                .split(['e', 'E'])
                .next()
                .is_some_and(|m| m.chars().any(|c| c.is_ascii_digit() && c != '0'))
        {
            return Err(perr(
                "PARSE_NUMBER_RANGE",
                path,
                at,
                format!("Number {text} underflows binary64 to zero"),
            ));
        }
        return Number::from_f64(f)
            .map(Value::Number)
            .ok_or_else(|| perr("PARSE_NON_FINITE", path, None, "Non-finite number"));
    }
    if re(&TS_RE, r"^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}(?:(?:[Tt]|[ \t]+)[0-9]{1,2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]*)?(?:[ \t]*(?:Z|[-+][0-9]{1,2}(?::[0-9]{2})?))?)?$").is_match(text) {
        return Err(perr("PARSE_IMPLICIT_TIMESTAMP", path, at, format!("Timestamp/date scalar {text} must be quoted")));
    }
    if re(
        &AMBIG_RE,
        r"^(?:~|[Nn]ull|NULL|[Tt]rue|TRUE|[Ff]alse|FALSE|[Yy]es|YES|[Nn]o|NO|[Oo]n|ON|[Oo]ff|OFF|[yYnN]|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN)|[-+]?0[xXoObB][0-9a-fA-F_]+|[-+]?[0-9][0-9_]*(?:\.[0-9_]*)?(?:[eE][-+]?[0-9]+)?|[-+]?\.[0-9]+(?:[eE][-+]?[0-9]+)?|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+(?:\.[0-9_]*)?|<<)$",
    )
    .is_match(text)
    {
        return Err(perr(
            "PARSE_AMBIGUOUS_SCALAR",
            path,
            at,
            format!("Plain scalar {text:?} is ambiguous across YAML versions; quote it or use JSON-compatible true/false/null/number syntax"),
        ));
    }
    Ok(Value::String(text.to_string()))
}

fn check_interpolation(
    text: &str,
    path: &str,
    location: &SourceLocation,
) -> Result<(), Diagnostic> {
    if re(&INTERP_RE, r"\$\{[^}]*\}").is_match(text) {
        return Err(perr(
            "PARSE_INTERPOLATION",
            path,
            Some(location.clone()),
            "Environment/executable interpolation markers (${...}) are not permitted; documents are never expanded",
        ));
    }
    Ok(())
}

impl Builder {
    fn current_path(&self) -> String {
        match self.stack.last() {
            None => String::new(),
            Some(Frame::Seq { path, items }) => format!("{path}/{}", items.len()),
            Some(Frame::Map {
                path,
                key: Some((k, _)),
                ..
            }) => format!("{path}/{}", escape_pointer(k)),
            Some(Frame::Map { path, .. }) => path.clone(),
        }
    }

    fn expecting_key(&self) -> bool {
        matches!(self.stack.last(), Some(Frame::Map { key: None, .. }))
    }

    fn push_value(&mut self, value: Value) -> Result<(), Diagnostic> {
        match self.stack.last_mut() {
            None => {
                self.root = Some(value);
                Ok(())
            }
            Some(Frame::Seq { items, .. }) => {
                items.push(value);
                Ok(())
            }
            Some(Frame::Map { map, key, .. }) => {
                let (k, _) = key.take().expect("value without key");
                map.insert(k, value);
                Ok(())
            }
        }
    }

    fn count(&mut self, path: &str, location: &SourceLocation) -> Result<(), Diagnostic> {
        self.nodes += 1;
        if self.nodes > MAX_NODES {
            return Err(perr(
                "PARSE_LIMIT",
                path,
                Some(location.clone()),
                format!("Document exceeds {MAX_NODES} nodes"),
            ));
        }
        if self.stack.len() > MAX_DEPTH {
            return Err(perr(
                "PARSE_LIMIT",
                path,
                Some(location.clone()),
                format!("Document nesting exceeds {MAX_DEPTH}"),
            ));
        }
        Ok(())
    }

    fn event(&mut self, event: Event, mark: Marker) -> Result<(), Diagnostic> {
        let l = loc(&mark);
        match event {
            Event::Alias(_) => Err(perr(
                "PARSE_ALIAS",
                &self.current_path(),
                Some(l),
                "YAML aliases are not permitted",
            )),
            Event::Scalar(text, style, anchor, tag) => {
                let key_position = self.expecting_key();
                let parent_path = match self.stack.last() {
                    Some(Frame::Map { path, .. }) if key_position => {
                        format!("{path}/{}", escape_pointer(&text))
                    }
                    _ => self.current_path(),
                };
                if anchor != 0 {
                    return Err(perr(
                        "PARSE_ANCHOR",
                        &parent_path,
                        Some(l),
                        "YAML anchors are not permitted",
                    ));
                }
                if tag.is_some() {
                    return Err(perr(
                        "PARSE_TAG",
                        &parent_path,
                        Some(l),
                        "Explicit or custom YAML tags are not permitted",
                    ));
                }
                check_interpolation(&text, &parent_path, &l)?;
                if key_position {
                    if style == TScalarStyle::Plain {
                        if text == "<<" {
                            return Err(perr(
                                "PARSE_MERGE_KEY",
                                &parent_path,
                                Some(l),
                                "YAML merge keys are not permitted",
                            ));
                        }
                        match resolve_plain(&text, &parent_path, &l)? {
                            Value::String(_) => {}
                            other => {
                                return Err(perr(
                                    "PARSE_NON_STRING_KEY",
                                    &parent_path,
                                    Some(l),
                                    format!("Mapping keys must be strings; found {other}"),
                                ))
                            }
                        }
                    }
                    if let Some(Frame::Map {
                        map, key, key_locs, ..
                    }) = self.stack.last_mut()
                    {
                        if map.contains_key(&text) {
                            let first = key_locs
                                .get(&text)
                                .map(|f| {
                                    format!(
                                        " (first defined at line {}, column {})",
                                        f.line, f.column
                                    )
                                })
                                .unwrap_or_default();
                            return Err(perr(
                                "PARSE_DUPLICATE_KEY",
                                &parent_path,
                                Some(l),
                                format!("Duplicate mapping key {text:?}{first}"),
                            ));
                        }
                        key_locs.insert(text.clone(), l.clone());
                        *key = Some((text, l.clone()));
                    }
                    self.locations.insert(parent_path, l);
                    return Ok(());
                }
                self.count(&parent_path, &l)?;
                let value = if style == TScalarStyle::Plain {
                    resolve_plain(&text, &parent_path, &l)?
                } else {
                    Value::String(text)
                };
                self.locations.insert(parent_path, l);
                self.push_value(value)
            }
            Event::SequenceStart(anchor, tag) | Event::MappingStart(anchor, tag)
                if self.expecting_key() =>
            {
                let _ = (anchor, tag);
                Err(perr(
                    "PARSE_NON_STRING_KEY",
                    &self.current_path(),
                    Some(l),
                    "Mapping keys must be scalar strings",
                ))
            }
            Event::SequenceStart(anchor, tag) => {
                let path = self.current_path();
                if anchor != 0 {
                    return Err(perr(
                        "PARSE_ANCHOR",
                        &path,
                        Some(l),
                        "YAML anchors are not permitted",
                    ));
                }
                if tag.is_some() {
                    return Err(perr(
                        "PARSE_TAG",
                        &path,
                        Some(l),
                        "Explicit or custom YAML tags are not permitted",
                    ));
                }
                self.count(&path, &l)?;
                self.locations.insert(path.clone(), l);
                self.stack.push(Frame::Seq {
                    path,
                    items: Vec::new(),
                });
                Ok(())
            }
            Event::MappingStart(anchor, tag) => {
                let path = self.current_path();
                if anchor != 0 {
                    return Err(perr(
                        "PARSE_ANCHOR",
                        &path,
                        Some(l),
                        "YAML anchors are not permitted",
                    ));
                }
                if tag.is_some() {
                    return Err(perr(
                        "PARSE_TAG",
                        &path,
                        Some(l),
                        "Explicit or custom YAML tags are not permitted",
                    ));
                }
                self.count(&path, &l)?;
                self.locations.insert(path.clone(), l);
                self.stack.push(Frame::Map {
                    path,
                    map: Map::new(),
                    key: None,
                    key_locs: HashMap::new(),
                });
                Ok(())
            }
            Event::SequenceEnd => {
                if let Some(Frame::Seq { items, .. }) = self.stack.pop() {
                    self.push_value(Value::Array(items))
                } else {
                    Err(perr("PARSE_SYNTAX", "", Some(l), "Unbalanced sequence"))
                }
            }
            Event::MappingEnd => {
                if let Some(Frame::Map { map, .. }) = self.stack.pop() {
                    self.push_value(Value::Object(map))
                } else {
                    Err(perr("PARSE_SYNTAX", "", Some(l), "Unbalanced mapping"))
                }
            }
            _ => Ok(()),
        }
    }
}

/// Parse bytes under the restricted profile. Returns parse-stage diagnostics on failure.
pub fn parse_bytes(bytes: &[u8], format: Format) -> Result<ParsedDocument, Vec<Diagnostic>> {
    if bytes.len() > MAX_DOCUMENT_BYTES {
        return Err(vec![perr(
            "PARSE_LIMIT",
            "",
            None,
            format!("Document exceeds the {MAX_DOCUMENT_BYTES}-byte limit"),
        )]);
    }
    let text = std::str::from_utf8(bytes).map_err(|e| {
        vec![perr(
            "PARSE_ENCODING",
            "",
            None,
            format!("Document is not UTF-8: {e}"),
        )]
    })?;
    if text.starts_with('\u{feff}') {
        return Err(vec![perr(
            "PARSE_ENCODING",
            "",
            None,
            "Byte-order marks are not permitted",
        )]);
    }
    if text.lines().any(|l| l.starts_with('%')) {
        return Err(vec![perr(
            "PARSE_DIRECTIVE",
            "",
            None,
            "YAML directives (%YAML, %TAG) are not permitted",
        )]);
    }
    if text.contains('\0') {
        return Err(vec![perr(
            "PARSE_ENCODING",
            "",
            None,
            "NUL characters are not permitted",
        )]);
    }
    if format == Format::Json {
        if let Err(e) = serde_json::from_str::<serde::de::IgnoredAny>(text) {
            return Err(vec![perr(
                "PARSE_SYNTAX",
                "",
                Some(SourceLocation {
                    line: e.line(),
                    column: e.column(),
                }),
                format!("Invalid JSON: {e}"),
            )]);
        }
    }
    let mut parser = Parser::new_from_str(text);
    let mut builder = Builder {
        stack: Vec::new(),
        root: None,
        locations: HashMap::new(),
        nodes: 0,
    };
    let mut documents = 0usize;
    loop {
        let (event, mark) = match parser.next_token() {
            Ok(x) => x,
            Err(e) => {
                let m = e.marker();
                return Err(vec![perr(
                    "PARSE_SYNTAX",
                    &builder.current_path(),
                    Some(loc(m)),
                    format!("YAML syntax error: {}", e.info()),
                )]);
            }
        };
        match event {
            Event::StreamEnd => break,
            Event::DocumentStart => {
                documents += 1;
                if documents > 1 {
                    return Err(vec![perr(
                        "PARSE_MULTIPLE_DOCUMENTS",
                        "",
                        Some(loc(&mark)),
                        "Exactly one YAML document is permitted",
                    )]);
                }
            }
            other => builder.event(other, mark).map_err(|d| vec![d])?,
        }
    }
    match builder.root {
        Some(value @ Value::Object(_)) => Ok(ParsedDocument {
            value,
            locations: builder.locations,
        }),
        Some(_) => Err(vec![perr(
            "PARSE_ROOT",
            "",
            None,
            "The document root must be a mapping",
        )]),
        None => Err(vec![perr("PARSE_EMPTY", "", None, "The document is empty")]),
    }
}

pub fn parse_str(text: &str, format: Format) -> Result<ParsedDocument, Vec<Diagnostic>> {
    parse_bytes(text.as_bytes(), format)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code(text: &str) -> String {
        match parse_str(text, Format::Yaml) {
            Ok(_) => "OK".into(),
            Err(d) => d[0].code.clone(),
        }
    }

    #[test]
    fn accepts_json_compatible_yaml() {
        let doc = parse_str(
            "a: 1\nb: -2.5\nc: true\nd: null\ne: '2026-01-01T00:00:00Z'\nf: [x, \"y\"]\n",
            Format::Yaml,
        )
        .unwrap();
        assert_eq!(doc.value["a"], 1);
        assert_eq!(doc.value["b"], -2.5);
        assert_eq!(doc.value["e"], "2026-01-01T00:00:00Z");
        assert_eq!(doc.locations["/f/1"].line, 6);
    }

    #[test]
    fn rejects_forbidden_constructs() {
        assert_eq!(code("a: 1\na: 2\n"), "PARSE_DUPLICATE_KEY");
        assert_eq!(code("a: &x 1\nb: *x\n"), "PARSE_ANCHOR");
        assert_eq!(code("base: {a: 1}\nc:\n  <<: {a: 1}\n"), "PARSE_MERGE_KEY");
        assert_eq!(code("a: !!str 1\n"), "PARSE_TAG");
        assert_eq!(code("a: !custom x\n"), "PARSE_TAG");
        assert_eq!(code("a: 1\n---\nb: 2\n"), "PARSE_MULTIPLE_DOCUMENTS");
        assert_eq!(code("1: a\n"), "PARSE_NON_STRING_KEY");
        assert_eq!(code("true: a\n"), "PARSE_NON_STRING_KEY");
        assert_eq!(code("? [a]\n: b\n"), "PARSE_NON_STRING_KEY");
        assert_eq!(
            code("a: 2026-01-01T00:00:00Z\n"),
            "PARSE_IMPLICIT_TIMESTAMP"
        );
        assert_eq!(code("a: 2026-01-01\n"), "PARSE_IMPLICIT_TIMESTAMP");
        assert_eq!(code("a: .inf\n"), "PARSE_AMBIGUOUS_SCALAR");
        assert_eq!(code("a: .nan\n"), "PARSE_AMBIGUOUS_SCALAR");
        assert_eq!(code("a: yes\n"), "PARSE_AMBIGUOUS_SCALAR");
        assert_eq!(code("a: ~\n"), "PARSE_AMBIGUOUS_SCALAR");
        assert_eq!(code("a: 0x1F\n"), "PARSE_AMBIGUOUS_SCALAR");
        assert_eq!(code("a: 1e999\n"), "PARSE_NON_FINITE");
        assert_eq!(code("a: 9223372036854775808\n"), "PARSE_INTEGER_RANGE");
        assert_eq!(code("a: ${HOME}\n"), "PARSE_INTERPOLATION");
        assert_eq!(code("a: 'x ${HOME}'\n"), "PARSE_INTERPOLATION");
        assert_eq!(code("- 1\n"), "PARSE_ROOT");
        assert_eq!(code(""), "PARSE_EMPTY");
        assert_eq!(code("a: [1\n"), "PARSE_SYNTAX");
    }

    #[test]
    fn json_shares_rules() {
        assert!(parse_str(r#"{"a": 1, "b": [true, null, 1.5]}"#, Format::Json).is_ok());
        let e = parse_str(r#"{"a": 1, "a": 2}"#, Format::Json).unwrap_err();
        assert_eq!(e[0].code, "PARSE_DUPLICATE_KEY");
        let e = parse_str("{a: 1}", Format::Json).unwrap_err();
        assert_eq!(e[0].code, "PARSE_SYNTAX");
        let e = parse_str(r#"{"a": "2026-01-01T00:00:00Z"}"#, Format::Json).unwrap();
        assert_eq!(e.value["a"], "2026-01-01T00:00:00Z");
    }

    #[test]
    fn quoted_ambiguous_values_are_strings() {
        let d = parse_str("a: 'yes'\nb: \"null\"\n", Format::Yaml).unwrap();
        assert_eq!(d.value["a"], "yes");
        assert_eq!(d.value["b"], "null");
    }
}

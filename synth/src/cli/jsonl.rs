use crate::cli::export::ExportStrategy;
use crate::cli::import::ImportStrategy;
use crate::sampler::SamplerOutput;

use synth_core::graph::{json::synth_val_to_json, Value};
use synth_core::schema::{MergeStrategy, OptionalMergeStrategy};
use synth_core::Content;

use anyhow::{Context, Result};

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct JsonLinesFileExportStrategy {
    pub from_file: PathBuf,
    pub collection_field_name: String,
}

impl ExportStrategy for JsonLinesFileExportStrategy {
    fn export(&self, _namespace: Content, sample: SamplerOutput) -> Result<()> {
        let mut f = std::io::BufWriter::new(std::fs::File::create(&self.from_file)?);

        for val in json_lines_from_sampler_output(sample, &self.collection_field_name) {
            f.write_all((val.to_string() + "\n").as_bytes())?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct JsonLinesStdoutExportStrategy<W> {
    pub collection_field_name: String,
    pub writer: RefCell<W>,
}

impl<W: Write> ExportStrategy for JsonLinesStdoutExportStrategy<W> {
    fn export(&self, _namespace: Content, sample: SamplerOutput) -> Result<()> {
        // TODO: Warn user if the collection field name would overwrite an existing field in a collection.
        for line in json_lines_from_sampler_output(sample, &self.collection_field_name) {
            writeln!(self.writer.borrow_mut(), "{}", line).expect("failed to write jsonl line");
        }

        Ok(())
    }
}

pub struct JsonLinesFileImportStrategy {
    pub from_file: PathBuf,
    pub collection_field_name: String,
}

impl ImportStrategy for JsonLinesFileImportStrategy {
    fn import_namespace(&self) -> Result<Content> {
        import_json_lines(
            std::io::BufReader::new(std::fs::File::open(&self.from_file)?)
                .lines()
                .map(|line| serde_json::from_str(&line.unwrap()))
                .collect::<serde_json::Result<Vec<serde_json::Value>>>()?,
            &self.collection_field_name,
        )
    }
}

pub struct JsonLinesStdinImportStrategy {
    pub collection_field_name: String,
}

impl ImportStrategy for JsonLinesStdinImportStrategy {
    fn import_namespace(&self) -> Result<Content> {
        import_json_lines(
            std::io::stdin()
                .lock()
                .lines()
                .map(|line| serde_json::from_str(&line.unwrap()))
                .collect::<serde_json::Result<Vec<serde_json::Value>>>()?,
            &self.collection_field_name,
        )
    }
}

pub fn import_json_lines(
    json_lines: Vec<serde_json::Value>,
    collection_field_name: &str,
) -> Result<Content> {
    let mut collection_names_to_values: HashMap<Option<String>, Vec<serde_json::Value>> =
        HashMap::new();

    for mut value in json_lines {
        match value {
            serde_json::Value::Object(ref mut obj_content) => {
                let entry = {
                    if let Some(serde_json::Value::String(collection_name)) =
                        obj_content.remove(collection_field_name)
                    {
                        collection_names_to_values.entry(Some(collection_name))
                    } else {
                        collection_names_to_values.entry(None)
                    }
                }
                .or_default();

                entry.push(value);
            }
            _ => {
                collection_names_to_values
                    .entry(None)
                    .or_default()
                    .push(value);
            }
        }
    }

    collection_names_to_values
        .into_iter()
        .map(|(name, values)| {
            let name_or_default = name.unwrap_or_else(|| "collection".to_string());

            collection_from_values_jsonl(values)
                .and_then(|content| Ok((name_or_default.parse()?, content)))
                .with_context(|| anyhow!("While importing the collection '{}'", name_or_default))
        })
        .collect()
}

fn json_lines_from_sampler_output(
    output: SamplerOutput,
    collection_field_name: &str,
) -> Vec<serde_json::Value> {
    match output {
        SamplerOutput::Namespace(key_values) => {
            let mut jsonl = Vec::new();

            for (collection, value) in key_values {
                let json_values = synth_val_to_jsonl(value);

                let lines = json_values.into_iter().map(|val| {
                    // When no specific collection to generate data with is specified with --collection,
                    // each output line is labelled to indicate which collection in the namespace it was
                    // generated from.

                    match val {
                        serde_json::Value::Object(mut obj_values) => {
                            // If the collection generates an object, then the collection name is saved directly as
                            // a field of the object.

                            obj_values.insert(
                                collection_field_name.to_string(),
                                serde_json::Value::String(collection.clone()),
                            );

                            serde_json::Value::Object(obj_values)
                        }
                        non_obj_val => {
                            // If the collection does not generate a object, then the output value is an object with
                            // the collection specified as a field, and the generated non-object data as another.

                            serde_json::json!({
                                collection_field_name: collection,
                                "data": non_obj_val
                            })
                        }
                    }
                });

                jsonl.extend(lines);
            }

            jsonl
        }

        SamplerOutput::Collection(_, value) => synth_val_to_jsonl(value),
    }
}

fn synth_val_to_jsonl(val: Value) -> Vec<serde_json::Value> {
    match val {
        Value::Array(values) => values.into_iter().map(synth_val_to_json).collect(),
        non_array => vec![synth_val_to_json(non_array)],
    }
}

/// Create a collection (`Content`) from a set of Serde JSON values that were all generated originally from the same
/// collection.
fn collection_from_values_jsonl(values: Vec<serde_json::Value>) -> Result<Content> {
    let fst = values.first().unwrap_or(&serde_json::Value::Null);
    let mut as_content = Content::new_collection(fst.into());
    OptionalMergeStrategy.try_merge(&mut as_content, &serde_json::Value::Array(values))?;
    Ok(as_content)
}

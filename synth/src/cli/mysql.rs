use crate::cli::export::{create_and_insert_values, ExportStrategy};
use crate::cli::import::ImportStrategy;
use crate::cli::import_utils::build_namespace_import;
use crate::datasource::mysql_datasource::MySqlDataSource;
use crate::datasource::DataSource;
use crate::sampler::SamplerOutput;

use synth_core::Content;

use anyhow::Result;

#[derive(Clone, Debug)]
pub struct MySqlExportStrategy {
    pub uri_string: String,
}

impl ExportStrategy for MySqlExportStrategy {
    fn export(&self, _namespace: Content, sample: SamplerOutput) -> Result<()> {
        let datasource = MySqlDataSource::new(&self.uri_string)?;

        create_and_insert_values(sample, &datasource)
    }
}

#[derive(Clone, Debug)]
pub struct MySqlImportStrategy {
    pub uri_string: String,
}

impl ImportStrategy for MySqlImportStrategy {
    fn import_namespace(&self) -> Result<Content> {
        let datasource = MySqlDataSource::new(&self.uri_string)?;

        build_namespace_import(&datasource)
    }
}

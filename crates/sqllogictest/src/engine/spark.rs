// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_sqllogictest::{convert_batches, convert_schema_to_types, DFColumnType, DataFusion};
use indicatif::ProgressBar;
use spark_connect_rs::{SparkSession, SparkSessionBuilder};
use sqllogictest::{AsyncDB, DBOutput, Runner};
use toml::Table;
use crate::engine::Engine;
use crate::error::{Error, Result};
use toml::Table as TomlTable;

/// SparkSql engine implementation for sqllogictests.
#[derive(Clone)]
pub struct SparkSqlEngine {
    session: Arc<SparkSession>,
}

#[async_trait]
impl Engine for SparkSqlEngine {
    async fn new(config: TomlTable) -> Result<Self> {
        let url = config
            .get("url")
            .ok_or_else(|| anyhow!("url property doesn't exist for spark engine"))?
            .as_str()
            .ok_or_else(|| anyhow!("url property is not a string for spark engine"))?;

        let session = SparkSessionBuilder::remote(url)
            .app_name("SparkConnect")
            .build()
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(Self { session: Arc::new(session) })
    }

    async fn run_slt_file(&mut self, path: &Path) -> Result<()> {
        let engine = self.clone();
        let mut runner = Runner::new(move || {
            let engine = engine.clone();
            async move { Ok(engine) }
        });
        // runner.with_column_validator(strict_column_validator);
        Ok(runner.run_file_async(path).await.map_err(|e| anyhow!(e))?)
    }
}

#[async_trait]
impl AsyncDB for SparkSqlEngine {
    type Error = Error;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DFColumnType>> {
        let results = self
            .session
            .sql(sql)
            .await
            .map_err(|e| anyhow!(e))?
            .collect()
            .await
            .map_err(|e| anyhow!(e))?;
        let types = convert_schema_to_types(results.schema().fields());
        let rows = convert_batches(vec![results])
            .map_err(|e| anyhow!(e))?;

        if rows.is_empty() && types.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows { types, rows })
        }
    }

    async fn shutdown(&mut self) {
        println!("Spark Engine shutdown triggered");
    }

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        "SparkConnect"
    }

    /// [`DataFusionEngine`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

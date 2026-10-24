//! Placeholder for background downloads on Android; see
//! [`crate::background`].

use std::sync::Arc;

use crate::background::{BackgroundHandler, BackgroundJob, JobStatus};
use crate::{Error, Result};

fn unsupported() -> Error {
    Error::Internal("Background downloads are not implemented on Android yet".to_string())
}

pub(crate) struct Downloads;

impl Downloads {
    pub(crate) fn new(_identifier: &str, _handler: Arc<dyn BackgroundHandler>) -> Result<Self> {
        Err(unsupported())
    }

    pub(crate) fn enqueue(&self, _job: BackgroundJob) -> Result<()> {
        Err(unsupported())
    }

    pub(crate) fn cancel(&self, _job: &str) -> Result<()> {
        Err(unsupported())
    }

    pub(crate) async fn jobs(&self) -> Result<Vec<JobStatus>> {
        Err(unsupported())
    }
}

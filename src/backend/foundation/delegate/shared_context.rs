//! Shared context for task delegates

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arc_swap::ArcSwapOption;
use objc2::rc::Retained;
use objc2_foundation::{NSError, NSURL};

use super::GenericWaker;

/// Progress callback function type
pub type ProgressCallback = dyn Fn(u64, Option<u64>) + Send + Sync;

/// Download-specific context
pub struct DownloadContext {
    /// Destination file path
    pub destination_path: Option<std::path::PathBuf>,
    /// Temporary download location from NSURLSession
    pub download_location: ArcSwapOption<Retained<NSURL>>,
    /// Final location after copying the file
    pub final_location: ArcSwapOption<std::path::PathBuf>,
}

impl DownloadContext {
    /// Create a new download context
    pub fn new(destination_path: Option<std::path::PathBuf>) -> Self {
        Self {
            destination_path,
            download_location: ArcSwapOption::new(None),
            final_location: ArcSwapOption::new(None),
        }
    }

    /// Set the temporary download location
    pub fn set_download_location(&self, location: Retained<NSURL>) {
        self.download_location.store(Some(Arc::new(location)));
    }

    /// Set the final file location
    pub fn set_final_location(&self, path: std::path::PathBuf) {
        self.final_location.store(Some(Arc::new(path)));
    }
}

/// Shared context for task delegates
pub struct TaskSharedContext {
    /// Response data
    pub response: ArcSwapOption<Retained<objc2_foundation::NSURLResponse>>,
    /// Waker for async operations
    pub waker: GenericWaker,
    /// Whether the task is completed
    pub completed: AtomicBool,
    /// Any error that occurred
    pub error: ArcSwapOption<Retained<NSError>>,
    /// Bytes downloaded so far
    pub bytes_downloaded: AtomicU64,
    /// Total bytes expected (if known)
    pub total_bytes_expected: AtomicU64,
    /// Progress callback
    pub progress_callback: Option<Arc<ProgressCallback>>,
    /// Download-specific context (for download tasks)
    pub download_context: Option<Arc<DownloadContext>>,
}

impl TaskSharedContext {
    /// Create new shared context
    pub fn new() -> Self {
        Self {
            response: ArcSwapOption::new(None),
            waker: GenericWaker::new(),
            completed: AtomicBool::new(false),
            error: ArcSwapOption::new(None),
            bytes_downloaded: AtomicU64::new(0),
            total_bytes_expected: AtomicU64::new(0),
            progress_callback: None,
            download_context: None,
        }
    }

    /// Create new shared context with progress callback
    pub fn with_progress_callback(callback: Arc<ProgressCallback>) -> Self {
        Self {
            response: ArcSwapOption::new(None),
            waker: GenericWaker::new(),
            completed: AtomicBool::new(false),
            error: ArcSwapOption::new(None),
            bytes_downloaded: AtomicU64::new(0),
            total_bytes_expected: AtomicU64::new(0),
            progress_callback: Some(callback),
            download_context: None,
        }
    }

    /// Create new shared context for download with destination path
    pub fn with_download(
        destination_path: Option<std::path::PathBuf>,
        progress_callback: Option<Arc<ProgressCallback>>,
    ) -> Self {
        Self {
            response: ArcSwapOption::new(None),
            waker: GenericWaker::new(),
            completed: AtomicBool::new(false),
            error: ArcSwapOption::new(None),
            bytes_downloaded: AtomicU64::new(0),
            total_bytes_expected: AtomicU64::new(0),
            progress_callback,
            download_context: Some(Arc::new(DownloadContext::new(destination_path))),
        }
    }

    /// Check if the task is completed
    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }

    /// Mark the task as completed
    pub fn mark_completed(&self) {
        self.completed.store(true, Ordering::Release);
        self.waker.wake();
    }

    /// Set an error
    pub fn set_error(&self, error: Retained<NSError>) {
        self.error.store(Some(Arc::new(error)));
        self.mark_completed();
    }

    /// Set the total bytes expected for this download
    pub fn set_total_bytes_expected(&self, total: u64) {
        self.total_bytes_expected.store(total, Ordering::Release);
    }

    /// Update progress when new data is received
    pub fn update_progress(&self, additional_bytes: u64) {
        let new_downloaded = self
            .bytes_downloaded
            .fetch_add(additional_bytes, Ordering::AcqRel)
            + additional_bytes;

        if let Some(ref callback) = self.progress_callback {
            let total_expected = self.total_bytes_expected.load(Ordering::Acquire);
            let total = if total_expected > 0 {
                Some(total_expected)
            } else {
                None
            };
            callback(new_downloaded, total);
        }
    }

    /// Get current progress (downloaded, total_expected)
    pub fn get_progress(&self) -> (u64, Option<u64>) {
        let downloaded = self.bytes_downloaded.load(Ordering::Acquire);
        let total_expected = self.total_bytes_expected.load(Ordering::Acquire);
        let total = if total_expected > 0 {
            Some(total_expected)
        } else {
            None
        };
        (downloaded, total)
    }

    /// Set error from string message
    pub fn set_error_from_string(&self, message: String) {
        use objc2::runtime::{AnyObject, ProtocolObject};
        use objc2_foundation::{NSMutableDictionary, NSString};

        // Create a simple NSError for the message
        let user_info = unsafe {
            let dict = NSMutableDictionary::new();
            dict.setObject_forKey(
                &NSString::from_str(&message) as &AnyObject,
                ProtocolObject::from_ref(&*NSString::from_str("NSLocalizedDescription")),
            );
            dict
        };

        let error = unsafe {
            objc2_foundation::NSError::errorWithDomain_code_userInfo(
                &objc2_foundation::NSString::from_str("fraktError"),
                -1,
                Some(&user_info),
            )
        };
        self.error.store(Some(Arc::new(error)));
    }
}

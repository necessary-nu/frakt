//! Request body types

use bytes::Bytes;
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Request body content for HTTP requests.
///
/// This enum represents different types of request bodies that can be sent with HTTP requests.
/// It supports common content types including text, binary data, form data, JSON, and multipart forms.
///
/// # Examples
///
/// Creating different body types:
/// ```rust,no_run
/// use frakt::Body;
///
/// // Text body
/// let body = Body::text("Hello, World!");
///
/// // Binary body
/// let data = vec![1, 2, 3, 4];
/// let body = Body::bytes(data, "application/octet-stream");
///
/// // Form data
/// let body = Body::form(vec![
///     ("username", "john"),
///     ("password", "secret"),
/// ]);
/// ```
///
/// Using convenient `From` implementations:
/// ```rust,no_run
/// use frakt::{Client, Body};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new()?;
///
/// // `String` automatically converts to [`Body`]
/// let response = client
///     .post("https://api.example.com/data")?
///     .body("Hello, World!")
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub enum Body {
    /// Empty body
    Empty,

    /// Raw bytes with content type
    Bytes {
        /// The content
        content: Bytes,
        /// Content type
        content_type: String,
    },

    /// Form-encoded data
    Form {
        /// Form fields
        fields: Vec<(Cow<'static, str>, Cow<'static, str>)>,
    },

    /// Multipart form data
    Multipart {
        /// Multipart parts
        parts: Vec<MultipartPart>,
    },

    /// JSON data
    Json {
        /// JSON value
        value: serde_json::Value,
    },

    /// A file, read from disk as it is sent
    File {
        /// The open file
        file: FileBody,
        /// Content type
        content_type: String,
    },
}

/// Bodies carry passwords, tokens and private data, so only their shape is
/// shown.
impl std::fmt::Debug for Body {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Body::Empty => f.write_str("Empty"),
            Body::Bytes {
                content,
                content_type,
            } => f
                .debug_struct("Bytes")
                .field("len", &content.len())
                .field("content_type", content_type)
                .finish(),
            Body::Form { fields } => f
                .debug_struct("Form")
                .field("fields", &fields.len())
                .finish(),
            Body::Multipart { parts } => f
                .debug_struct("Multipart")
                .field("parts", &parts.len())
                .finish(),
            Body::Json { .. } => f.debug_struct("Json").finish_non_exhaustive(),
            Body::File { file, content_type } => f
                .debug_struct("File")
                .field("path", &file.path)
                .field("len", &file.len)
                .field("content_type", content_type)
                .finish(),
        }
    }
}

/// A file opened to be sent as a request body.
///
/// The file is opened once, when the body is created, and its length is
/// taken then. Exactly that many bytes are sent, read from disk a chunk at
/// a time as the request goes out, so memory use does not grow with the
/// file. If the file is shorter by the time it is read, the request fails;
/// bytes appended after it was opened are not sent.
///
/// The reqwest, WinHTTP and Cronet backends read through the handle opened
/// here, so the file may be renamed or unlinked meanwhile. The Foundation
/// backend hands NSURLSession the path, which must keep naming the same
/// file until the upload ends.
#[derive(Clone, Debug)]
pub struct FileBody {
    file: Arc<std::fs::File>,
    path: PathBuf,
    len: u64,
}

impl FileBody {
    /// Open `path` for sending. Fails if it cannot be opened for reading or
    /// is not a regular file.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = std::path::absolute(path)?;
        let file = std::fs::File::open(&path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{} is not a regular file", path.display()),
            ));
        }
        Ok(Self {
            file: Arc::new(file),
            path,
            len: metadata.len(),
        })
    }

    /// The number of bytes that will be sent.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the file was empty when it was opened.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The file's absolute path, as it was when the file was opened.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The open file's current metadata.
    #[cfg_attr(
        not(all(feature = "backend-foundation", target_vendor = "apple")),
        allow(dead_code)
    )]
    pub(crate) fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        self.file.metadata()
    }

    /// Read up to `max` bytes at `offset`, stopping at the length taken
    /// when the file was opened; empty at or past that length. Blocks on
    /// disk I/O. Positional, so concurrent readers and re-sends do not
    /// disturb each other.
    pub(crate) fn read_chunk(&self, offset: u64, max: usize) -> std::io::Result<Bytes> {
        let want = self.len.saturating_sub(offset).min(max as u64) as usize;
        let mut chunk = vec![0u8; want];
        let mut filled = 0;
        while filled < want {
            match read_at(&self.file, &mut chunk[filled..], offset + filled as u64) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!(
                            "{} was truncated while it was being sent",
                            self.path.display()
                        ),
                    ));
                }
                Ok(n) => filled += n,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(chunk.into())
    }

    /// The file's bytes in chunks of up to `chunk_size`, each read on the
    /// blocking pool when the stream is polled for it.
    #[cfg_attr(not(feature = "backend-reqwest"), allow(dead_code))]
    pub(crate) fn chunks(
        &self,
        chunk_size: usize,
    ) -> impl futures_util::Stream<Item = std::io::Result<Bytes>> + Send + 'static {
        let file = self.clone();
        futures_util::stream::try_unfold(0u64, move |offset| {
            let file = file.clone();
            async move {
                if offset >= file.len {
                    return Ok(None);
                }
                let chunk =
                    tokio::task::spawn_blocking(move || file.read_chunk(offset, chunk_size))
                        .await
                        .map_err(std::io::Error::other)??;
                let next = offset + chunk.len() as u64;
                Ok(Some((chunk, next)))
            }
        })
    }
}

#[cfg(unix)]
fn read_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(windows)]
fn read_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

/// A single part of multipart form data.
///
/// This struct represents one field in a multipart/form-data request body.
/// It can contain either text data or file uploads with optional content type and filename.
///
/// # Examples
///
/// ```rust,no_run
/// use frakt::MultipartPart;
///
/// // Text field
/// let text_part = MultipartPart::text("description", "A sample file");
///
/// // File field
/// let file_data = vec![1, 2, 3, 4];
/// let file_part = MultipartPart::file(
///     "upload",
///     file_data,
///     "data.bin",
///     Some("application/octet-stream".to_string())
/// );
/// ```
#[derive(Clone)]
pub struct MultipartPart {
    /// Field name
    pub name: String,
    /// Content
    pub content: Bytes,
    /// Content type
    pub content_type: Option<String>,
    /// Filename
    pub filename: Option<String>,
}

impl std::fmt::Debug for MultipartPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultipartPart")
            .field("name", &self.name)
            .field("len", &self.content.len())
            .field("content_type", &self.content_type)
            .field("filename", &self.filename)
            .finish()
    }
}

impl Body {
    /// Create an empty body.
    ///
    /// This creates a body with no content, typically used for GET, HEAD, and DELETE requests.
    pub fn empty() -> Self {
        Self::Empty
    }

    /// Create a body from raw bytes with a specific content type.
    ///
    /// # Arguments
    ///
    /// * `content` - The raw bytes to use as body content
    /// * `content_type` - The MIME type of the content
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::Body;
    ///
    /// let data = vec![0xFF, 0xD8, 0xFF, 0xE0]; // JPEG header
    /// let body = Body::bytes(data, "image/jpeg");
    /// ```
    pub fn bytes(content: impl Into<Bytes>, content_type: impl Into<String>) -> Self {
        Self::Bytes {
            content: content.into(),
            content_type: content_type.into(),
        }
    }

    /// Create a body from plain text.
    ///
    /// This sets the content type to `text/plain; charset=utf-8`.
    ///
    /// # Arguments
    ///
    /// * `content` - The text content to use as body
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::Body;
    ///
    /// let body = Body::text("Hello, World!");
    /// ```
    pub fn text(content: impl Into<String>) -> Self {
        Self::Bytes {
            content: content.into().into(),
            content_type: "text/plain; charset=utf-8".to_string(),
        }
    }

    /// Create a form-urlencoded body from field/value pairs.
    ///
    /// This creates a body with content type `application/x-www-form-urlencoded`
    /// and URL-encodes the provided field/value pairs.
    ///
    /// # Arguments
    ///
    /// * `fields` - A vector of (field_name, field_value) tuples
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::Body;
    ///
    /// let body = Body::form(vec![
    ///     ("username", "john_doe"),
    ///     ("password", "secret123"),
    ///     ("remember_me", "true"),
    /// ]);
    /// ```
    pub fn form(fields: Vec<(impl Into<Cow<'static, str>>, impl Into<Cow<'static, str>>)>) -> Self {
        Self::Form {
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }

    /// Create a JSON body from a serializable value.
    ///
    /// This serializes the provided value to JSON and sets the content type to
    /// `application/json`. This feature requires the "json" feature flag.
    ///
    /// # Arguments
    ///
    /// * `value` - Any value that implements `serde::Serialize`
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be serialized to JSON.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::Body;
    /// use serde_json::json;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let body = Body::json(json!({
    ///     "name": "John Doe",
    ///     "age": 30,
    ///     "active": true
    /// }))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn json(value: impl serde::Serialize) -> Result<Self, crate::Error> {
        Ok(Self::Json {
            value: serde_json::to_value(value).map_err(|e| crate::Error::Json(e.to_string()))?,
        })
    }

    /// Create a multipart form-data body.
    ///
    /// This creates a body with content type `multipart/form-data` containing
    /// the provided parts. This feature requires the "multipart" feature flag.
    ///
    /// # Arguments
    ///
    /// * `parts` - A vector of multipart parts to include
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::{Body, MultipartPart};
    ///
    /// let parts = vec![
    ///     MultipartPart::text("description", "Profile image"),
    ///     MultipartPart::file("image", vec![1, 2, 3, 4], "avatar.jpg", Some("image/jpeg".to_string())),
    /// ];
    /// let body = Body::multipart(parts);
    /// ```
    pub fn multipart(parts: Vec<MultipartPart>) -> Self {
        Self::Multipart { parts }
    }

    /// Create a body that sends a file.
    ///
    /// The file is opened here and read from disk as the request is sent,
    /// not loaded into memory; see [`FileBody`] for what happens if it
    /// changes meanwhile. If no content type is provided, it defaults to
    /// `application/octet-stream`.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to send
    /// * `content_type` - Optional MIME type for the file content
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or is not a regular
    /// file.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::Body;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let body = Body::from_file("image.jpg", Some("image/jpeg".to_string())).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn from_file<P: AsRef<std::path::Path>>(
        path: P,
        content_type: Option<String>,
    ) -> Result<Self, crate::Error> {
        let path = path.as_ref().to_path_buf();
        let file = tokio::task::spawn_blocking(move || FileBody::open(path))
            .await
            .map_err(|e| crate::Error::Internal(format!("Failed to open file: {}", e)))??;

        Ok(Self::File {
            file,
            content_type: content_type.unwrap_or_else(|| "application/octet-stream".to_string()),
        })
    }
}

// Convenience From implementations
impl From<String> for Body {
    fn from(content: String) -> Self {
        Self::text(content)
    }
}

impl From<&str> for Body {
    fn from(content: &str) -> Self {
        Self::text(content)
    }
}

impl From<Vec<u8>> for Body {
    fn from(content: Vec<u8>) -> Self {
        Self::bytes(content, "application/octet-stream")
    }
}

impl From<&[u8]> for Body {
    fn from(content: &[u8]) -> Self {
        Self::bytes(content.to_vec(), "application/octet-stream")
    }
}

impl From<Bytes> for Body {
    fn from(content: Bytes) -> Self {
        Self::bytes(content, "application/octet-stream")
    }
}

impl From<serde_json::Value> for Body {
    fn from(value: serde_json::Value) -> Self {
        Self::Json { value }
    }
}

impl MultipartPart {
    /// Create a text part
    pub fn text(name: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            content: content.into().into(),
            content_type: Some("text/plain; charset=utf-8".to_string()),
            filename: None,
        }
    }

    /// Create a file part
    pub fn file(
        name: impl Into<String>,
        content: impl Into<Bytes>,
        filename: impl Into<String>,
        content_type: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            content: content.into(),
            content_type,
            filename: Some(filename.into()),
        }
    }

    /// Create a file part from a file path. The file is read into memory
    /// here: multipart bodies are assembled in memory.
    pub async fn from_file<P: AsRef<std::path::Path>>(
        name: impl Into<String>,
        path: P,
        content_type: Option<String>,
    ) -> Result<Self, crate::Error> {
        let content = tokio::fs::read(&path).await?;
        let filename = path
            .as_ref()
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("file")
            .to_string();

        Ok(Self {
            name: name.into(),
            content: content.into(),
            content_type,
            filename: Some(filename),
        })
    }
}

//! The resumable download behind background downloads on the reqwest
//! backend, whether it runs in the calling task or in the helper process.
//!
//! The body goes to `<name>.frakt-partial` next to the destination, with
//! what a later attempt needs to resume it in `<name>.frakt-partial.meta`,
//! and is renamed over the destination only once it is complete. The partial
//! file is locked for as long as a download is writing to it.

use super::{ReqwestBody, body_items, decodable_coding, follow_redirects};
use crate::backend::types::{BodyAccounting, content_length, has_content_coding};
use crate::client::download::DownloadResponse;
use crate::{Error, Result};
use futures_util::StreamExt;
use http::{HeaderMap, StatusCode, header};
use serde::{Deserialize, Serialize};
use std::io::{Read, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use url::Url;

/// How many times a download is attempted when the transport fails.
const ATTEMPTS: u32 = 5;

/// The wait before the second attempt; it doubles for each one after.
const RETRY_DELAY: Duration = Duration::from_secs(2);

/// A sidecar larger than this was not written by us.
const SIDECAR_LIMIT: u64 = 64 * 1024;

/// What to download, and where to.
#[derive(Clone)]
pub(super) struct Download {
    pub url: Url,
    pub headers: HeaderMap,
    pub destination: PathBuf,
    pub error_for_status: bool,
    /// The longest to wait on the network for a response to start or for
    /// the next chunk of its body. There is no deadline for the whole
    /// download.
    pub read_timeout: Option<Duration>,
}

pub(super) enum Event<'a> {
    /// A response was accepted, and its body, if any, is about to be stored.
    /// Only the helper process passes this on, and not every target has one.
    #[allow(dead_code)]
    Started {
        status: StatusCode,
        headers: &'a HeaderMap,
        total: Option<u64>,
    },
    /// The length of the file so far, and its final length if known.
    Progress { downloaded: u64, total: Option<u64> },
}

/// The response that completed the download.
pub(super) struct Finished {
    pub status: StatusCode,
    pub headers: HeaderMap,
    /// The length of the published file.
    pub bytes: u64,
}

impl Finished {
    pub(super) fn into_response(self, file_path: PathBuf) -> DownloadResponse {
        DownloadResponse {
            file_path,
            bytes_downloaded: self.bytes,
            status: self.status,
            headers: self.headers,
        }
    }
}

/// Download to `download.destination`, resuming what an earlier attempt
/// left behind if the server confirms it is still the same representation.
/// Transport failures are retried; nothing is retried once a server has
/// answered with a status the caller does not accept.
pub(super) async fn run(
    client: &reqwest::Client,
    download: &Download,
    mut events: impl FnMut(Event<'_>) + Send,
) -> Result<Finished> {
    let mut transfer = Transfer::open(client, download)?;
    let mut attempt = 1;
    loop {
        match transfer.attempt(&mut events).await {
            Err(Error::Network { .. } | Error::Timeout) if attempt < ATTEMPTS => {
                tokio::time::sleep(RETRY_DELAY * 2u32.pow(attempt - 1)).await;
                attempt += 1;
            }
            outcome => return outcome,
        }
    }
}

/// What identifies the representation a partial file is part of.
#[derive(Clone, PartialEq)]
enum Validator {
    ETag(String),
    LastModified(String),
}

impl Validator {
    /// A strong ETag if there is one, as a weak one does not promise the
    /// same bytes, and otherwise the modification date.
    fn of(headers: &HeaderMap) -> Option<Self> {
        let text = |name: header::HeaderName| headers.get(name)?.to_str().ok().map(str::trim);
        if let Some(tag) = text(header::ETAG)
            && tag.len() >= 2
            && tag.starts_with('"')
            && tag.ends_with('"')
        {
            return Some(Validator::ETag(tag.to_string()));
        }
        let date = text(header::LAST_MODIFIED)?;
        (!date.is_empty()).then(|| Validator::LastModified(date.to_string()))
    }

    fn value(&self) -> &str {
        match self {
            Validator::ETag(value) | Validator::LastModified(value) => value,
        }
    }

    /// Whether a response names a different representation. One that sends
    /// no validator of this kind contradicts nothing.
    fn contradicted_by(&self, headers: &HeaderMap) -> bool {
        let name = match self {
            Validator::ETag(_) => header::ETAG,
            Validator::LastModified(_) => header::LAST_MODIFIED,
        };
        headers
            .get(name)
            .is_some_and(|sent| sent.as_bytes().trim_ascii() != self.value().as_bytes())
    }
}

/// The contents of `<name>.frakt-partial.meta`: which representation the
/// partial file holds the start of.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct Sidecar {
    url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_modified: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    total: Option<u64>,
}

impl Sidecar {
    fn new(url: &Url, validator: Option<Validator>, total: Option<u64>) -> Self {
        let (etag, last_modified) = match validator {
            Some(Validator::ETag(tag)) => (Some(tag), None),
            Some(Validator::LastModified(date)) => (None, Some(date)),
            None => (None, None),
        };
        Self {
            url: Self::source(url),
            etag,
            last_modified,
            total,
        }
    }

    /// The URL as the sidecar records it: without a user name or password,
    /// which have no business in a file left beside a download.
    fn source(url: &Url) -> String {
        let mut url = url.clone();
        let _ = url.set_username("");
        let _ = url.set_password(None);
        url.into()
    }

    fn validator(&self) -> Option<Validator> {
        match (&self.etag, &self.last_modified) {
            (Some(tag), _) => Some(Validator::ETag(tag.clone())),
            (None, Some(date)) => Some(Validator::LastModified(date.clone())),
            (None, None) => None,
        }
    }

    /// The sidecar at `path`, if it is a file of ours that parses.
    fn read(path: &Path, trusted: &Trusted) -> Option<Self> {
        let file = no_follow(std::fs::OpenOptions::new().read(true))
            .open(path)
            .ok()?;
        trusted.check(&file.metadata().ok()?).then_some(())?;
        let mut json = Vec::new();
        file.take(SIDECAR_LIMIT).read_to_end(&mut json).ok()?;
        serde_json::from_slice(&json).ok()
    }

    /// Replace the sidecar at `path` in one step, so that a reader never
    /// sees half of one.
    fn write(&self, path: &Path, directory: &Path) -> Result<()> {
        let failed = |e| Error::Io(format!("Failed to write {}: {}", path.display(), e));
        let mut file = tempfile::Builder::new()
            .prefix(".frakt-")
            .tempfile_in(directory)
            .map_err(failed)?;
        serde_json::to_writer(&mut file, self).map_err(|e| failed(e.into()))?;
        file.persist(path).map_err(|e| failed(e.error))?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum ContentRange {
    /// `bytes <start>-<end>/<total or *>`
    Part {
        start: u64,
        end: u64,
        total: Option<u64>,
    },
    /// `bytes */<total>`, as a 416 sends it.
    Unsatisfied { total: u64 },
}

fn content_range(headers: &HeaderMap) -> Option<ContentRange> {
    let value = headers.get(header::CONTENT_RANGE)?.to_str().ok()?;
    let (range, total) = value.trim().strip_prefix("bytes")?.split_once('/')?;
    let (range, total) = (range.trim(), total.trim());
    if range == "*" {
        return Some(ContentRange::Unsatisfied {
            total: total.parse().ok()?,
        });
    }
    let (start, end) = range.split_once('-')?;
    let (start, end): (u64, u64) = (start.trim().parse().ok()?, end.trim().parse().ok()?);
    let total = match total {
        "*" => None,
        total => Some(total.parse::<u64>().ok()?),
    };
    (start <= end && total.is_none_or(|total| end < total)).then_some(ContentRange::Part {
        start,
        end,
        total,
    })
}

/// The request to make for what is on disk.
enum Ask {
    /// For the whole body; the partial file is empty.
    Fresh,
    /// For the bytes after the partial file's, if it is still the start of
    /// what the server has.
    Resume(Validator),
    /// For the whole body unless the partial file, which is as long as the
    /// body was said to be, is still what the server has.
    Revalidate(Validator),
}

/// What to do with a response.
enum Plan {
    /// Write its body at `from`, which is either zero or the length of the
    /// partial file, and expect the file to end up `expected` bytes long.
    Store {
        from: u64,
        expected: Option<u64>,
        state: Sidecar,
    },
    /// The partial file is the whole body already.
    Complete,
    /// It has no body and says nothing about the partial file.
    Nothing,
    /// What is on disk is not the start of what the server has now.
    Restart,
}

/// A download in progress: the locked partial file and what is known of it.
struct Transfer<'a> {
    client: &'a reqwest::Client,
    download: &'a Download,
    /// The caller's own headers ask for a range or make the request
    /// conditional, so the body is stored as received and never resumed.
    verbatim: bool,
    directory: PathBuf,
    partial: PathBuf,
    sidecar: PathBuf,
    /// Open on `partial`, holding its lock.
    file: tokio::fs::File,
    /// The permissions of a file created in `directory` with nothing asked
    /// for.
    #[cfg(unix)]
    default_mode: u32,
    /// What the sidecar on disk says of the partial file, if anything that
    /// applies to this download.
    state: Option<Sidecar>,
    /// The length of the partial file.
    have: u64,
    published: bool,
}

impl<'a> Transfer<'a> {
    fn open(client: &'a reqwest::Client, download: &'a Download) -> Result<Self> {
        let destination = &download.destination;
        let name = destination
            .file_name()
            .ok_or_else(|| Error::Io(format!("{} has no file name", destination.display())))?;
        if std::fs::symlink_metadata(destination).is_ok_and(|existing| existing.is_dir()) {
            return Err(Error::Io(format!(
                "{} is a directory",
                destination.display()
            )));
        }
        let sibling = |suffix: &str| {
            let mut name = name.to_os_string();
            name.push(suffix);
            destination.with_file_name(name)
        };
        let directory = match destination.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        };
        let (partial, sidecar) = (sibling(".frakt-partial"), sibling(".frakt-partial.meta"));

        let file = open_locked(&partial)?;
        let unusable = |e| Error::Io(format!("Cannot use {}: {}", partial.display(), e));
        let trusted = Trusted::new(&directory).map_err(unusable)?;
        let metadata = file.metadata().map_err(unusable)?;
        if !trusted.check(&metadata) {
            return Err(Error::Io(format!(
                "{} is not a regular file of this user's",
                partial.display()
            )));
        }

        let conditions = [
            header::RANGE,
            header::IF_RANGE,
            header::IF_MATCH,
            header::IF_NONE_MATCH,
            header::IF_MODIFIED_SINCE,
            header::IF_UNMODIFIED_SINCE,
        ];
        Ok(Self {
            client,
            download,
            verbatim: conditions
                .iter()
                .any(|name| download.headers.contains_key(name)),
            state: Sidecar::read(&sidecar, &trusted)
                .filter(|state| state.url == Sidecar::source(&download.url)),
            have: metadata.len(),
            file: tokio::fs::File::from_std(file),
            #[cfg(unix)]
            default_mode: trusted.default_mode,
            directory,
            partial,
            sidecar,
            published: false,
        })
    }

    /// One request, or two if the first shows that what is on disk has to
    /// be thrown away, and the storing of its body.
    async fn attempt(&mut self, events: &mut (impl FnMut(Event<'_>) + Send)) -> Result<Finished> {
        let mut ask = self.ask();
        loop {
            if matches!(ask, Ask::Fresh) {
                self.reset().await?;
            }
            let response = self.send(&ask).await?;
            let (mut status, headers) = (response.status(), response.headers().clone());
            match self.plan(&ask, status, &headers)? {
                Plan::Restart => {
                    ask = Ask::Fresh;
                    continue;
                }
                Plan::Nothing => {
                    return Ok(Finished {
                        status,
                        headers,
                        bytes: 0,
                    });
                }
                Plan::Complete => {
                    // The caller asked for the file and has all of it: that
                    // the last exchange was a 304 or a 416 is how this
                    // download was resumed, not what became of it.
                    status = StatusCode::OK;
                    let total = Some(self.have);
                    events(Event::Started {
                        status,
                        headers: &headers,
                        total,
                    });
                    events(Event::Progress {
                        downloaded: self.have,
                        total,
                    });
                }
                Plan::Store {
                    from,
                    expected,
                    state,
                } => {
                    // Likewise for the 206 that brought the rest of a file.
                    if from > 0 {
                        status = StatusCode::OK;
                    }
                    events(Event::Started {
                        status,
                        headers: &headers,
                        total: expected,
                    });
                    self.store(response, from, expected, state, events).await?;
                }
            }
            self.publish().await?;
            return Ok(Finished {
                status,
                headers,
                bytes: self.have,
            });
        }
    }

    fn ask(&self) -> Ask {
        let Some(state) = &self.state else {
            return Ask::Fresh;
        };
        match state.validator() {
            Some(validator) if !self.verbatim && self.have > 0 => match state.total {
                Some(total) if self.have > total => Ask::Fresh,
                Some(total) if self.have == total => Ask::Revalidate(validator),
                _ => Ask::Resume(validator),
            },
            _ => Ask::Fresh,
        }
    }

    async fn send(&self, ask: &Ask) -> Result<reqwest::Response> {
        let mut headers = self.download.headers.clone();
        // A byte range of an encoded body would not be a range of the file.
        if !headers.contains_key(header::ACCEPT_ENCODING) {
            headers.insert(
                header::ACCEPT_ENCODING,
                http::HeaderValue::from_static("identity"),
            );
        }
        // Validators come from header values that were read as text, and
        // the length is digits.
        let value =
            |text: &str| http::HeaderValue::from_str(text).map_err(|_| Error::InvalidHeader);
        match ask {
            Ask::Fresh => {}
            Ask::Resume(validator) => {
                headers.insert(header::RANGE, value(&format!("bytes={}-", self.have))?);
                headers.insert(header::IF_RANGE, value(validator.value())?);
            }
            Ask::Revalidate(Validator::ETag(tag)) => {
                headers.insert(header::IF_NONE_MATCH, value(tag)?);
            }
            Ask::Revalidate(Validator::LastModified(date)) => {
                headers.insert(header::IF_MODIFIED_SINCE, value(date)?);
            }
        }

        follow_redirects(
            http::Method::GET,
            self.download.url.clone(),
            headers,
            self.download.read_timeout,
            |method, url, headers, _| Ok(self.client.request(method, url).headers(headers.clone())),
        )
        .await
    }

    fn plan(&self, ask: &Ask, status: StatusCode, headers: &HeaderMap) -> Result<Plan> {
        let refused = self.download.error_for_status && status.as_u16() >= 400;
        Ok(match (ask, status) {
            (Ask::Resume(validator), StatusCode::PARTIAL_CONTENT) => {
                match self.continuation(validator, headers) {
                    Some((total, expected)) => Plan::Store {
                        from: self.have,
                        expected: Some(expected),
                        state: Sidecar::new(&self.download.url, Some(validator.clone()), total),
                    },
                    None => Plan::Restart,
                }
            }
            (Ask::Resume(validator), StatusCode::RANGE_NOT_SATISFIABLE) => {
                let whole = content_range(headers)
                    == Some(ContentRange::Unsatisfied { total: self.have })
                    && !validator.contradicted_by(headers);
                if whole { Plan::Complete } else { Plan::Restart }
            }
            (Ask::Revalidate(_), StatusCode::NOT_MODIFIED) => Plan::Complete,
            // Neither is an answer to what was asked.
            (Ask::Resume(_), StatusCode::NOT_MODIFIED)
            | (Ask::Revalidate(_), StatusCode::PARTIAL_CONTENT) => Plan::Restart,
            _ if refused => return Err(Error::HttpStatus(status)),
            (_, StatusCode::NOT_MODIFIED) => Plan::Nothing,
            (_, StatusCode::PARTIAL_CONTENT) if !self.verbatim => {
                return Err(Error::Network {
                    code: -1,
                    message: "The server sent part of the body without being asked for a range"
                        .to_string(),
                });
            }
            _ => {
                // Only a whole, unencoded body can be carried on from later.
                let resumable =
                    status == StatusCode::OK && !self.verbatim && !has_content_coding(headers);
                let expected = match decodable_coding(headers) {
                    Some(_) => None,
                    None => content_length(headers),
                };
                Plan::Store {
                    from: 0,
                    expected,
                    state: Sidecar::new(
                        &self.download.url,
                        Validator::of(headers).filter(|_| resumable),
                        expected.filter(|_| resumable),
                    ),
                }
            }
        })
    }

    /// For a 206 that carries on exactly where the partial file ends, with
    /// the same representation: the total it and the sidecar agree on, and
    /// how long the file will be once its body is appended.
    fn continuation(
        &self,
        validator: &Validator,
        headers: &HeaderMap,
    ) -> Option<(Option<u64>, u64)> {
        let Some(ContentRange::Part { start, end, total }) = content_range(headers) else {
            return None;
        };
        let known = self.state.as_ref().and_then(|state| state.total);
        let fits = start == self.have
            && (total.is_none() || known.is_none() || total == known)
            && content_length(headers).is_none_or(|length| length == end - start + 1)
            && !has_content_coding(headers)
            && !validator.contradicted_by(headers);
        let total = total.or(known);
        fits.then(|| (total, total.unwrap_or(end + 1)))
    }

    /// Write a response's body at `from`. The order of the steps matters:
    /// whatever is on disk when this stops, the sidecar never describes
    /// bytes it does not belong to.
    async fn store(
        &mut self,
        response: reqwest::Response,
        from: u64,
        expected: Option<u64>,
        state: Sidecar,
        events: &mut (impl FnMut(Event<'_>) + Send),
    ) -> Result<()> {
        if from == 0 {
            self.reset().await?;
        }
        if self.state.as_ref() != Some(&state) {
            state.write(&self.sidecar, &self.directory)?;
            self.state = Some(state);
        }
        let failed = |e| Error::Io(format!("Failed to write {}: {}", self.partial.display(), e));
        self.file
            .seek(SeekFrom::Start(from))
            .await
            .map_err(failed)?;

        let coding = decodable_coding(response.headers());
        let accounting = BodyAccounting::new(coding.is_some(), content_length(response.headers()));
        let mut body = ReqwestBody {
            inner: body_items(response.bytes_stream(), coding, accounting),
            read_timeout: self.download.read_timeout,
            idle: None,
            done: false,
        };
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            // The write itself happens on another thread, and flushing waits
            // for it: the bytes counted are on disk, and a download dropped
            // while it waits for the network has nothing still being
            // written, so the lock is released with it.
            self.file.write_all(&chunk).await.map_err(failed)?;
            self.file.flush().await.map_err(failed)?;
            self.have += chunk.len() as u64;
            events(Event::Progress {
                downloaded: self.have,
                total: expected,
            });
        }

        if let Some(expected) = expected
            && self.have != expected
        {
            let (have, partial) = (self.have, self.partial.display().to_string());
            if have > expected {
                self.reset().await?;
            }
            return Err(Error::Network {
                code: -1,
                message: format!(
                    "The body for {} ended at {} of {} bytes",
                    partial, have, expected
                ),
            });
        }
        // The last progress reported is always the whole file.
        if expected.is_none() || self.have == from {
            events(Event::Progress {
                downloaded: self.have,
                total: Some(self.have),
            });
        }
        Ok(())
    }

    /// Empty the partial file and forget what was known of it. The file is
    /// emptied first: a sidecar without its bytes is harmless, bytes with
    /// the wrong sidecar are not.
    async fn reset(&mut self) -> Result<()> {
        let failed = |e| Error::Io(format!("Failed to reset {}: {}", self.partial.display(), e));
        if self.have > 0 {
            self.file.set_len(0).await.map_err(failed)?;
            self.have = 0;
        }
        self.state = None;
        match std::fs::remove_file(&self.sidecar) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(failed(e)),
            _ => Ok(()),
        }
    }

    /// Move the partial file, now the whole body, to the destination.
    async fn publish(&mut self) -> Result<()> {
        let destination = &self.download.destination;
        let failed = |e| Error::Io(format!("Failed to write {}: {}", destination.display(), e));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // The partial file is private; the published one is what the
            // caller would have got by creating the destination itself.
            let mode = match std::fs::symlink_metadata(destination) {
                Ok(existing) if existing.is_file() => existing.permissions().mode() & 0o777,
                _ => self.default_mode,
            };
            self.file
                .set_permissions(std::fs::Permissions::from_mode(mode))
                .await
                .map_err(failed)?;
        }
        self.file.sync_data().await.map_err(failed)?;
        std::fs::rename(&self.partial, destination).map_err(failed)?;
        self.published = true;
        let _ = std::fs::remove_file(&self.sidecar);
        Ok(())
    }
}

impl Drop for Transfer<'_> {
    /// Leave behind only what a later download can carry on from. This also
    /// runs when the download is cancelled by dropping its future.
    fn drop(&mut self) {
        let resumable = self.have > 0
            && self
                .state
                .as_ref()
                .is_some_and(|state| state.validator().is_some());
        if !self.published && !resumable {
            let _ = std::fs::remove_file(&self.sidecar);
            let _ = std::fs::remove_file(&self.partial);
        }
    }
}

/// Which files in the destination's directory can be taken for ones an
/// earlier download of ours left there, rather than ones planted for us to
/// find.
struct Trusted {
    /// Who owns a file created there, which on a mount that maps owners is
    /// not the user we run as.
    #[cfg(unix)]
    owner: u32,
    /// The permissions of a file created there with nothing asked for.
    #[cfg(unix)]
    default_mode: u32,
}

impl Trusted {
    #[cfg(unix)]
    fn new(directory: &Path) -> std::io::Result<Self> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        // Creating a file is the only way to learn the umask without
        // changing it for the whole process.
        let probe = tempfile::Builder::new()
            .prefix(".frakt-")
            .permissions(std::fs::Permissions::from_mode(0o666))
            .tempfile_in(directory)?;
        let metadata = probe.as_file().metadata()?;
        Ok(Self {
            owner: metadata.uid(),
            default_mode: metadata.permissions().mode() & 0o777,
        })
    }

    #[cfg(not(unix))]
    fn new(_directory: &Path) -> std::io::Result<Self> {
        Ok(Self {})
    }

    /// Whether an open file is a regular file of ours with no other name,
    /// so that writing to it changes nothing else.
    fn check(&self, metadata: &std::fs::Metadata) -> bool {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            metadata.is_file() && metadata.uid() == self.owner && metadata.nlink() == 1
        }
        #[cfg(not(unix))]
        {
            metadata.is_file()
        }
    }
}

/// Make `options` open the file at a path itself, never what a symbolic
/// link there points to.
fn no_follow(options: &mut std::fs::OpenOptions) -> &mut std::fs::OpenOptions {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_FLAG_OPEN_REPARSE_POINT: a link is opened as itself, and is
        // then not a regular file.
        options.custom_flags(0x0020_0000);
    }
    options
}

fn busy(path: &Path) -> Error {
    Error::Io(format!(
        "A download to this destination is already in progress ({} is locked)",
        path.display()
    ))
}

fn open_failed(path: &Path, error: std::io::Error) -> Error {
    if std::fs::symlink_metadata(path).is_ok_and(|existing| existing.is_symlink()) {
        return Error::Io(format!("{} is a symbolic link", path.display()));
    }
    Error::Io(format!("Failed to open {}: {}", path.display(), error))
}

/// Open the partial file, creating it if need be, and take the lock that
/// keeps every other download away from it until the file is closed.
#[cfg(unix)]
fn open_locked(path: &Path) -> Result<std::fs::File> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    // The download that held the lock may have published or removed the
    // file between our opening it and our locking it, and then the lock is
    // on a file that is no longer the partial file. Open it again.
    for _ in 0..16 {
        let file = no_follow(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .mode(0o600),
        )
        .open(path)
        .map_err(|e| open_failed(path, e))?;

        while unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = std::io::Error::last_os_error();
            match error.kind() {
                std::io::ErrorKind::Interrupted => {}
                std::io::ErrorKind::WouldBlock => return Err(busy(path)),
                _ => {
                    return Err(Error::Io(format!(
                        "Failed to lock {}: {}",
                        path.display(),
                        error
                    )));
                }
            }
        }

        let held = file.metadata().map_err(|e| open_failed(path, e))?;
        if std::fs::symlink_metadata(path)
            .is_ok_and(|named| named.dev() == held.dev() && named.ino() == held.ino())
        {
            return Ok(file);
        }
    }
    Err(busy(path))
}

/// Open the partial file, creating it if need be, so that no other download
/// can open it until the file is closed.
#[cfg(windows)]
fn open_locked(path: &Path) -> Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_SHARE_DELETE: u32 = 0x4;
    const ERROR_SHARING_VIOLATION: i32 = 32;

    // Sharing nothing but deletion keeps every other open out while still
    // letting the open file be renamed to the destination.
    no_follow(
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .share_mode(FILE_SHARE_DELETE),
    )
    .open(path)
    .map_err(|e| match e.raw_os_error() {
        Some(ERROR_SHARING_VIOLATION) => busy(path),
        _ => open_failed(path, e),
    })
}

#[cfg(not(any(unix, windows)))]
fn open_locked(path: &Path) -> Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .map_err(|e| open_failed(path, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(value: &str) -> Option<ContentRange> {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_RANGE, value.parse().unwrap());
        content_range(&headers)
    }

    #[test]
    fn content_range_is_parsed_strictly() {
        let part = |start, end, total| Some(ContentRange::Part { start, end, total });
        assert_eq!(range("bytes 0-9/10"), part(0, 9, Some(10)));
        assert_eq!(range("bytes 5-9/*"), part(5, 9, None));
        assert_eq!(range(" bytes  5-9 / 10 "), part(5, 9, Some(10)));
        assert_eq!(
            range("bytes */10"),
            Some(ContentRange::Unsatisfied { total: 10 })
        );
        for malformed in [
            "bytes 9-5/10",
            "bytes 0-10/10",
            "bytes 0-9",
            "bytes */*",
            "bytes -9/10",
            "bytes 0-/10",
            "items 0-9/10",
            "bytes 0-9/ten",
            "",
        ] {
            assert_eq!(range(malformed), None, "{malformed:?}");
        }
    }

    #[test]
    fn only_a_strong_etag_is_a_validator() {
        let headers = |pairs: &[(header::HeaderName, &str)]| {
            pairs
                .iter()
                .map(|(name, value)| (name.clone(), value.parse().unwrap()))
                .collect::<HeaderMap>()
        };
        let date = "Wed, 21 Oct 2015 07:28:00 GMT";
        let strong = headers(&[(header::ETAG, "\"v1\""), (header::LAST_MODIFIED, date)]);
        assert!(Validator::of(&strong) == Some(Validator::ETag("\"v1\"".into())));
        let weak = headers(&[(header::ETAG, "W/\"v1\""), (header::LAST_MODIFIED, date)]);
        assert!(Validator::of(&weak) == Some(Validator::LastModified(date.into())));
        assert!(Validator::of(&headers(&[(header::ETAG, "W/\"v1\"")])).is_none());
        assert!(Validator::of(&headers(&[(header::ETAG, "v1")])).is_none());

        let etag = Validator::ETag("\"v1\"".into());
        assert!(!etag.contradicted_by(&strong));
        assert!(etag.contradicted_by(&weak));
        assert!(etag.contradicted_by(&headers(&[(header::ETAG, "\"v2\"")])));
        assert!(!etag.contradicted_by(&headers(&[(header::LAST_MODIFIED, date)])));
    }
}

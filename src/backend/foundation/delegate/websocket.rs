//! WebSocket delegate for NSURLSessionWebSocketTask

use crate::Error;
use block2::DynBlock;
use objc2::rc::Retained;
use objc2::{AllocAnyThread, DefinedClass, define_class, msg_send};
use objc2_foundation::{
    NSData, NSError, NSObject, NSObjectProtocol, NSString, NSURLAuthenticationChallenge,
    NSURLAuthenticationMethodServerTrust, NSURLCredential, NSURLSession,
    NSURLSessionAuthChallengeDisposition, NSURLSessionDelegate, NSURLSessionTask,
    NSURLSessionTaskDelegate, NSURLSessionWebSocketCloseCode, NSURLSessionWebSocketDelegate,
    NSURLSessionWebSocketTask,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::oneshot;

/// Connection state for WebSocket
#[derive(Debug, Clone)]
pub enum ConnectionState {
    /// Currently trying to connect
    Connecting,
    /// Connection successfully opened
    Open(Option<String>), // protocol name
    /// Connection closed
    Closed(Option<i64>, Option<String>), // close code and reason
    /// Connection error occurred
    Error(String),
}

/// Instance variables for the WebSocketDelegate
pub struct WebSocketDelegateIvars {
    /// Sender to signal when connection is opened
    pub connection_sender: std::sync::Mutex<Option<oneshot::Sender<Result<String, Error>>>>,
    /// Current connection state
    pub connection_state: Arc<std::sync::Mutex<ConnectionState>>,
    /// Whether connection has been established
    pub is_connected: Arc<AtomicBool>,
}

define_class!(
    #[allow(missing_docs)]
    #[unsafe(super = NSObject)]
    #[name = "fraktWebSocketDelegate"]
    #[ivars = WebSocketDelegateIvars]
    pub struct WebSocketDelegate;

    unsafe impl NSObjectProtocol for WebSocketDelegate {}

    unsafe impl NSURLSessionDelegate for WebSocketDelegate {}

    unsafe impl NSURLSessionTaskDelegate for WebSocketDelegate {
        #[unsafe(method(URLSession:task:didCompleteWithError:))]
        fn URLSession_task_didCompleteWithError(
            &self,
            _session: &NSURLSession,
            _task: &NSURLSessionTask,
            error: Option<&NSError>,
        ) {
            tracing::debug!(
                "WebSocketDelegate::URLSession_task_didCompleteWithError - Called with error: {:?}",
                error.is_some()
            );

            if let Some(error) = error {
                let error_msg = objc2::rc::autoreleasepool(|pool| unsafe {
                    error.localizedDescription().to_str(pool).to_string()
                });

                tracing::debug!(
                    "WebSocketDelegate::URLSession_task_didCompleteWithError - Error message: {}",
                    error_msg
                );
                tracing::debug!(
                    "WebSocketDelegate::URLSession_task_didCompleteWithError - Error domain: {:?}",
                    error.domain()
                );
                tracing::debug!(
                    "WebSocketDelegate::URLSession_task_didCompleteWithError - Error code: {}",
                    error.code()
                );

                // Update connection state
                let mut state = self.ivars().connection_state.lock().unwrap();
                *state = ConnectionState::Error(error_msg.clone());

                // Signal connection failure if sender is still available
                if let Ok(mut sender_guard) = self.ivars().connection_sender.lock() {
                    if let Some(sender) = sender_guard.take() {
                        tracing::debug!(
                            "WebSocketDelegate::URLSession_task_didCompleteWithError - Sending error via channel"
                        );
                        let _ = sender.send(Err(Error::from_ns_error(error)));
                    } else {
                        tracing::debug!(
                            "WebSocketDelegate::URLSession_task_didCompleteWithError - No sender available"
                        );
                    }
                }

                self.ivars().is_connected.store(false, Ordering::Relaxed);
            } else {
                tracing::debug!(
                    "WebSocketDelegate::URLSession_task_didCompleteWithError - Task completed successfully"
                );
            }
        }

        #[unsafe(method(URLSession:task:didReceiveChallenge:completionHandler:))]
        fn URLSession_task_didReceiveChallenge_completionHandler(
            &self,
            _session: &NSURLSession,
            _task: &NSURLSessionTask,
            challenge: &NSURLAuthenticationChallenge,
            completion_handler: &DynBlock<
                dyn Fn(NSURLSessionAuthChallengeDisposition, *mut NSURLCredential),
            >,
        ) {
            tracing::debug!(
                "WebSocketDelegate::URLSession_task_didReceiveChallenge_completionHandler - Called"
            );

            unsafe {
                let protection_space = challenge.protectionSpace();
                let auth_method = protection_space.authenticationMethod();

                tracing::debug!(
                    "WebSocketDelegate::URLSession_task_didReceiveChallenge_completionHandler - Auth method: {:?}",
                    auth_method
                );

                // Check if this is a server trust challenge
                if auth_method.isEqualToString(NSURLAuthenticationMethodServerTrust) {
                    tracing::debug!(
                        "WebSocketDelegate::URLSession_task_didReceiveChallenge_completionHandler - Server trust challenge"
                    );

                    // For now, we'll use the default handling which respects the session configuration
                    // In the future, this could be expanded to allow custom certificate validation
                    completion_handler.call((
                        NSURLSessionAuthChallengeDisposition::PerformDefaultHandling,
                        std::ptr::null_mut(),
                    ));
                    tracing::debug!(
                        "WebSocketDelegate::URLSession_task_didReceiveChallenge_completionHandler - Used default handling for server trust"
                    );
                } else {
                    tracing::debug!(
                        "WebSocketDelegate::URLSession_task_didReceiveChallenge_completionHandler - Other challenge type"
                    );

                    // For other types of challenges (HTTP auth, etc.), use default handling
                    completion_handler.call((
                        NSURLSessionAuthChallengeDisposition::PerformDefaultHandling,
                        std::ptr::null_mut(),
                    ));
                    tracing::debug!(
                        "WebSocketDelegate::URLSession_task_didReceiveChallenge_completionHandler - Used default handling for other challenge"
                    );
                }
            }
        }
    }

    unsafe impl NSURLSessionWebSocketDelegate for WebSocketDelegate {
        #[unsafe(method(URLSession:webSocketTask:didOpenWithProtocol:))]
        fn URLSession_webSocketTask_didOpenWithProtocol(
            &self,
            _session: &NSURLSession,
            _task: &NSURLSessionWebSocketTask,
            protocol: Option<&NSString>,
        ) {
            tracing::debug!(
                "WebSocketDelegate::URLSession_webSocketTask_didOpenWithProtocol - WebSocket connection opened!"
            );

            let protocol_string = protocol
                .map(|p| objc2::rc::autoreleasepool(|pool| unsafe { p.to_str(pool).to_string() }));

            tracing::debug!(
                "WebSocketDelegate::URLSession_webSocketTask_didOpenWithProtocol - Protocol: {:?}",
                protocol_string
            );

            // Update connection state
            let mut state = self.ivars().connection_state.lock().unwrap();
            *state = ConnectionState::Open(protocol_string.clone());

            // Signal successful connection
            if let Ok(mut sender_guard) = self.ivars().connection_sender.lock() {
                if let Some(sender) = sender_guard.take() {
                    tracing::debug!(
                        "WebSocketDelegate::URLSession_webSocketTask_didOpenWithProtocol - Sending success via channel"
                    );
                    let _ = sender.send(Ok(protocol_string.unwrap_or_default()));
                } else {
                    tracing::debug!(
                        "WebSocketDelegate::URLSession_webSocketTask_didOpenWithProtocol - No sender available"
                    );
                }
            }

            self.ivars().is_connected.store(true, Ordering::Relaxed);
            tracing::debug!(
                "WebSocketDelegate::URLSession_webSocketTask_didOpenWithProtocol - Set connected to true"
            );
        }

        #[unsafe(method(URLSession:webSocketTask:didCloseWithCode:reason:))]
        fn URLSession_webSocketTask_didCloseWithCode_reason(
            &self,
            _session: &NSURLSession,
            _task: &NSURLSessionWebSocketTask,
            close_code: NSURLSessionWebSocketCloseCode,
            reason: Option<&NSData>,
        ) {
            tracing::debug!(
                "WebSocketDelegate::URLSession_webSocketTask_didCloseWithCode_reason - WebSocket closed with code: {}",
                close_code.0
            );

            let reason_string =
                reason.map(|data| String::from_utf8_lossy(&data.to_vec()).to_string());

            tracing::debug!(
                "WebSocketDelegate::URLSession_webSocketTask_didCloseWithCode_reason - Reason: {:?}",
                reason_string
            );

            // Update connection state
            let mut state = self.ivars().connection_state.lock().unwrap();
            *state = ConnectionState::Closed(Some(close_code.0 as i64), reason_string);

            self.ivars().is_connected.store(false, Ordering::Relaxed);
            tracing::debug!(
                "WebSocketDelegate::URLSession_webSocketTask_didCloseWithCode_reason - Set connected to false"
            );
        }
    }
);

impl WebSocketDelegate {
    /// Create a new WebSocket delegate
    pub fn new() -> Retained<Self> {
        let (sender, _) = oneshot::channel();

        let delegate = Self::alloc().set_ivars(WebSocketDelegateIvars {
            connection_sender: std::sync::Mutex::new(Some(sender)),
            connection_state: Arc::new(std::sync::Mutex::new(ConnectionState::Connecting)),
            is_connected: Arc::new(AtomicBool::new(false)),
        });

        unsafe { msg_send![super(delegate), init] }
    }

    /// Create a new WebSocket delegate with a connection channel
    pub fn new_with_channel() -> (Retained<Self>, oneshot::Receiver<Result<String, Error>>) {
        let (sender, receiver) = oneshot::channel();

        let delegate = Self::alloc().set_ivars(WebSocketDelegateIvars {
            connection_sender: std::sync::Mutex::new(Some(sender)),
            connection_state: Arc::new(std::sync::Mutex::new(ConnectionState::Connecting)),
            is_connected: Arc::new(AtomicBool::new(false)),
        });

        let delegate = unsafe { msg_send![super(delegate), init] };
        (delegate, receiver)
    }

    /// Check if the WebSocket is connected
    pub fn is_connected(&self) -> bool {
        self.ivars().is_connected.load(Ordering::Relaxed)
    }

    /// Get the current connection state
    pub fn connection_state(&self) -> ConnectionState {
        self.ivars().connection_state.lock().unwrap().clone()
    }
}

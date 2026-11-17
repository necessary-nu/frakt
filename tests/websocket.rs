//! WebSocket message size limits against a local server, once per backend
//! that has WebSockets and can run on this host.
//!
//! The server is tokio-tungstenite's, which the reqwest backend already
//! depends on, so the suite needs `backend-reqwest` whichever backend is
//! under test. Each connection's first message tells the server what to
//! send back, so the client side decides the sizes and the fragmentation.

#![cfg(all(feature = "backend-reqwest", not(target_os = "android")))]

use frakt::{BackendType, Client, DEFAULT_MAXIMUM_MESSAGE_SIZE, Error, Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::{
    self,
    protocol::{
        WebSocketConfig,
        frame::{
            Frame,
            coding::{Data, OpCode},
        },
    },
};

const LIMIT: usize = 64 * 1024;

fn binary_byte(index: usize) -> u8 {
    (index % 251) as u8
}

/// `len` bytes of text in which every character takes two bytes, so that
/// fragments of an odd size split characters.
fn text_payload(len: usize) -> String {
    assert_eq!(len % 2, 0);
    "ø".repeat(len / 2)
}

/// Serve WebSocket connections on 127.0.0.1 and return the `ws://` URL.
///
/// A text message `<kind> <len> <fragment>` is answered with a message of
/// `len` bytes, `kind` being `text` or `binary`, sent as one frame if
/// `fragment` is 0 and otherwise in frames of at most `fragment` bytes.
/// Anything else is echoed.
async fn serve() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            tokio::spawn(async move {
                let config = WebSocketConfig::default()
                    .max_message_size(None)
                    .max_frame_size(None);
                let Ok(mut socket) =
                    tokio_tungstenite::accept_async_with_config(stream, Some(config)).await
                else {
                    return;
                };
                while let Some(Ok(message)) = socket.next().await {
                    let replies = match &message {
                        tungstenite::Message::Text(text) => match command(text.as_str()) {
                            Some(frames) => frames,
                            None => vec![message],
                        },
                        tungstenite::Message::Binary(_) => vec![message],
                        _ => continue,
                    };
                    for reply in replies {
                        if socket.send(reply).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    url
}

/// The frames a command asks for, or `None` if the text is not a command.
fn command(text: &str) -> Option<Vec<tungstenite::Message>> {
    let mut words = text.split(' ');
    let kind = words.next()?;
    let len: usize = words.next()?.parse().ok()?;
    let fragment: usize = words.next()?.parse().ok()?;
    let (payload, opcode) = match kind {
        "text" => (text_payload(len).into_bytes(), Data::Text),
        "binary" => ((0..len).map(binary_byte).collect(), Data::Binary),
        _ => return None,
    };
    if fragment == 0 {
        return Some(vec![tungstenite::Message::Frame(Frame::message(
            payload,
            OpCode::Data(opcode),
            true,
        ))]);
    }

    let pieces: Vec<&[u8]> = payload.chunks(fragment).collect();
    let last = pieces.len() - 1;
    Some(
        pieces
            .iter()
            .enumerate()
            .map(|(index, piece)| {
                let opcode = if index == 0 { opcode } else { Data::Continue };
                tungstenite::Message::Frame(Frame::message(
                    piece.to_vec(),
                    OpCode::Data(opcode),
                    index == last,
                ))
            })
            .collect(),
    )
}

async fn connect(
    backend: BackendType,
    url: &str,
    limit: Option<usize>,
) -> frakt::Result<WebSocket> {
    let client = Client::builder().backend(backend).build().unwrap();
    let mut builder = client.websocket();
    if let Some(limit) = limit {
        builder = builder.maximum_message_size(limit);
    }
    builder.connect(url).await
}

/// Ask the server for a message and check that it arrives whole.
async fn expect_message(socket: &mut WebSocket, kind: &str, len: usize, fragment: usize) {
    socket
        .send(Message::text(format!("{} {} {}", kind, len, fragment)))
        .await
        .unwrap();
    let case = format!("{} of {} bytes in fragments of {}", kind, len, fragment);
    match socket.receive().await {
        Ok(Message::Text(text)) => {
            assert_eq!(kind, "text", "{}", case);
            assert!(text == text_payload(len), "{}: wrong text", case);
        }
        Ok(Message::Binary(data)) => {
            assert_eq!(kind, "binary", "{}", case);
            assert_eq!(data.len(), len, "{}", case);
            assert!(
                data.iter().enumerate().all(|(i, &b)| b == binary_byte(i)),
                "{}: wrong bytes",
                case
            );
        }
        Err(error) => panic!("{}: {:?}", case, error),
    }
}

async fn messages_up_to_the_limit_arrive_whole(backend: BackendType) {
    let url = serve().await;
    let mut socket = connect(backend, &url, Some(LIMIT)).await.unwrap();
    assert_eq!(socket.maximum_message_size(), LIMIT);

    for kind in ["binary", "text"] {
        // One below, and exactly, the limit; in one frame, in fragments
        // larger than any backend's read buffer, and in fragments that
        // split two-byte characters.
        for len in [LIMIT - 2, LIMIT] {
            for fragment in [0, 16 * 1024, 4097] {
                expect_message(&mut socket, kind, len, fragment).await;
            }
        }
        expect_message(&mut socket, kind, 0, 0).await;
    }

    // Empty messages go out too, and come back from the echo.
    socket.send(Message::binary(Vec::new())).await.unwrap();
    assert!(matches!(socket.receive().await, Ok(Message::Binary(data)) if data.is_empty()));
    socket.send(Message::text("")).await.unwrap();
    assert!(matches!(socket.receive().await, Ok(Message::Text(text)) if text.is_empty()));
}

async fn a_message_over_the_limit_is_refused(backend: BackendType) {
    let url = serve().await;
    for (kind, fragment) in [("binary", 0), ("text", 0), ("binary", 4096), ("text", 4096)] {
        let mut socket = connect(backend, &url, Some(LIMIT)).await.unwrap();
        // Proves the connection works before the oversized message.
        expect_message(&mut socket, kind, 1024, 0).await;

        socket
            .send(Message::text(format!(
                "{} {} {}",
                kind,
                LIMIT + 2,
                fragment
            )))
            .await
            .unwrap();
        let case = format!("{:?}: {} in fragments of {}", backend, kind, fragment);
        match socket.receive().await {
            Err(Error::WebSocketMessageTooLarge { limit }) => assert_eq!(limit, LIMIT, "{}", case),
            other => panic!(
                "{}: expected WebSocketMessageTooLarge, got {:?}",
                case,
                other.map(|message| message.len())
            ),
        }
        assert!(
            matches!(socket.receive().await, Err(Error::WebSocketClosed)),
            "{}: still open",
            case
        );
    }
}

async fn sending_over_the_limit_fails_and_keeps_the_connection(backend: BackendType) {
    let url = serve().await;
    let mut socket = connect(backend, &url, Some(LIMIT)).await.unwrap();

    for message in [
        Message::binary(vec![0u8; LIMIT + 1]),
        Message::text("x".repeat(LIMIT + 1)),
    ] {
        match socket.send(message).await {
            Err(Error::WebSocketMessageTooLarge { limit }) => assert_eq!(limit, LIMIT),
            other => panic!("{:?}: {:?}", backend, other),
        }
    }

    // At the limit goes out, and comes back, on the same connection.
    let payload: Vec<u8> = (0..LIMIT).map(binary_byte).collect();
    socket.send(Message::binary(payload.clone())).await.unwrap();
    match socket.receive().await.unwrap() {
        Message::Binary(echoed) => assert!(echoed == payload),
        Message::Text(_) => panic!("{:?}: a binary message came back as text", backend),
    }
}

async fn the_limit_is_fixed_before_connecting(backend: BackendType) {
    let url = serve().await;

    assert!(
        connect(backend, &url, Some(0)).await.is_err(),
        "{:?}: a limit of zero was accepted",
        backend
    );

    let mut socket = connect(backend, &url, None).await.unwrap();
    assert_eq!(socket.maximum_message_size(), DEFAULT_MAXIMUM_MESSAGE_SIZE);
    expect_message(&mut socket, "binary", DEFAULT_MAXIMUM_MESSAGE_SIZE, 0).await;
    socket
        .send(Message::text(format!(
            "binary {} 0",
            DEFAULT_MAXIMUM_MESSAGE_SIZE + 1
        )))
        .await
        .unwrap();
    assert!(
        matches!(
            socket.receive().await,
            Err(Error::WebSocketMessageTooLarge { .. })
        ),
        "{:?}: the default limit was not applied",
        backend
    );
}

macro_rules! backend_suite {
    ($module:ident, $backend:expr) => {
        mod $module {
            use super::BackendType;

            backend_suite!(@tests $backend;
                messages_up_to_the_limit_arrive_whole,
                a_message_over_the_limit_is_refused,
                sending_over_the_limit_fails_and_keeps_the_connection,
                the_limit_is_fixed_before_connecting,
            );
        }
    };
    (@tests $backend:expr; $($test:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn $test() {
                super::$test($backend).await
            }
        )*
    };
}

#[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
backend_suite!(foundation, BackendType::Foundation);

backend_suite!(reqwest, BackendType::Reqwest);

#[cfg(all(feature = "backend-winhttp", windows))]
backend_suite!(winhttp, BackendType::Windows);

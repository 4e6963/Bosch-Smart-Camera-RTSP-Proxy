//! RTSP/1.0 message framing over a TCP stream.
//!
//! A single RTSP/TCP connection carries a mix of text control messages
//! (`OPTIONS ... RTSP/1.0`) and interleaved binary media frames (RFC 2326 §10.12,
//! `$` channel length payload). This module reads either kind from an async stream
//! and writes text responses / interleaved frames back.

use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::{ProxyError, Result};

/// One item read from an RTSP connection.
#[derive(Debug)]
pub enum Frame {
    /// A text control request.
    Request(Request),
    /// An interleaved binary media frame (`channel`, `payload`).
    Interleaved { channel: u8, data: Vec<u8> },
}

/// A parsed RTSP request.
#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub uri: String,
    /// Lower-cased header name -> value.
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(&name.to_ascii_lowercase()).map(|s| s.as_str())
    }

    /// The mandatory `CSeq` echoed on every response.
    pub fn cseq(&self) -> &str {
        self.header("cseq").unwrap_or("0")
    }

}

/// Parse a request from its header block (everything up to, not including, the blank
/// line) plus an already-read body.
pub fn parse_request(head: &str, body: Vec<u8>) -> Result<Request> {
    let mut lines = head.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| ProxyError::Rtsp("empty request".into()))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| ProxyError::Rtsp("missing method".into()))?
        .to_string();
    let uri = parts
        .next()
        .ok_or_else(|| ProxyError::Rtsp("missing request URI".into()))?
        .to_string();

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }

    Ok(Request {
        method,
        uri,
        headers,
        body,
    })
}

/// Read the next [`Frame`] from an async reader, or `None` at clean EOF.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Frame>> {
    let first = match reader.read_u8().await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    if first == b'$' {
        // Interleaved frame: channel (1) + length (2, big-endian) + payload.
        let channel = reader.read_u8().await?;
        let len = reader.read_u16().await? as usize;
        let mut data = vec![0u8; len];
        reader.read_exact(&mut data).await?;
        return Ok(Some(Frame::Interleaved { channel, data }));
    }

    // Text request: accumulate until the CRLF CRLF header terminator.
    let mut buf = vec![first];
    loop {
        let b = reader.read_u8().await?;
        buf.push(b);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
        if buf.len() > 64 * 1024 {
            return Err(ProxyError::Rtsp("request header too large".into()));
        }
    }

    let head = String::from_utf8_lossy(&buf[..buf.len() - 4]).to_string();
    // Read the body if Content-Length says so (used by ANNOUNCE with SDP).
    let content_length = head
        .split("\r\n")
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            if k.trim().eq_ignore_ascii_case("content-length") {
                v.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);

    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).await?;
    }

    Ok(Some(Frame::Request(parse_request(&head, body)?)))
}

/// Build an RTSP response as bytes.
pub struct Response {
    pub status: u16,
    pub reason: &'static str,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn ok(cseq: &str) -> Self {
        Self {
            status: 200,
            reason: "OK",
            headers: vec![("CSeq".into(), cseq.to_string())],
            body: Vec::new(),
        }
    }

    pub fn error(cseq: &str, status: u16, reason: &'static str) -> Self {
        Self {
            status,
            reason,
            headers: vec![("CSeq".into(), cseq.to_string())],
            body: Vec::new(),
        }
    }

    pub fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.push((name.to_string(), value.into()));
        self
    }

    pub fn with_body(mut self, content_type: &str, body: Vec<u8>) -> Self {
        self.headers
            .push(("Content-Type".into(), content_type.to_string()));
        self.headers
            .push(("Content-Length".into(), body.len().to_string()));
        self.body = body;
        self
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = format!("RTSP/1.0 {} {}\r\n", self.status, self.reason);
        for (k, v) in &self.headers {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
        out.push_str("\r\n");
        let mut bytes = out.into_bytes();
        bytes.extend_from_slice(&self.body);
        bytes
    }
}

/// Encode an interleaved media frame (`$` channel length payload) into bytes.
///
/// Returns `None` if the payload exceeds the 16-bit interleaved length field.
pub fn encode_interleaved(channel: u8, data: &[u8]) -> Option<Vec<u8>> {
    let len = u16::try_from(data.len()).ok()?;
    let mut out = Vec::with_capacity(4 + data.len());
    out.extend_from_slice(&[b'$', channel]);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(data);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn reads_text_request_with_body() {
        let raw = "ANNOUNCE rtsp://h/cam1 RTSP/1.0\r\nCSeq: 2\r\nContent-Length: 5\r\n\r\nhello";
        let mut cur = Cursor::new(raw.as_bytes().to_vec());
        let frame = read_frame(&mut cur).await.unwrap().unwrap();
        match frame {
            Frame::Request(req) => {
                assert_eq!(req.method, "ANNOUNCE");
                assert_eq!(req.cseq(), "2");
                assert_eq!(req.body, b"hello");
            }
            _ => panic!("expected request"),
        }
    }

    #[tokio::test]
    async fn reads_interleaved_frame() {
        let mut raw = vec![b'$', 0u8, 0u8, 3u8];
        raw.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        let mut cur = Cursor::new(raw);
        let frame = read_frame(&mut cur).await.unwrap().unwrap();
        match frame {
            Frame::Interleaved { channel, data } => {
                assert_eq!(channel, 0);
                assert_eq!(data, vec![0xAA, 0xBB, 0xCC]);
            }
            _ => panic!("expected interleaved"),
        }
    }

    #[tokio::test]
    async fn clean_eof_returns_none() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        assert!(read_frame(&mut cur).await.unwrap().is_none());
    }

    #[test]
    fn response_encodes_status_and_headers() {
        let resp = Response::ok("7").header("Public", "OPTIONS, DESCRIBE");
        let text = String::from_utf8(resp.encode()).unwrap();
        assert!(text.starts_with("RTSP/1.0 200 OK\r\n"));
        assert!(text.contains("CSeq: 7\r\n"));
        assert!(text.contains("Public: OPTIONS, DESCRIBE\r\n"));
    }
}

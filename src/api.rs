use std::error::Error;
use std::str::FromStr;

use scru128::Scru128Id;

use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;

// needed to convert async-std AsyncRead/Write to a tokio AsyncRead/Write
use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};

use http_body_util::StreamBody;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;

use crate::store::{ReadOptions, Content, Store};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type HTTPResult = Result<Response<BoxBody<Bytes, BoxError>>, BoxError>;

enum Routes {
    Root,
    Get(Scru128Id),
    CasGet(ssri::Integrity),
    NotFound,
}

fn match_route(path: &str) -> Routes {
    match path {
        "/" => Routes::Root,
        p if p.starts_with("/cas/") => {
            if let Some(hash) = p.strip_prefix("/cas/") {
                eprintln!("hash: '{}'", &hash);
                if let Ok(integrity) = ssri::Integrity::from_str(hash) {
                    return Routes::CasGet(integrity);
                }
            }
            Routes::NotFound
        }
        p => {
            if let Ok(id) = Scru128Id::from_str(p.trim_start_matches('/')) {
                Routes::Get(id)
            } else {
                Routes::NotFound
            }
        }
    }
}

async fn get(store: Store, req: Request<hyper::body::Incoming>) -> HTTPResult {
    eprintln!("uri: {:?}", req.uri());
    match match_route(req.uri().path()) {
        Routes::Root => {
            let options = match ReadOptions::from_query(req.uri().query()) {
                Ok(opts) => opts,
                Err(err) => return response_400(err.to_string()),
            };

            let rx = store.read(options).await;
            let stream = ReceiverStream::new(rx);
            let stream = stream.map(|frame| {
                eprintln!("streaming");
                let mut encoded = serde_json::to_vec(&frame).unwrap();
                encoded.push(b'\n');
                Ok(hyper::body::Frame::data(bytes::Bytes::from(encoded)))
            });
            let body = StreamBody::new(stream).boxed();
            Ok(Response::new(body))
        }

        Routes::CasGet(hash) => {
            let reader = store.cas_reader(hash).await?;
            // convert reader from async-std -> tokio
            let reader = reader.compat();
            let stream = ReaderStream::new(reader);

            let stream = stream.map(|frame| {
                let frame = frame.unwrap();
                Ok(hyper::body::Frame::data(frame))
            });

            let body = StreamBody::new(stream).boxed();
            Ok(Response::new(body))
        }

        Routes::Get(id) => {
            if let Some(frame) = store.get(&id) {
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/json")
                    .body(full(serde_json::to_string(&frame).unwrap()))?)
            } else {
                response_404()
            }
        }

        Routes::NotFound => response_404(),
    }
}

async fn post(mut store: Store, req: Request<hyper::body::Incoming>) -> HTTPResult {
    let (parts, mut body) = req.into_parts();
    eprintln!("parts: {:?}", &parts);
    eprintln!("uri: {:?}", &parts.uri.path());
    eprintln!("headers: {:?}", &parts.headers);

    // is the request body empty, full or streaming?
    eprintln!("BODY: {:?}", &body);

    let writer = store.cas_writer().await?;

    // convert writer from async-std -> tokio
    let mut writer = writer.compat_write();
    while let Some(frame) = body.frame().await {
        let data = frame?.into_data().unwrap();
        writer.write_all(&data).await?;
    }
    // get the original writer back
    let writer = writer.into_inner();

    let meta = match parts
        .headers
        .get("xs-meta")
        .map(|x| x.to_str())
        .transpose()
        .unwrap()
        .map(|s| serde_json::from_str(s).map_err(|_| format!("xs-meta isn't valid JSON: {}", s)))
        .transpose()
    {
        Ok(meta) => meta,
        Err(e) => return response_400(e.to_string()),
    };

    eprintln!("meta: {:?}", &meta);

    let hash = writer.commit().await?;
    let frame = store
        .append(
            parts.uri.path().trim_start_matches('/'),
            meta,
            Content::Full(hash),
        )
        .await;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(full(serde_json::to_string(&frame).unwrap()))?)
}

async fn handle(store: Store, req: Request<hyper::body::Incoming>) -> HTTPResult {
    eprintln!("\n\nreq: {:?}", &req);
    match *req.method() {
        Method::GET => get(store, req).await,
        Method::POST => post(store, req).await,
        _ => response_404(),
    }
}

pub async fn serve(store: Store) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("starting api: {:?}", &store.path);
    let listener = UnixListener::bind(store.path.join("sock")).unwrap();
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let store = store.clone();
        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service_fn(move |req| handle(store.clone(), req)))
                .await
            {
                // Match against the error kind to selectively ignore `NotConnected` errors
                if let Some(std::io::ErrorKind::NotConnected) = err.source().and_then(|source| {
                    source
                        .downcast_ref::<std::io::Error>()
                        .map(|io_err| io_err.kind())
                }) {
                    // Silently ignore the NotConnected error
                } else {
                    // Handle or log other errors
                    println!("Error serving connection: {:?}", err);
                }
            }
        });
    }
}

fn response_404() -> HTTPResult {
    Ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(empty())?)
}

fn response_400(message: String) -> HTTPResult {
    let body = full(message);
    Ok(Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(body)?)
}

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, BoxError> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

fn empty() -> BoxBody<Bytes, BoxError> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

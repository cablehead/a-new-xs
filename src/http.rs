use std::collections::HashMap;
use std::error::Error;

use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

// needed to convert async-std AsyncWrite to a tokio AsyncWrite
use tokio_util::compat::FuturesAsyncWriteCompatExt;

use http_body_util::StreamBody;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;

use path_tree::PathTree;

use crate::store::Store;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type HTTPResult = Result<Response<BoxBody<Bytes, BoxError>>, BoxError>;

enum Routes {
    Root,
    CasGet,
}

async fn get(store: Store, req: Request<hyper::body::Incoming>) -> HTTPResult {
    let mut tree = PathTree::new();
    let _ = tree.insert("/", Routes::Root);
    let _ = tree.insert("/cas/:hash+", Routes::CasGet);

    eprintln!("path: {:?}", req.uri().path());

    match tree.find(req.uri().path()) {
        Some((h, p)) => match h {
            Routes::Root => {
                let rx = store.subscribe().await;
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

            Routes::CasGet => Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(full("let's go"))?),
        },
        None => response_404(),
    }
}

async fn post(mut store: Store, req: Request<hyper::body::Incoming>) -> HTTPResult {
    let (parts, mut body) = req.into_parts();
    eprintln!("parts: {:?}", &parts);
    eprintln!("uri: {:?}", &parts.uri.path());

    let writer = store.cas_open().await?;

    // convert writer from async-std -> tokio
    let mut writer = writer.compat_write();
    while let Some(frame) = body.frame().await {
        let data = frame?.into_data().unwrap();
        writer.write_all(&data).await?;
    }
    // get the original writer back
    let writer = writer.into_inner();

    let hash = writer.commit().await?;
    let frame = store.append(parts.uri.path().to_string(), Some(hash)).await;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(full(serde_json::to_string(&frame).unwrap()))?)
}

async fn handle(store: Store, req: Request<hyper::body::Incoming>) -> HTTPResult {
    eprintln!("req: {:?}", &req);
    match *req.method() {
        Method::GET => get(store, req).await,
        Method::POST => post(store, req).await,
        _ => response_404(),
    }
}

pub async fn serve(store: Store) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("let's go");
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

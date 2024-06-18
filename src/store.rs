use std::ops::Bound;
use std::path::PathBuf;
use std::time::Duration;

use scru128::Scru128Id;
use ssri::Integrity;

use serde::{Deserialize, Deserializer, Serialize};

use tokio::sync::mpsc;

use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum Content {
    Full(Integrity),
    Stream,
    Empty,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct Frame {
    pub id: Scru128Id,
    pub topic: String,
    pub meta: Option<serde_json::Value>,
    pub content: Content,
}

#[derive(Clone)]
pub struct Store {
    pub path: PathBuf,
    // keep a reference to the keyspace, so we get a fsync when the store is dropped:
    // https://github.com/fjall-rs/fjall/discussions/44
    _keyspace: Keyspace,
    pub partition: PartitionHandle,
    commands_tx: mpsc::Sender<Command>,
}

#[derive(Default, PartialEq, Clone, Debug)]
pub enum FollowOption {
    #[default]
    Off,
    On,
    WithHeartbeat(Duration),
}

impl<'de> Deserialize<'de> for FollowOption {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        if s.is_empty() || s == "follow" || s == "yes" {
            Ok(FollowOption::On)
        } else if let Ok(duration) = s.parse::<u64>() {
            Ok(FollowOption::WithHeartbeat(Duration::from_millis(duration)))
        } else {
            match s.as_str() {
                "true" => Ok(FollowOption::On),
                "false" | "no" => Ok(FollowOption::Off),
                _ => Err(serde::de::Error::custom("Invalid value for follow option")),
            }
        }
    }
}

fn deserialize_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    match s.as_str() {
        "false" | "no" | "0" => Ok(false),
        _ => Ok(true),
    }
}

#[derive(PartialEq, Deserialize, Clone, Debug, Default)]
pub struct ReadOptions {
    #[serde(default)]
    pub follow: FollowOption,
    #[serde(default, deserialize_with = "deserialize_bool")]
    pub tail: bool,
    #[serde(rename = "last-id")]
    pub last_id: Option<Scru128Id>,
}

impl ReadOptions {
    pub fn from_query(query: Option<&str>) -> Result<Self, serde_urlencoded::de::Error> {
        match query {
            Some(q) => serde_urlencoded::from_str(q),
            None => Ok(Self::default()),
        }
    }
}

#[derive(Debug)]
enum Command {
    Read(mpsc::Sender<Frame>, ReadOptions),
    Append(Frame),
}

impl Store {
    pub fn spawn(path: PathBuf) -> Store {
        let config = Config::new(path.join("fjall"));
        let keyspace = config.open().unwrap();
        let partition = keyspace
            .open_partition("main", PartitionCreateOptions::default())
            .unwrap();

        let (tx, mut rx) = mpsc::channel::<Command>(32);

        let store = Store {
            path,
            _keyspace: keyspace,
            partition,
            commands_tx: tx,
        };

        {
            let store = store.clone();
            std::thread::spawn(move || {
                let mut subscribers: Vec<mpsc::Sender<Frame>> = Vec::new();
                'outer: while let Some(command) = rx.blocking_recv() {
                    match command {
                        Command::Read(tx, options) => {
                            if !options.tail {
                                let range = match &options.last_id {
                                    Some(last_id) => (
                                        Bound::Excluded(last_id.to_bytes()),
                                        Bound::<[u8; 16]>::Unbounded,
                                    ),
                                    None => (Bound::Unbounded, Bound::Unbounded),
                                };
                                for record in store.partition.range(range) {
                                    let record = record.unwrap();
                                    let frame: Frame = serde_json::from_slice(&record.1).unwrap();
                                    if tx.blocking_send(frame).is_err() {
                                        continue 'outer;
                                    }
                                }
                            }

                            match options.follow {
                                FollowOption::On | FollowOption::WithHeartbeat(_) => {
                                    let frame = Frame {
                                        id: scru128::new(),
                                        topic: "stream.cross.threshold".into(),
                                        meta: None,
                                        content: Content::Empty,
                                    };
                                    if tx.blocking_send(frame).is_err() {
                                        continue 'outer;
                                    }
                                    subscribers.push(tx);
                                }
                                FollowOption::Off => {
                                    // Do nothing
                                }
                            }
                        }
                        Command::Append(frame) => {
                            subscribers.retain(|tx| tx.blocking_send(frame.clone()).is_ok());
                        }
                    }
                }
            });
        }

        store
    }

    pub async fn read(&self, options: ReadOptions) -> mpsc::Receiver<Frame> {
        let (tx, rx) = mpsc::channel::<Frame>(100);
        self.commands_tx
            .send(Command::Read(tx.clone(), options.clone()))
            .await
            .unwrap(); // our thread went away?

        if let FollowOption::WithHeartbeat(duration) = options.follow {
            tokio::task::spawn(async move {
                loop {
                    tokio::time::sleep(duration).await;
                    let frame = Frame {
                        id: scru128::new(),
                        topic: "stream.cross.pulse".into(),
                        content: Content::Empty,
                        meta: None,
                    };
                    if tx.send(frame).await.is_err() {
                        break;
                    }
                }
            });
        }

        rx
    }

    pub fn get(&self, id: &Scru128Id) -> Option<Frame> {
        let res = self.partition.get(id.to_bytes()).unwrap();
        res.map(|value| serde_json::from_slice(&value).unwrap())
    }

    pub async fn cas_reader(&self, hash: Integrity) -> cacache::Result<cacache::Reader> {
        cacache::Reader::open_hash(&self.path.join("cacache"), hash).await
    }

    pub async fn cas_writer(&self) -> cacache::Result<cacache::Writer> {
        cacache::WriteOpts::new()
            .open_hash(&self.path.join("cacache"))
            .await
    }

    pub async fn cas_insert(&self, content: &str) -> cacache::Result<Integrity> {
        cacache::write_hash(&self.path.join("cacache"), content).await
    }

    pub async fn cas_read(&self, hash: &Integrity) -> cacache::Result<Vec<u8>> {
        cacache::read_hash(&self.path.join("cacache"), hash).await
    }

    pub async fn append(
        &mut self,
        topic: &str,
        meta: Option<serde_json::Value>,
        content: Content,
    ) -> Frame {
        let frame = Frame {
            id: scru128::new(),
            topic: topic.to_string(),
            meta,
            content,
        };
        let encoded: Vec<u8> = serde_json::to_vec(&frame).unwrap();
        self.partition.insert(frame.id.to_bytes(), encoded).unwrap();

        self.commands_tx
            .send(Command::Append(frame.clone()))
            .await
            .unwrap(); // our thread went away?

        frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::assert_impl_all;

    #[test]
    fn test_store_is_send_sync() {
        assert_impl_all!(Store: Send, Sync);
    }
}

#[cfg(test)]
mod tests_read_options {
    use super::*;

    #[derive(Debug)]
    struct TestCase<'a> {
        input: Option<&'a str>,
        expected: ReadOptions,
    }

    #[test]
    fn test_from_query() {
        let test_cases = [
            TestCase {
                input: None,
                expected: ReadOptions {
                    follow: FollowOption::Off,
                    tail: false,
                    last_id: None,
                },
            },
            TestCase {
                input: Some("foo=bar"),
                expected: ReadOptions {
                    follow: FollowOption::Off,
                    tail: false,
                    last_id: None,
                },
            },
            TestCase {
                input: Some("follow"),
                expected: ReadOptions {
                    follow: FollowOption::On,
                    tail: false,
                    last_id: None,
                },
            },
            TestCase {
                input: Some("follow=1"),
                expected: ReadOptions {
                    follow: FollowOption::WithHeartbeat(Duration::from_millis(1)),
                    tail: false,
                    last_id: None,
                },
            },
            TestCase {
                input: Some("follow=yes"),
                expected: ReadOptions {
                    follow: FollowOption::On,
                    tail: false,
                    last_id: None,
                },
            },
            TestCase {
                input: Some("follow=true"),
                expected: ReadOptions {
                    follow: FollowOption::On,
                    tail: false,
                    last_id: None,
                },
            },
            TestCase {
                input: Some("last-id=03BIDZVKNOTGJPVUEW3K23G45"),
                expected: ReadOptions {
                    follow: FollowOption::Off,
                    tail: false,
                    last_id: Some("03BIDZVKNOTGJPVUEW3K23G45".parse().unwrap()),
                },
            },
            TestCase {
                input: Some("follow&last-id=03BIDZVKNOTGJPVUEW3K23G45"),
                expected: ReadOptions {
                    follow: FollowOption::On,
                    tail: false,
                    last_id: Some("03BIDZVKNOTGJPVUEW3K23G45".parse().unwrap()),
                },
            },
        ];

        for case in &test_cases {
            let options = ReadOptions::from_query(case.input);
            assert_eq!(options, Ok(case.expected.clone()), "case {:?}", case.input);
        }

        assert!(ReadOptions::from_query(Some("last-id=123")).is_err());
    }
}

#[cfg(test)]
mod tests_store {
    use super::*;
    use tempfile::TempDir;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_get() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = Store::spawn(temp_dir.into_path());
        let meta = serde_json::json!({"key": "value"});
        eprintln!("meta: {:?}", &meta);
        let frame = store.append("stream", None, Some(meta)).await;
        let got = store.get(&frame.id);
        assert_eq!(Some(frame.clone()), got);
    }

    #[tokio::test]
    async fn test_follow() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = Store::spawn(temp_dir.into_path());

        // Append two initial clips
        let f1 = store.append("stream", None, None).await;
        let f2 = store.append("stream", None, None).await;

        // cat the full stream and follow new items with a heartbeat every 5ms
        let follow_options = ReadOptions {
            follow: FollowOption::WithHeartbeat(Duration::from_millis(5)),
            tail: false,
            last_id: None,
        };
        let mut recver = store.read(follow_options).await;

        assert_eq!(f1, recver.recv().await.unwrap());
        assert_eq!(f2, recver.recv().await.unwrap());

        // crossing the threshold
        assert_eq!(
            "stream.cross.threshold".to_string(),
            recver.recv().await.unwrap().topic
        );

        // Append two more clips
        let f3 = store.append("stream", None, None).await;
        let f4 = store.append("stream", None, None).await;
        assert_eq!(f3, recver.recv().await.unwrap());
        assert_eq!(f4, recver.recv().await.unwrap());

        // Assert we see some heartbeats
        assert_eq!(
            "stream.cross.pulse".to_string(),
            recver.recv().await.unwrap().topic
        );
        assert_eq!(
            "stream.cross.pulse".to_string(),
            recver.recv().await.unwrap().topic
        );
    }

    #[tokio::test]
    async fn test_stream_basics() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = Store::spawn(temp_dir.into_path());

        let f1 = store.append("/stream", None, None).await;
        let f2 = store.append("/stream", None, None).await;

        let recver = store.read(ReadOptions::default()).await;
        assert_eq!(
            tokio_stream::wrappers::ReceiverStream::new(recver)
                .collect::<Vec<Frame>>()
                .await,
            vec![f1.clone(), f2.clone()]
        );

        let recver = store
            .read(ReadOptions {
                follow: FollowOption::Off,
                tail: false,
                last_id: Some(f1.id),
            })
            .await;
        assert_eq!(
            tokio_stream::wrappers::ReceiverStream::new(recver)
                .collect::<Vec<Frame>>()
                .await,
            vec![f2]
        );
    }
}

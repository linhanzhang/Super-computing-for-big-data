use futures::StreamExt;
use geojson::{Feature, GeoJson};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, DefaultConsumerContext, StreamConsumer},
    message::Message,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::Infallible, error::Error};
use tokio::{fs::read_to_string, sync::broadcast, task};
use tokio_stream::wrappers::BroadcastStream;
use warp::Filter;

#[derive(Deserialize)]
struct Update {
    refugees: u64,
    city_id: Option<u64>,
    // city_name: String, -- ignore
    change: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct Event {
    count: u64,
    feature: Option<Feature>,
    change: Option<u64>,
}

fn read_city_map(input: GeoJson) -> HashMap<u64, Feature> {
    match input {
        GeoJson::FeatureCollection(collection) => collection
            .features
            .into_iter()
            .map(|feature| {
                (
                    feature
                        .properties
                        .as_ref()
                        .unwrap()
                        .get("population")
                        .unwrap()
                        .as_u64()
                        .unwrap(),
                    (
                        feature
                            .properties
                            .as_ref()
                            .unwrap()
                            .get("geoname_id")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .parse::<u64>()
                            .unwrap(),
                        feature,
                    ),
                )
            })
            .filter_map(|(pop, x)| (pop > 400_000).then(|| x))
            .collect(),
        _ => panic!("bad file"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (tx, _) = broadcast::channel(10000);
    let tx2 = tx.clone();

    task::spawn(async move {
        let cities = read_city_map(
            read_to_string("/src/cities.geojson")
                .await
                .unwrap()
                .parse::<GeoJson>()
                .expect("failed to parse geojson"),
        );

        let consumer: StreamConsumer<_> = ClientConfig::new()
            .set("group.id", "123456")
            .set("bootstrap.servers", "kafka-server:9092")
            .create_with_context(DefaultConsumerContext)
            .expect("failed to create consumer");

        consumer
            .subscribe(&["updates"])
            .expect("failed to subscribe");

        let mut stream = consumer.stream();

        while let Some(Ok(message)) = stream.next().await {
            if let Some(Ok(message)) = message.payload_view::<str>() {
                if let Ok(update) = serde_json::from_str::<Update>(message) {
                    let feature = update
                        .city_id
                        .and_then(|city_id| cities.get(&city_id))
                        .cloned();
                    let event = Event {
                        count: update.refugees,
                        change: update.change,
                        feature,
                    };
                    if let Ok(event) = serde_json::to_string(&event) {
                        if tx2.receiver_count() > 0 {
                            if let Err(e) = tx2.send(event) {
                                eprintln!("Failed to send: {:?}", e)
                            }
                        }
                    }
                }
            }
        }
    });

    let updates = warp::path("updates").and(warp::get()).map(move || {
        let rx = tx.clone().subscribe();
        warp::sse::reply(warp::sse::keep_alive().stream(BroadcastStream::new(rx).map(
            |msg| -> Result<_, Infallible> { Ok(warp::sse::Event::default().data(msg.unwrap())) },
        )))
    });

    warp::serve(
        warp::path("dist")
            .and(warp::fs::dir("/dist"))
            .or(warp::path::end().and(warp::fs::file("/dist/index.html")))
            .or(updates),
    )
    .run(([0, 0, 0, 0], 1234))
    .await;

    Ok(())
}

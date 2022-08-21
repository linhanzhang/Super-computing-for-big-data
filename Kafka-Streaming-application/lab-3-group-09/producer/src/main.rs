use chrono::Utc;
use geojson::GeoJson;
use rand::{distributions::WeightedIndex, prelude::Distribution, thread_rng, Rng};
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use serde::Serialize;
use std::{
    error::Error,
    fs::read_to_string,
    thread::{self, sleep},
    time::Duration,
};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct City {
    id: u64,
    name: String,
    population: u64,
}

#[derive(Serialize)]
pub struct Event {
    timestamp: u64,
    city_id: u64,
    city_name: String,
    refugees: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Read and parse input file with cities information.
    let cities = match read_to_string("/src/cities.geojson")?.parse::<GeoJson>()? {
        GeoJson::FeatureCollection(ref collection) => collection
            .features
            .iter()
            .filter_map(|feature| feature.properties.as_ref())
            .filter_map(|properties| {
                properties.get("geoname_id").and_then(|id| {
                    properties.get("name").and_then(|name| {
                        properties
                            .get("population")
                            .map(|population| (id, name, population))
                    })
                })
            })
            .filter_map(|(id, name, population)| {
                id.as_str().and_then(|id| {
                    name.as_str().and_then(|name| {
                        population.as_u64().map(|population| City {
                            id: id.parse::<u64>().unwrap(),
                            name: name.to_string(),
                            population,
                        })
                    })
                })
            })
            .filter(|City { population, .. }| *population > 400_000)
            .collect::<Vec<_>>(),
        _ => panic!("bad file"),
    };

    // Create weighted index for cities based on population.
    let mut weights = WeightedIndex::new(
        cities
            .iter()
            .map(|City { population, .. }| population * (*population as f64).sqrt() as u64),
    )?;

    // Create channel to send events over.
    let (tx, mut rx) = mpsc::channel(10);

    // Spawn thread that produces the stream of random events.
    thread::spawn(move || {
        let mut rng = thread_rng();

        let mut rand_city = || {
            // Get random index weighted by population.
            let index = weights.sample(&mut rng);
            // Remove the city from the vec.
            let mut city = cities[index].clone();
            // Reduce population to the part that evacuates.
            city.population /= 4;
            // Remove city from weights index and return city
            weights.update_weights(&[(index, &0)]).map(|_| city)
        };

        // Fill buffer with 10 random cities.
        let mut buf = Vec::with_capacity(10);
        for _ in 0..10 {
            buf.push(rand_city().unwrap());
        }

        // Produce loop.
        let mut rng = thread_rng();
        loop {
            // Pick a random city from the buffer.
            let idx = rng.gen_range(0..buf.len());
            let city = buf.get_mut(idx).unwrap();

            // Evacuate some people.
            let refugees = rng.gen_range(
                (city.population / 30).min(10_000.max(city.population))..=city.population,
            );
            // Update city population.
            city.population -= refugees;
            let population = city.population;

            // Send the city and stop thread when receiver is dropped.
            if tx
                .blocking_send(Event {
                    timestamp: Utc::now().timestamp_millis() as u64,
                    city_id: city.id,
                    city_name: city.name.clone(),
                    refugees,
                })
                .is_err()
            {
                return;
            }

            // Replace empty cities.
            if population == 0 {
                if let Ok(city) = rand_city() {
                    buf[idx] = city;
                } else {
                    // check if all cities are empty
                    if buf.iter().all(|City { population, .. }| *population == 0) {
                        return;
                    }
                }
            }
        }
    });

    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", "kafka-server:9092")
        .create()?;

    let mut rng = thread_rng();

    while let Some(event) = rx.recv().await {
        producer
            .send(
                FutureRecord::to("events")
                    .payload(&serde_json::to_string(&event)?)
                    .key(&event.city_id.to_string()),
                Duration::from_secs(0),
            )
            .await
            .map_err(|(err, _)| err)?;
        sleep(Duration::from_millis(rng.gen_range(100..500)));
    }

    Ok(())
}

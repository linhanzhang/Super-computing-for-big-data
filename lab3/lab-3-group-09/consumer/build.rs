use std::{error::Error, fs::File, path::PathBuf};

static ROOT: &str = env!("CARGO_MANIFEST_DIR");
static CITIES: &str = "https://data.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000@public/download/?format=geojson&timezone=Europe/Berlin";

fn get<S: AsRef<str>>(url: S, dest: S) -> Result<(), Box<dyn Error>> {
    let dest = PathBuf::from(ROOT).join(dest.as_ref());
    if !dest.exists() {
        let mut file = File::create(dest)?;
        reqwest::blocking::get(url.as_ref())?.copy_to(&mut file)?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    get(CITIES, "cities.geojson")?;
    Ok(())
}

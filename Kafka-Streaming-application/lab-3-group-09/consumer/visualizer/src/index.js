import "./index.css";
import chroma from "chroma-js";
import * as L from "leaflet";

const map = L.map("map").setView([0, 0], 2);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution:
    '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  noWrap: true,
}).addTo(map);
const group = new L.LayerGroup().addTo(map);

const color = chroma
  .scale([
    "#4470b1",
    "#3b92b8",
    "#59b4aa",
    "#7ecba4",
    "#a6dba4",
    "#cae99d",
    "#e8f69c",
    "#f7fcb3",
    "#fef5af",
    "#fee391",
    "#fdc877",
    "#fcaa5f",
    "#f7834d",
    "#ec6145",
    "#da464c",
    "#be2449",
  ])
  .mode("lch");

const defaultStyle = {
  weight: 1,
  opacity: 0.9,
};

const style = (size, change) => ({
  radius: 7 + size * 7,
  color: color(change),
  fillOpacity: change === 0 ? 0 : 0.5,
  fillColor: color(change),
  ...defaultStyle,
});

const layers = new Map();
const counts = new Map();
const changes = new Map();
const updates = new EventSource("http://localhost:12345/updates");

updates.onmessage = (event) => {
  let { count, feature, change } = JSON.parse(event.data);

  // It could be that there is just a count
  if (feature === null) {
    console.log(`Total: ${count}`);
  } else {
    const id = feature.properties.geoname_id;

    if (change !== null) {
      changes.set(id, change);
    }

    counts.set(id, count);
    const maxCount = Math.max(...counts.values());
    const scaleCount = (count) => count / maxCount;
    const scaleChange = (change, count) => change ? change / count : 0;

    if (layers.has(id)) {
      layers.get(id).remove();
      layers.delete(id);
    }

    layers.forEach((layer, id) => {
      group
        .getLayer(group.getLayerId(layer))
        .setStyle(style(scaleCount(counts.get(id)), scaleChange(changes.get(id), counts.get(id))));
    });

    if (count > 0) {
      const change_text = (change === null) ? `?` : `${change}`;
      const text = `
<div>
<b>${feature.properties.name}</b></br>
Total: ${count}</br>
Recent: ${change_text}
</div>`
      const layer = L.geoJSON(feature, {
        pointToLayer: (_, latlng) =>
          L.circleMarker(latlng, style(scaleCount(count), scaleChange(change, count)))
            .bindPopup(text)
            .bindTooltip(text)
      });

      layers.set(id, layer);
      group.addLayer(layer);
    }
  }

};

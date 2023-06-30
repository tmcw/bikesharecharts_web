import * as duckdb from '@duckdb/duckdb-wasm';
import * as d3 from 'd3';
// import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.17.0/+esm';

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

// Select a bundle based on browser checks
const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

const worker_url = URL.createObjectURL(
  new Blob([`importScripts("${bundle.mainWorker}");`], {type: 'text/javascript'})
);

// Instantiate the asynchronus version of DuckDB-wasm
const worker = new Worker(worker_url);
const logger = new duckdb.ConsoleLogger();
const db = new duckdb.AsyncDuckDB(logger, worker);
await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
URL.revokeObjectURL(worker_url);

const c = await db.connect();

const width = window.innerWidth * 2;

const res = await fetch('./data.parquet');

await db.registerFileBuffer('station_status.parquet', new Uint8Array(await res.arrayBuffer()));

const bounds = await c.query('SELECT MAX(times) as max, MIN(times) as min FROM "station_status.parquet";');
const b = bounds.toArray()[0].toJSON();


const scaleX = d3.scaleTime().domain([b.min, b.max]).range([0, width]);

const q = c.send('SELECT * FROM "station_status.parquet" ORDER BY times;');

const height = 20;

const chartsContainer = document.querySelector('#charts');
/** @type Map<string, [HTMLDivElement, HTMLCanvasElement, CanvasRenderingContext2D]> */
const charts = new Map();

for await (const batch of await q) {
    for (let row of batch.toArray()) {
        const data = row.toJSON();
        if (!charts.has(data.station_ids)) {
            const chart = document.createElement('div');
            const canvas = document.createElement('canvas');
            canvas.width = width;
            canvas.height = height;
            canvas.style.width = width / 2;
            canvas.style.height = height / 2;
            canvas.className = 'chart-canvas';
            chart.appendChild(canvas);
            chartsContainer.appendChild(chart);
            const ctx = canvas.getContext('2d')
            ctx.fillStyle = 'blue';
            charts.set(data.station_ids, [ chart, canvas, ctx ]);
        }
        const ctx = charts.get(data.station_ids)[2];
        ctx.fillRect(
            ~~scaleX(data.times),
            height - data.num_bikes_available,
             10, height);
    }
}

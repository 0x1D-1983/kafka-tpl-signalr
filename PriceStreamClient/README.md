# PriceStream React Client

React app that consumes the EPEX price stream from the SignalR hub. Subscribe to symbols (e.g. DE, FR) and see live price updates. Resubscribes automatically after reconnection.

## Setup

```bash
cd PriceStreamClient
npm install
```

## Run

1. Start the backend (PriceStream) so the hub is available at `http://localhost:5044`.
2. Start the React app:

   ```bash
   npm run dev
   ```

3. Open http://localhost:5173. Vite proxies `/hubs` to the backend, so the SignalR connection works without CORS. Price updates arrive only after you click Subscribe, and only for the symbols in the textbox. A dropped connection keeps retrying, then resubscribes to those same symbols.

   Against the kind cluster (edge on port 8080):

   ```bash
   VITE_PROXY_TARGET=http://localhost:8080 npm run dev
   ```

## Backend on a different host

Set the hub URL via env when building or at runtime. For dev, in `src/hooks/usePriceStream.js` you can pass a base URL, or use `VITE_HUB_URL`:

```js
const hubUrl = import.meta.env.VITE_HUB_URL || '/hubs/pricestream'
```

Then run with `VITE_HUB_URL=https://your-backend/hubs/pricestream npm run dev`. Ensure the backend allows CORS for your React origin if not using the proxy.

## Build

```bash
npm run build
```

Output is in `dist/`. Serve with any static host; point the app at your backend hub URL (e.g. env or config).

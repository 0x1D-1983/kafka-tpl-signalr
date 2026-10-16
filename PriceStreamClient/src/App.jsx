import { useState } from 'react'
import * as signalR from '@microsoft/signalr'
import { usePriceStream } from './hooks/usePriceStream'
import './App.css'

const stateLabels = {
  [signalR.HubConnectionState.Connected]: 'Connected',
  [signalR.HubConnectionState.Connecting]: 'Connecting',
  [signalR.HubConnectionState.Disconnected]: 'Disconnected',
  [signalR.HubConnectionState.Reconnecting]: 'Reconnecting',
}

function formatTime(ts) {
  if (!ts) return '—'
  try {
    const d = new Date(ts)
    return Number.isNaN(d.getTime()) ? ts : d.toLocaleTimeString()
  } catch {
    return ts
  }
}

export default function App() {
  const [symbolInput, setSymbolInput] = useState('DE, FR')
  const { connectionState, updates, subscribedSymbols, rejectedSymbols, subscribe } = usePriceStream()

  const handleSubscribe = (e) => {
    e.preventDefault()
    const symbols = symbolInput.split(/[\s,]+/).filter(Boolean)
    subscribe(symbols)
  }

  const isConnected = connectionState === signalR.HubConnectionState.Connected

  return (
    <div className="app">
      <header className="header">
        <h1>EPEX Price Stream</h1>
        <p className="status" data-state={connectionState}>
          {stateLabels[connectionState] ?? 'Unknown'}
        </p>
      </header>

      <form className="subscribe-form" onSubmit={handleSubscribe}>
        <label htmlFor="symbols">Symbols (comma-separated)</label>
        <div className="row">
          <input
            id="symbols"
            type="text"
            value={symbolInput}
            onChange={(e) => setSymbolInput(e.target.value)}
            placeholder="DE, FR, AT"
            disabled={!isConnected}
          />
          <button type="submit" disabled={!isConnected}>
            Subscribe
          </button>
        </div>
        {subscribedSymbols.length > 0 && (
          <p className="subscribed">Subscribed: {subscribedSymbols.join(', ')}</p>
        )}
        {rejectedSymbols.length > 0 && (
          <p className="rejected">
            Rejected: {rejectedSymbols.map((r) => `${r.symbol} (${r.reason})`).join(', ')}
          </p>
        )}
      </form>

      <section className="updates">
        <h2>Price updates</h2>
        {updates.length === 0 ? (
          <p className="empty">No updates yet. Subscribe to symbols and produce messages to the Kafka topic.</p>
        ) : (
          <ul className="update-list">
            {updates.map((u) => (
              <li key={u.id} className="update-item">
                <span className="symbol">{u.symbol}</span>
                <span className="price">{u.price != null ? Number(u.price).toFixed(2) : '—'}</span>
                <span className="unit">{u.unit || 'EUR/MWh'}</span>
                <span className="meta">
                  {u.deliveryArea && `${u.deliveryArea} · `}
                  {u.market && `${u.market} · `}
                  {u.contract && `${u.contract} · `}
                  {formatTime(u.timestamp)}
                </span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  )
}

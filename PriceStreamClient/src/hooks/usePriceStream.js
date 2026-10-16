import { useEffect, useRef, useState, useCallback } from 'react'
import * as signalR from '@microsoft/signalr'

const STORAGE_KEY = 'pricestream-symbols'
const START_BASE_DELAY_MS = 1000
const START_MAX_DELAY_MS = 30000

function normalizeSymbols(symbols) {
  return [...new Set(symbols.map((s) => String(s).trim().toUpperCase()).filter(Boolean))]
}

// One symbol carries many instruments (day-ahead and intraday, hourly and quarter-hour contracts).
// Must match the server's InstrumentKey: "newer replaces older" only applies within one instrument.
export function instrumentKey(update) {
  return [update?.symbol, update?.market, update?.deliveryArea, update?.contract ?? ''].join('\u001f')
}

export function usePriceStream(hubUrl) {
  const [connectionState, setConnectionState] = useState(signalR.HubConnectionState.Disconnected)
  const [updates, setUpdates] = useState([])
  const [subscribedSymbols, setSubscribedSymbols] = useState([])
  const [rejectedSymbols, setRejectedSymbols] = useState([])
  const connectionRef = useRef(null)
  const symbolsRef = useRef([])
  const latestTimestampRef = useRef(new Map())

  const applySymbols = useCallback((symbols) => {
    symbolsRef.current = symbols
    try {
      localStorage.setItem(STORAGE_KEY, symbols.join(','))
    } catch (_) {}
    setSubscribedSymbols(symbols)
    setUpdates((prev) => prev.filter((u) => symbols.includes(String(u.symbol ?? '').toUpperCase())))
  }, [])

  // The server returns { accepted, rejected: [{ symbol, reason }] }. Only accepted symbols are kept.
  const subscribe = useCallback(async (symbols) => {
    const normalized = normalizeSymbols(symbols)
    const previous = symbolsRef.current
    const removed = previous.filter((s) => !normalized.includes(s))
    for (const [key, latest] of latestTimestampRef.current) {
      if (removed.includes(latest.symbol)) latestTimestampRef.current.delete(key)
    }
    applySymbols(normalized)
    setRejectedSymbols([])
    const conn = connectionRef.current
    if (conn && conn.state === signalR.HubConnectionState.Connected) {
      if (removed.length > 0) await conn.invoke('Unsubscribe', removed)
      if (normalized.length > 0) {
        const result = await conn.invoke('Subscribe', normalized)
        applySymbols(result?.accepted ?? [])
        setRejectedSymbols(result?.rejected ?? [])
      }
    }
  }, [applySymbols])

  const resubscribe = useCallback(async () => {
    const conn = connectionRef.current
    if (conn && conn.state === signalR.HubConnectionState.Connected && symbolsRef.current.length > 0) {
      const result = await conn.invoke('Subscribe', symbolsRef.current)
      applySymbols(result?.accepted ?? [])
      setRejectedSymbols(result?.rejected ?? [])
    }
  }, [applySymbols])

  useEffect(() => {
    const url = hubUrl || '/hubs/pricestream'
    const connection = new signalR.HubConnectionBuilder()
      .withUrl(url)
      // Keep trying. A dead pod is dropped by the load balancer; the next attempt lands on a healthy one.
      .withAutomaticReconnect({
        nextRetryDelayInMilliseconds(retryContext) {
          const delays = [0, 2000, 5000, 10000]
          return delays[Math.min(retryContext.previousRetryCount, delays.length - 1)]
        },
      })
      .build()

    connection.on('PriceUpdate', (data) => {
      const symbol = String(data?.symbol ?? '').toUpperCase()
      if (!symbolsRef.current.includes(symbol)) return
      // Source time is already on the payload. A late tick must not move an older price
      // above a newer one when two sends for the same instrument pass each other.
      const incoming = Date.parse(data?.timestamp ?? '')
      if (Number.isFinite(incoming)) {
        const key = instrumentKey(data)
        const latest = latestTimestampRef.current.get(key)
        if (latest !== undefined && incoming < latest.timestamp) return
        latestTimestampRef.current.set(key, { symbol, timestamp: incoming })
      }
      setUpdates((prev) => [{ ...data, id: crypto.randomUUID() }, ...prev].slice(0, 200))
    })

    connection.onreconnecting(() => {
      setConnectionState(signalR.HubConnectionState.Reconnecting)
    })

    connection.onreconnected(async () => {
      setConnectionState(signalR.HubConnectionState.Connected)
      await resubscribe()
    })

    connection.onclose(() => {
      setConnectionState(signalR.HubConnectionState.Disconnected)
    })

    // withAutomaticReconnect only covers a connection that was up once; the first start() is retried here.
    let stopped = false
    let retryTimer
    let wakeRetry
    const sleep = (ms) =>
      new Promise((resolve) => {
        wakeRetry = resolve
        retryTimer = setTimeout(resolve, ms)
      })

    const startWithRetry = async () => {
      setConnectionState(signalR.HubConnectionState.Connecting)
      for (let attempt = 0; !stopped; attempt++) {
        try {
          await connection.start()
        } catch (err) {
          if (stopped) return
          // Full jitter, so clients that failed together (edge down, deploy) do not retry together.
          const delay = Math.random() * Math.min(START_MAX_DELAY_MS, START_BASE_DELAY_MS * 2 ** attempt)
          console.warn(`SignalR start failed, retrying in ${Math.round(delay)} ms`, err)
          await sleep(delay)
          continue
        }
        if (stopped) return
        setConnectionState(signalR.HubConnectionState.Connected)
        await resubscribe().catch((err) => console.error('Subscribe after connect failed', err))
        return
      }
    }

    connectionRef.current = connection
    startWithRetry()
    return () => {
      stopped = true
      clearTimeout(retryTimer)
      wakeRetry?.()
      connection.stop()
      connectionRef.current = null
    }
  }, [hubUrl, resubscribe])

  return { connectionState, updates, subscribedSymbols, rejectedSymbols, subscribe }
}

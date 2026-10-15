import { useEffect, useRef, useState, useCallback } from 'react'
import * as signalR from '@microsoft/signalr'

const STORAGE_KEY = 'pricestream-symbols'

function normalizeSymbols(symbols) {
  return [...new Set(symbols.map((s) => String(s).trim().toUpperCase()).filter(Boolean))]
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
    for (const symbol of removed) latestTimestampRef.current.delete(symbol)
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
      // above a newer one when two sends for the same symbol pass each other.
      const incoming = Date.parse(data?.timestamp ?? '')
      if (Number.isFinite(incoming)) {
        const latest = latestTimestampRef.current.get(symbol)
        if (latest !== undefined && incoming < latest) return
        latestTimestampRef.current.set(symbol, incoming)
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

    let stopped = false
    connection
      .start()
      .then(() => {
        if (stopped) return
        setConnectionState(signalR.HubConnectionState.Connected)
      })
      .catch((err) => {
        if (stopped) return
        console.error('SignalR connection failed', err)
        setConnectionState(signalR.HubConnectionState.Disconnected)
      })

    connectionRef.current = connection
    return () => {
      stopped = true
      connection.stop()
      connectionRef.current = null
    }
  }, [hubUrl, resubscribe])

  return { connectionState, updates, subscribedSymbols, rejectedSymbols, subscribe }
}

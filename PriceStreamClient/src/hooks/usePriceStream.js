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
  const connectionRef = useRef(null)
  const symbolsRef = useRef([])

  const subscribe = useCallback(async (symbols) => {
    const normalized = normalizeSymbols(symbols)
    const previous = symbolsRef.current
    const removed = previous.filter((s) => !normalized.includes(s))
    symbolsRef.current = normalized
    try {
      localStorage.setItem(STORAGE_KEY, normalized.join(','))
    } catch (_) {}
    setSubscribedSymbols(normalized)
    setUpdates((prev) => prev.filter((u) => normalized.includes(String(u.symbol ?? '').toUpperCase())))
    const conn = connectionRef.current
    if (conn && conn.state === signalR.HubConnectionState.Connected) {
      if (removed.length > 0) await conn.invoke('Unsubscribe', removed)
      if (normalized.length > 0) await conn.invoke('Subscribe', normalized)
    }
  }, [])

  const resubscribe = useCallback(async () => {
    const conn = connectionRef.current
    if (conn && conn.state === signalR.HubConnectionState.Connected && symbolsRef.current.length > 0) {
      await conn.invoke('Subscribe', symbolsRef.current)
    }
  }, [])

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

  return { connectionState, updates, subscribedSymbols, subscribe }
}

import { useEffect, useRef, useState, useCallback } from 'react'
import * as signalR from '@microsoft/signalr'

const STORAGE_KEY = 'pricestream-symbols'

function loadSavedSymbols() {
  try {
    const s = localStorage.getItem(STORAGE_KEY)
    return s ? s.split(',').map((x) => x.trim()).filter(Boolean) : []
  } catch {
    return []
  }
}

export function usePriceStream(hubUrl) {
  const initialRef = useRef(null)
  if (initialRef.current === null) initialRef.current = loadSavedSymbols()
  const [connectionState, setConnectionState] = useState(signalR.HubConnectionState.Disconnected)
  const [updates, setUpdates] = useState([])
  const [subscribedSymbols, setSubscribedSymbols] = useState(initialRef.current)
  const connectionRef = useRef(null)
  const symbolsRef = useRef([...initialRef.current])

  const subscribe = useCallback(async (symbols) => {
    const normalized = symbols
      .map((s) => String(s).trim().toUpperCase())
      .filter(Boolean)
    if (normalized.length === 0) return
    symbolsRef.current = normalized
    try {
      localStorage.setItem(STORAGE_KEY, normalized.join(','))
    } catch (_) {}
    setSubscribedSymbols(normalized)
    const conn = connectionRef.current
    if (conn && conn.state === signalR.HubConnectionState.Connected) {
      await conn.invoke('Subscribe', normalized)
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

    connection
      .start()
      .then(() => {
        setConnectionState(signalR.HubConnectionState.Connected)
        if (symbolsRef.current.length > 0) {
          return connection.invoke('Subscribe', symbolsRef.current)
        }
      })
      .catch((err) => {
        console.error('SignalR connection failed', err)
        setConnectionState(signalR.HubConnectionState.Disconnected)
      })

    connectionRef.current = connection
    return () => {
      connection.stop()
      connectionRef.current = null
    }
  }, [hubUrl, resubscribe])

  return { connectionState, updates, subscribedSymbols, subscribe }
}

import { useEffect, useRef, useState } from 'react'
import type { TrajectoryResponse } from '@/types/voyage'

export interface VoyageCorridorState {
  data: TrajectoryResponse | null
  loading: boolean
  error: string | null
  selectedVariable: string | null
  setSelectedVariable: (v: string) => void
}

export interface UseVoyageCorridorOptions {
  lineString: GeoJSON.Feature<GeoJSON.LineString> | null
  model: string
  runId: string | null
  numSamples?: number
  speedKnots?: number
}

export function useVoyageCorridor({
  lineString,
  model,
  runId,
  numSamples = 40,
  speedKnots,
}: UseVoyageCorridorOptions): VoyageCorridorState {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const [data, setData] = useState<TrajectoryResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selectedVariable, setSelectedVariable] = useState<string | null>(null)
  const requestIdRef = useRef(0)

  useEffect(() => {
    if (!lineString || !runId) {
      setData(null)
      return
    }

    const controller = new AbortController()
    const requestId = ++requestIdRef.current

    async function fetchTrajectory() {
      setLoading(true)
      setError(null)
      try {
        const body: Record<string, unknown> = {
          type: 'LineString',
          coordinates: lineString!.geometry.coordinates,
          num_samples: numSamples,
        }
        if (speedKnots && speedKnots > 0) {
          body.speed_knots = speedKnots
        }

        const res = await fetch(
          `${apiBase}/v1/edr/collections/${model}/instances/${runId}/trajectory`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
            signal: controller.signal,
          },
        )
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const result: TrajectoryResponse = await res.json()
        if (requestId !== requestIdRef.current) return
        setData(result)
        setSelectedVariable((prev) => {
          if (prev && result.ranges[prev]) return prev
          return Object.keys(result.ranges)[0] ?? null
        })
      } catch (err) {
        if (err instanceof DOMException && err.name === 'AbortError') return
        if (requestId !== requestIdRef.current) return
        setData(null)
        setError(err instanceof Error ? err.message : 'Unknown error')
      } finally {
        if (requestId === requestIdRef.current) {
          setLoading(false)
        }
      }
    }

    fetchTrajectory()
    return () => controller.abort()
  }, [apiBase, lineString, model, runId, numSamples, speedKnots])

  return { data, loading, error, selectedVariable, setSelectedVariable }
}

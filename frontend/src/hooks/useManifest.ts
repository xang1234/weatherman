import { useEffect, useState } from 'react'
import type { UIManifest } from '@/types/manifest'

export interface UseManifestOptions {
  model: string
  runId: string | null
}

export function useManifest({ model, runId }: UseManifestOptions): UIManifest | null {
  const [manifest, setManifest] = useState<UIManifest | null>(null)
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''

  useEffect(() => {
    setManifest(null)
    if (!runId) return

    const controller = new AbortController()

    async function fetchManifest() {
      try {
        const res = await fetch(
          `${apiBase}/api/manifest/${model}/${runId}`,
          { signal: controller.signal },
        )
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data: UIManifest = await res.json()
        if (data.schema_version !== 1) {
          throw new Error(`Unsupported manifest schema version ${data.schema_version}`)
        }
        setManifest(data)
      } catch (err) {
        if (err instanceof DOMException && err.name === 'AbortError') return
        console.warn('Failed to fetch manifest:', err)
      }
    }

    fetchManifest()
    return () => controller.abort()
  }, [apiBase, model, runId])

  return manifest
}

export interface DataAgeThresholds {
  /** Minutes after which status transitions from green to amber */
  amberMinutes: number
  /** Minutes after which status transitions from amber to red */
  redMinutes: number
}

export type FreshnessStatus = 'fresh' | 'aging' | 'stale'

export interface DataAgeState {
  /** Model name (e.g. "gfs") */
  model: string
  /** Run cycle ID (e.g. "20260306T12Z") */
  runId: string
  /** When this run was published */
  publishedAt: Date
  /** Minutes since publish */
  ageMinutes: number
  /** Color-coded freshness status */
  status: FreshnessStatus
  /** Whether the backend is currently unreachable */
  isOffline: boolean
  /** Error message if backend is unreachable */
  error?: string
}

export const DEFAULT_THRESHOLDS: DataAgeThresholds = {
  amberMinutes: 120,  // 2 hours
  redMinutes: 360,    // 6 hours
}

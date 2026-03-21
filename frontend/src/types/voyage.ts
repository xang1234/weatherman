/** Types for the Voyage Weather Corridor feature. */

export interface TrajectoryRequest {
  type: 'LineString'
  coordinates: [number, number][]
  num_samples?: number
  speed_knots?: number
}

export interface TrajectoryRange {
  type: string
  dataType: string
  axisNames: string[]
  shape: [number, number]
  values: Array<Array<number | null>>
}

export interface TrajectoryRoute {
  distances_nm: number[]
  total_nm: number
  speed_knots?: number
  eta_hours?: number[]
}

export interface TrajectoryResponse {
  type: string
  domain: {
    axes: {
      composite: {
        values: [number, number][]
      }
      t: {
        values: number[]
      }
    }
  }
  parameters: Record<string, {
    type: string
    observedProperty?: { label?: { en?: string } }
    unit?: { symbol?: string }
  }>
  ranges: Record<string, TrajectoryRange>
  route: TrajectoryRoute
}

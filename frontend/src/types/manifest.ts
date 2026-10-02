export interface ValueRange {
  min: number
  max: number
}

export interface ColorStop {
  position: number          // 0.0-1.0
  color: [number, number, number]  // RGB
  alpha?: number            // 0-255, defaults to 255
}

export interface LayerConfig {
  id: string
  display_name: string
  unit: string
  palette_name: string
  value_range: ValueRange
  color_stops?: ColorStop[]
}

export interface UIManifest {
  schema_version: number
  model: string
  run_id: string
  cycle_time: string
  published_at: string | null
  resolution_km: number
  layers: LayerConfig[]
  forecast_hours: number[]
  tile_url_template: string
  /** Range each data-tiled layer was encoded with, by layer; absent before #83. */
  data_ranges?: DataRanges
}

/** Encoding range of a run's data tiles, by layer name. */
export type DataRanges = Record<string, ValueRange>

export interface ValueRange {
  min: number
  max: number
}

export interface LayerConfig {
  id: string
  display_name: string
  unit: string
  palette_name: string
  value_range: ValueRange
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
}

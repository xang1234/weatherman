import { useMemo } from 'react'
import type { CSSProperties } from 'react'
import type { VoyageCorridorState } from '@/hooks/useVoyageCorridor'
import type { VoyageRouteState } from '@/hooks/useVoyageRoute'
import { valueToColor } from '@/utils/color-scale'
import type { LayerConfig } from '@/types/manifest'

export interface VoyageWeatherPanelProps {
  corridor: VoyageCorridorState
  route: VoyageRouteState
  forecastHour: number | null
  layers: LayerConfig[]
  onClose: () => void
}

export function VoyageWeatherPanel({
  corridor,
  route,
  forecastHour,
  layers,
  onClose,
}: VoyageWeatherPanelProps) {
  if (!route.lineString) return null

  const { data, loading, error, selectedVariable, setSelectedVariable } = corridor
  const ranges = data?.ranges ?? {}
  const parameters = data?.parameters ?? {}
  const variableKeys = Object.keys(ranges)
  const times = data?.domain.axes.t.values ?? []
  const distances = data?.route.distances_nm ?? []
  const totalNm = data?.route.total_nm ?? 0

  const activeVar = selectedVariable && ranges[selectedVariable]
    ? selectedVariable
    : variableKeys[0] ?? null

  // Find matching layer config for color stops + value range
  const layerConfig = useMemo(() => {
    if (!activeVar) return null
    // Try direct match or common mappings
    const varToLayer: Record<string, string> = {
      tmp_2m: 'temperature',
      htsgw_sfc: 'wave_height',
      ugrd_10m: 'wind_u',
      vgrd_10m: 'wind_v',
      perpw_sfc: 'wave_period',
    }
    const layerId = varToLayer[activeVar] ?? activeVar
    return layers.find((l) => l.id === layerId) ?? null
  }, [activeVar, layers])

  const colorStops = layerConfig?.color_stops ?? []
  const vMin = layerConfig?.value_range.min ?? 0
  const vMax = layerConfig?.value_range.max ?? 1

  return (
    <div style={panelStyle}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
        <div>
          <div style={{ fontSize: 12, color: '#8b949e', textTransform: 'uppercase' }}>
            Voyage Weather
          </div>
          <div style={{ marginTop: 4, fontSize: 12, color: '#c9d1d9' }}>
            {totalNm > 0 ? `${totalNm.toFixed(0)} nm` : ''} &middot; {route.waypoints.length} waypoints
          </div>
        </div>
        <button type="button" onClick={onClose} style={closeButtonStyle}>Close</button>
      </div>

      {loading && <div style={{ marginTop: 12 }}>Loading corridor data...</div>}
      {error && <div style={{ marginTop: 12, color: '#f85149' }}>{error}</div>}

      {!loading && !error && activeVar && data && (
        <>
          {/* Variable selector pills */}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 14, marginBottom: 12 }}>
            {variableKeys.map((v) => {
              const isActive = v === activeVar
              const label = parameters[v]?.observedProperty?.label?.en ?? v
              return (
                <button
                  key={v}
                  type="button"
                  onClick={() => setSelectedVariable(v)}
                  style={{
                    border: '1px solid rgba(48, 54, 61, 0.6)',
                    background: isActive ? 'rgba(56, 139, 253, 0.18)' : 'transparent',
                    color: isActive ? '#58a6ff' : '#c9d1d9',
                    borderRadius: 999,
                    padding: '4px 10px',
                    cursor: 'pointer',
                    fontSize: 12,
                  }}
                >
                  {label}
                </button>
              )
            })}
          </div>

          {/* SVG Heatmap */}
          <Heatmap
            values={ranges[activeVar].values}
            times={times}
            distances={distances}
            colorStops={colorStops}
            vMin={vMin}
            vMax={vMax}
            forecastHour={forecastHour}
            unit={parameters[activeVar]?.unit?.symbol ?? ''}
          />
        </>
      )}
    </div>
  )
}

function Heatmap({
  values,
  times,
  distances,
  colorStops,
  vMin,
  vMax,
  forecastHour,
  unit,
}: {
  values: Array<Array<number | null>>
  times: number[]
  distances: number[]
  colorStops: { position: number; color: [number, number, number] }[]
  vMin: number
  vMax: number
  forecastHour: number | null
  unit: string
}) {
  const nSamples = values.length
  const nTimes = times.length

  // useMemo must be called before any early returns (React hooks rules)
  const colorGrid = useMemo(() =>
    values.map((row) => row.map((val) => valueToColor(val, vMin, vMax, colorStops))),
    [values, vMin, vMax, colorStops],
  )

  if (nSamples === 0 || nTimes === 0) return null

  const padL = 40
  const padR = 8
  const padT = 20
  const padB = 30
  const cellW = Math.max(6, Math.min(12, 280 / nSamples))
  const cellH = Math.max(8, Math.min(16, 200 / nTimes))
  const gridW = nSamples * cellW
  const gridH = nTimes * cellH
  const width = padL + gridW + padR
  const height = padT + gridH + padB

  const highlightedTimeIdx = forecastHour != null ? times.indexOf(forecastHour) : -1

  const totalDist = distances[distances.length - 1] ?? 0
  const distTicks = [0, 0.25, 0.5, 0.75, 1].map((f) => ({
    x: padL + f * gridW,
    label: `${Math.round(f * totalDist)}`,
  }))

  const timeTicks = times
    .map((h, i) => ({ h, i }))
    .filter(({ i }) => i % 2 === 0 || i === nTimes - 1)
    .map(({ h, i }) => ({
      y: padT + i * cellH + cellH / 2,
      label: `F${String(h).padStart(3, '0')}`,
    }))

  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      style={{ width: '100%', height: 'auto', borderRadius: 8, background: 'rgba(22, 27, 34, 0.9)' }}
    >
      {/* Heatmap cells */}
      {colorGrid.map((row, sIdx) =>
        row.map((fill, tIdx) => (
          <rect
            key={`${sIdx}-${tIdx}`}
            x={padL + sIdx * cellW}
            y={padT + tIdx * cellH}
            width={cellW}
            height={cellH}
            fill={fill}
          />
        )),
      )}

      {/* Forecast hour highlight line */}
      {highlightedTimeIdx >= 0 && (
        <line
          x1={padL}
          x2={padL + gridW}
          y1={padT + highlightedTimeIdx * cellH + cellH / 2}
          y2={padT + highlightedTimeIdx * cellH + cellH / 2}
          stroke="#f2cc60"
          strokeWidth={2}
          strokeOpacity={0.8}
        />
      )}

      {/* X axis: distance */}
      {distTicks.map((tick) => (
        <text
          key={tick.label}
          x={tick.x}
          y={padT + gridH + 16}
          textAnchor="middle"
          fill="#8b949e"
          fontSize={9}
        >
          {tick.label}
        </text>
      ))}
      <text
        x={padL + gridW / 2}
        y={padT + gridH + 28}
        textAnchor="middle"
        fill="#8b949e"
        fontSize={8}
      >
        nm
      </text>

      {/* Y axis: forecast hours */}
      {timeTicks.map((tick) => (
        <text
          key={tick.label}
          x={padL - 4}
          y={tick.y + 3}
          textAnchor="end"
          fill="#8b949e"
          fontSize={9}
        >
          {tick.label}
        </text>
      ))}

      {/* Legend: min/max + unit */}
      <text x={padL} y={padT - 6} fill="#8b949e" fontSize={8}>
        {vMin.toFixed(1)} – {vMax.toFixed(1)} {unit}
      </text>
    </svg>
  )
}

const panelStyle: CSSProperties = {
  position: 'absolute',
  right: 12,
  top: 12,
  zIndex: 10,
  width: 'min(400px, calc(100vw - 24px))',
  maxHeight: 'calc(100vh - 120px)',
  overflowY: 'auto',
  padding: 14,
  borderRadius: 12,
  border: '1px solid rgba(48, 54, 61, 0.6)',
  background: 'rgba(13, 17, 23, 0.92)',
  backdropFilter: 'blur(8px)',
  color: '#e6edf3',
}

const closeButtonStyle: CSSProperties = {
  border: '1px solid rgba(48, 54, 61, 0.6)',
  background: 'transparent',
  color: '#c9d1d9',
  borderRadius: 8,
  padding: '4px 8px',
  cursor: 'pointer',
}

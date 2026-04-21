import type { CSSProperties } from 'react'
import type { HoverProbeState } from '@/hooks/useHoverProbe'
import type { CoverageParameter } from '@/types/edr'

export interface WeatherHoverHudProps {
  probe: HoverProbeState
  forecastHour: number | null
  /** Preferred variable to show first + emphasized (the currently active map layer). */
  activeVariable: string | null
}

/**
 * Cursor-following readout of weather values at the hovered point for the
 * current forecast hour. Ambient exploration — pair with the click-based
 * WeatherInspector for drill-down. Never renders on top of the map-center
 * controls; positions on the opposite side of the cursor from the edge.
 */
export function WeatherHoverHud({
  probe,
  forecastHour,
  activeVariable,
}: WeatherHoverHudProps) {
  const { point, data, loading } = probe
  if (!point) return null

  const ranges = data?.ranges ?? {}
  const parameters = data?.parameters ?? {}
  const times = data?.domain.axes.t.values ?? []
  const hourIndex = forecastHour != null ? times.indexOf(forecastHour) : -1

  const orderedVariables = orderVariables(Object.keys(ranges), activeVariable)

  const style = positionStyle(point.screen)

  return (
    <div style={style} data-testid="weather-hover-hud">
      <div style={coordStyle}>
        {point.lngLat.lat.toFixed(2)}°, {point.lngLat.lng.toFixed(2)}°
        {forecastHour != null && (
          <span style={hourStyle}>F{forecastHour.toString().padStart(3, '0')}</span>
        )}
      </div>

      {loading && orderedVariables.length === 0 && (
        <div style={emptyStyle}>Sampling…</div>
      )}

      {orderedVariables.length > 0 && (
        <div style={valuesContainerStyle}>
          {orderedVariables.map((variable, idx) => {
            const value = hourIndex >= 0 ? ranges[variable]?.values[hourIndex] : null
            const param = parameters[variable]
            const emphasized = variable === activeVariable || (idx === 0 && !activeVariable)
            return (
              <div key={variable} style={valueRowStyle(emphasized)}>
                <span style={labelStyle}>{parameterLabel(variable, param)}</span>
                <span style={valueStyle(emphasized)}>{formatValue(value, param)}</span>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

function orderVariables(keys: string[], active: string | null): string[] {
  if (!active) return keys
  const idx = keys.indexOf(active)
  if (idx < 0) return keys
  return [keys[idx], ...keys.slice(0, idx), ...keys.slice(idx + 1)]
}

function parameterLabel(variable: string, parameter?: CoverageParameter): string {
  return parameter?.observedProperty?.label?.en ?? variable
}

function formatValue(
  value: number | null | undefined,
  parameter?: CoverageParameter,
): string {
  if (value == null) return '—'
  const unit = parameter?.unit?.symbol
  return `${value.toFixed(1)}${unit ? ` ${unit}` : ''}`
}

/**
 * Position the bubble offset from the cursor, flipping across the cursor
 * when near the viewport edge so it never clips off-screen.
 */
function positionStyle(screen: { x: number; y: number }): CSSProperties {
  const offset = 16
  const approxWidth = 220
  const approxHeight = 140
  const viewportWidth = typeof window !== 'undefined' ? window.innerWidth : 1920
  const viewportHeight = typeof window !== 'undefined' ? window.innerHeight : 1080

  const flipX = screen.x + offset + approxWidth > viewportWidth
  const flipY = screen.y + offset + approxHeight > viewportHeight

  const left = flipX ? screen.x - offset - approxWidth : screen.x + offset
  const top = flipY ? screen.y - offset - approxHeight : screen.y + offset

  return {
    position: 'absolute',
    left,
    top,
    zIndex: 11,
    pointerEvents: 'none',
    minWidth: 180,
    maxWidth: 260,
    padding: '8px 10px',
    borderRadius: 10,
    border: '1px solid rgba(48, 54, 61, 0.6)',
    background: 'rgba(13, 17, 23, 0.88)',
    backdropFilter: 'blur(6px)',
    color: '#e6edf3',
    fontSize: 12,
    lineHeight: 1.35,
    fontVariantNumeric: 'tabular-nums',
    boxShadow: '0 4px 16px rgba(0, 0, 0, 0.35)',
  }
}

const coordStyle: CSSProperties = {
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'baseline',
  gap: 8,
  color: '#8b949e',
  fontSize: 11,
  marginBottom: 4,
  textTransform: 'uppercase',
  letterSpacing: 0.3,
}

const hourStyle: CSSProperties = {
  color: '#58a6ff',
  fontWeight: 600,
}

const valuesContainerStyle: CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
}

const emptyStyle: CSSProperties = {
  color: '#8b949e',
  fontStyle: 'italic',
}

const labelStyle: CSSProperties = {
  color: '#8b949e',
  marginRight: 8,
}

function valueRowStyle(emphasized: boolean): CSSProperties {
  return {
    display: 'flex',
    justifyContent: 'space-between',
    gap: 8,
    paddingTop: emphasized ? 2 : 0,
    paddingBottom: emphasized ? 4 : 0,
    borderBottom: emphasized ? '1px solid rgba(88, 166, 255, 0.25)' : undefined,
    marginBottom: emphasized ? 2 : 0,
  }
}

function valueStyle(emphasized: boolean): CSSProperties {
  return {
    color: emphasized ? '#f2cc60' : '#e6edf3',
    fontWeight: emphasized ? 600 : 400,
  }
}

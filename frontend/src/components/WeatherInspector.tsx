import type { CSSProperties } from 'react'
import type { WeatherInspectorState } from '@/hooks/useWeatherInspector'
import type { CoverageParameter } from '@/types/edr'

export interface WeatherInspectorProps {
  inspector: WeatherInspectorState
  forecastHour: number | null
}

export function WeatherInspector({ inspector, forecastHour }: WeatherInspectorProps) {
  if (!inspector.point) return null

  const times = inspector.data?.domain.axes.t.values ?? []
  const parameters = inspector.data?.parameters ?? {}
  const ranges = inspector.data?.ranges ?? {}
  const variableKeys = Object.keys(ranges)
  const selectedVariable = (
    inspector.selectedVariable && ranges[inspector.selectedVariable]
      ? inspector.selectedVariable
      : variableKeys[0]
  ) ?? null
  const selectedValues = selectedVariable ? ranges[selectedVariable].values : []
  const hourIndex = forecastHour != null ? times.indexOf(forecastHour) : -1

  return (
    <div
      style={{
        position: 'absolute',
        left: 12,
        top: 12,
        zIndex: 10,
        width: 'min(360px, calc(100vw - 24px))',
        maxHeight: 'calc(100vh - 120px)',
        overflowY: 'auto',
        padding: 14,
        borderRadius: 12,
        border: '1px solid rgba(48, 54, 61, 0.6)',
        background: 'rgba(13, 17, 23, 0.92)',
        backdropFilter: 'blur(8px)',
        color: '#e6edf3',
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
        <div>
          <div style={{ fontSize: 12, color: '#8b949e', textTransform: 'uppercase' }}>
            Weather Inspector
          </div>
          <div style={{ marginTop: 4, fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>
            {inspector.point.lat.toFixed(4)}, {inspector.point.lon.toFixed(4)}
          </div>
        </div>
        <button type="button" onClick={inspector.clear} style={closeButtonStyle}>
          Close
        </button>
      </div>

      {inspector.loading && <div style={{ marginTop: 12 }}>Loading point forecast...</div>}
      {inspector.error && (
        <div style={{ marginTop: 12, color: '#f85149' }}>{inspector.error}</div>
      )}

      {!inspector.loading && !inspector.error && selectedVariable && (
        <>
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: 6,
              marginTop: 14,
              marginBottom: 12,
            }}
          >
            {variableKeys.map((variable) => {
              const isActive = variable === selectedVariable
              return (
                <button
                  key={variable}
                  type="button"
                  onClick={() => inspector.setSelectedVariable(variable)}
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
                  {parameterLabel(variable, parameters[variable])}
                </button>
              )
            })}
          </div>

          <div style={{ fontSize: 12, color: '#8b949e', marginBottom: 8 }}>
            {parameterLabel(selectedVariable, parameters[selectedVariable])}
          </div>
          <MiniChart
            hours={times}
            values={selectedValues}
            highlightedHour={forecastHour}
          />

          {hourIndex >= 0 && (
            <div style={{ marginTop: 12 }}>
              <div style={{ fontSize: 12, color: '#8b949e', marginBottom: 6 }}>
                Current values at F{forecastHour?.toString().padStart(3, '0')}
              </div>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                <tbody>
                  {variableKeys.map((variable) => (
                    <tr key={variable}>
                      <td style={cellLabelStyle}>
                        {parameterLabel(variable, parameters[variable])}
                      </td>
                      <td style={cellValueStyle}>
                        {formatValue(ranges[variable].values[hourIndex], parameters[variable])}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  )
}

function parameterLabel(variable: string, parameter?: CoverageParameter) {
  return parameter?.observedProperty?.label?.en ?? variable
}

function formatValue(value: number | null | undefined, parameter?: CoverageParameter) {
  if (value == null) return 'N/A'
  const unit = parameter?.unit?.symbol
  return `${value.toFixed(2)}${unit ? ` ${unit}` : ''}`
}

function MiniChart({
  hours,
  values,
  highlightedHour,
}: {
  hours: number[]
  values: Array<number | null>
  highlightedHour: number | null
}) {
  const numericValues = values.filter((value): value is number => value != null)
  if (hours.length === 0 || numericValues.length === 0) {
    return <div style={{ fontSize: 12, color: '#8b949e' }}>No time-series values.</div>
  }

  const width = 320
  const height = 120
  const padX = 14
  const padY = 12
  const min = Math.min(...numericValues)
  const max = Math.max(...numericValues)
  const span = max - min || 1
  const xFor = (index: number) => {
    if (hours.length === 1) return width / 2
    return padX + (index / (hours.length - 1)) * (width - padX * 2)
  }
  const yFor = (value: number) =>
    height - padY - ((value - min) / span) * (height - padY * 2)

  const points = values
    .map((value, index) => (value == null ? null : `${xFor(index)},${yFor(value)}`))
    .filter((point): point is string => point !== null)
    .join(' ')

  const highlightedIndex = highlightedHour != null ? hours.indexOf(highlightedHour) : -1

  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      style={{
        width: '100%',
        height: 'auto',
        borderRadius: 8,
        background: 'rgba(22, 27, 34, 0.9)',
      }}
    >
      <line x1={padX} y1={height - padY} x2={width - padX} y2={height - padY} stroke="#30363d" />
      <line x1={padX} y1={padY} x2={padX} y2={height - padY} stroke="#30363d" />
      <polyline
        fill="none"
        stroke="#58a6ff"
        strokeWidth="2"
        points={points}
      />
      {highlightedIndex >= 0 && values[highlightedIndex] != null && (
        <>
          <line
            x1={xFor(highlightedIndex)}
            y1={padY}
            x2={xFor(highlightedIndex)}
            y2={height - padY}
            stroke="rgba(88, 166, 255, 0.35)"
            strokeDasharray="4 4"
          />
          <circle
            cx={xFor(highlightedIndex)}
            cy={yFor(values[highlightedIndex] as number)}
            r="4"
            fill="#f2cc60"
          />
        </>
      )}
    </svg>
  )
}

const closeButtonStyle: CSSProperties = {
  border: '1px solid rgba(48, 54, 61, 0.6)',
  background: 'transparent',
  color: '#c9d1d9',
  borderRadius: 8,
  padding: '4px 8px',
  cursor: 'pointer',
}

const cellLabelStyle: CSSProperties = {
  color: '#8b949e',
  padding: '4px 8px 4px 0',
  verticalAlign: 'top',
}

const cellValueStyle: CSSProperties = {
  color: '#e6edf3',
  padding: '4px 0',
  textAlign: 'right',
  fontVariantNumeric: 'tabular-nums',
}

import type { CSSProperties } from 'react'

export interface ForecastControlsProps {
  forecastHours: number[]
  forecastHour: number | null
  isPlaying: boolean
  onChange: (forecastHour: number) => void
  onTogglePlay: () => void
}

export function ForecastControls({
  forecastHours,
  forecastHour,
  isPlaying,
  onChange,
  onTogglePlay,
}: ForecastControlsProps) {
  if (forecastHours.length === 0 || forecastHour == null) return null

  const index = Math.max(0, forecastHours.indexOf(forecastHour))

  return (
    <div
      style={{
        position: 'absolute',
        left: '50%',
        bottom: 20,
        transform: 'translateX(-50%)',
        zIndex: 10,
        minWidth: 360,
        maxWidth: 'min(92vw, 720px)',
        padding: '12px 14px',
        borderRadius: 12,
        border: '1px solid rgba(48, 54, 61, 0.6)',
        background: 'rgba(13, 17, 23, 0.9)',
        backdropFilter: 'blur(8px)',
        color: '#e6edf3',
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 12,
          marginBottom: 8,
        }}
      >
        <div style={{ fontSize: 12, color: '#8b949e', textTransform: 'uppercase' }}>
          Forecast Hour
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <button
            type="button"
            onClick={onTogglePlay}
            style={buttonStyle}
          >
            {isPlaying ? 'Pause' : 'Play'}
          </button>
          <div style={{ fontVariantNumeric: 'tabular-nums', fontWeight: 600 }}>
            F{forecastHour.toString().padStart(3, '0')}
          </div>
        </div>
      </div>
      <input
        type="range"
        min={0}
        max={forecastHours.length - 1}
        step={1}
        value={index}
        onChange={(e) => onChange(forecastHours[Number(e.target.value)])}
        style={{ width: '100%' }}
      />
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          marginTop: 6,
          fontSize: 11,
          color: '#8b949e',
          fontVariantNumeric: 'tabular-nums',
        }}
      >
        <span>F{forecastHours[0].toString().padStart(3, '0')}</span>
        <span>F{forecastHours[forecastHours.length - 1].toString().padStart(3, '0')}</span>
      </div>
    </div>
  )
}

const buttonStyle: CSSProperties = {
  border: '1px solid rgba(88, 166, 255, 0.4)',
  background: 'rgba(56, 139, 253, 0.16)',
  color: '#c9d1d9',
  borderRadius: 8,
  padding: '5px 10px',
  cursor: 'pointer',
}

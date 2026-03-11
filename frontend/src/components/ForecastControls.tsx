import type { CSSProperties } from 'react'

export interface ForecastControlsProps {
  cycleTime: string | null
  forecastHours: number[]
  forecastHour: number | null
  isPlaying: boolean
  onChange: (forecastHour: number) => void
  onTogglePlay: () => void
}

export function ForecastControls({
  cycleTime,
  forecastHours,
  forecastHour,
  isPlaying,
  onChange,
  onTogglePlay,
}: ForecastControlsProps) {
  if (forecastHours.length === 0 || forecastHour == null) return null

  const index = Math.max(0, forecastHours.indexOf(forecastHour))
  const atStart = index <= 0
  const atEnd = index >= forecastHours.length - 1
  const currentLabel = formatForecastDateTime(cycleTime, forecastHour)

  const stepBack = () => {
    if (!atStart) onChange(forecastHours[index - 1])
  }
  const stepForward = () => {
    if (!atEnd) onChange(forecastHours[index + 1])
  }

  return (
    <div style={barStyle}>
      <span style={timeLabelStyle}>{currentLabel}</span>
      <button
        type="button"
        onClick={stepBack}
        disabled={atStart}
        style={{ ...stepBtnStyle, opacity: atStart ? 0.35 : 1 }}
      >
        &#x276E;
      </button>
      <button type="button" onClick={onTogglePlay} style={playBtnStyle}>
        {isPlaying ? '\u23F8' : '\u25B6'}
      </button>
      <button
        type="button"
        onClick={stepForward}
        disabled={atEnd}
        style={{ ...stepBtnStyle, opacity: atEnd ? 0.35 : 1 }}
      >
        &#x276F;
      </button>
      <input
        type="range"
        min={0}
        max={forecastHours.length - 1}
        step={1}
        value={index}
        onChange={(e) => onChange(forecastHours[Number(e.target.value)])}
        style={{ flex: 1, minWidth: 80, accentColor: '#58a6ff' }}
      />
    </div>
  )
}

const FORECAST_DATE_FORMATTER = new Intl.DateTimeFormat(undefined, {
  weekday: 'short',
  month: 'short',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
  timeZone: 'UTC',
})

function formatForecastDateTime(
  cycleTime: string | null,
  forecastHour: number,
): string {
  const cycleDate = cycleTime ? new Date(cycleTime) : null
  if (!cycleDate || Number.isNaN(cycleDate.getTime())) {
    return `F${forecastHour.toString().padStart(3, '0')}`
  }
  const validDate = new Date(cycleDate.getTime() + forecastHour * 60 * 60 * 1000)
  return FORECAST_DATE_FORMATTER.format(validDate)
}

const barStyle: CSSProperties = {
  position: 'absolute',
  left: '50%',
  bottom: 20,
  transform: 'translateX(-50%)',
  zIndex: 10,
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  maxWidth: 'min(92vw, 520px)',
  padding: '6px 14px',
  borderRadius: 10,
  border: '1px solid rgba(48, 54, 61, 0.6)',
  background: 'rgba(13, 17, 23, 0.9)',
  backdropFilter: 'blur(8px)',
  color: '#e6edf3',
  fontFamily: 'system-ui, -apple-system, sans-serif',
}

const timeLabelStyle: CSSProperties = {
  fontSize: 12,
  fontWeight: 600,
  fontVariantNumeric: 'tabular-nums',
  whiteSpace: 'nowrap',
}

const playBtnStyle: CSSProperties = {
  border: '1px solid rgba(88, 166, 255, 0.4)',
  background: 'rgba(56, 139, 253, 0.16)',
  color: '#c9d1d9',
  borderRadius: 8,
  padding: '4px 10px',
  cursor: 'pointer',
  fontSize: 14,
  lineHeight: 1,
}

const stepBtnStyle: CSSProperties = {
  border: '1px solid rgba(48, 54, 61, 0.6)',
  background: 'transparent',
  color: '#c9d1d9',
  borderRadius: 6,
  padding: '4px 7px',
  cursor: 'pointer',
  fontSize: 11,
  lineHeight: 1,
}

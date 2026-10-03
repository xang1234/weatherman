import { useEffect, useRef, useState, type CSSProperties } from 'react'
import { formatForecastDateTime } from '@/utils/format'

export interface ForecastControlsProps {
  cycleTime: string | null
  forecastHours: number[]
  forecastHour: number | null
  isPlaying: boolean
  onChange: (forecastHour: number) => void
  onTogglePlay: () => void
  /**
   * Called while dragging with the slider's position, a fractional index
   * into forecastHours — at most once per animation frame — and with null
   * on release.
   */
  onScrub: (position: number | null) => void
}

export function ForecastControls({
  cycleTime,
  forecastHours,
  forecastHour,
  isPlaying,
  onChange,
  onTogglePlay,
  onScrub,
}: ForecastControlsProps) {
  const dragging = useRef(false)
  // The live drag position is this component's alone: each input re-renders
  // the slider and its label, and the map hears of it once a frame (#94).
  const [scrubPosition, setScrubPosition] = useState<number | null>(null)
  const frame = useRef(0)
  const latest = useRef(0)
  useEffect(() => () => cancelAnimationFrame(frame.current), [])
  if (forecastHours.length === 0 || forecastHour == null) return null

  const index = Math.max(0, forecastHours.indexOf(forecastHour))
  const atStart = index <= 0
  const atEnd = index >= forecastHours.length - 1
  const currentLabel = formatForecastDateTime(cycleTime, hourAt(forecastHours, scrubPosition ?? index))

  /** Release: settle on the nearest forecast hour. */
  const commit = (position: number) => {
    dragging.current = false
    cancelAnimationFrame(frame.current)
    frame.current = 0
    setScrubPosition(null)
    onScrub(null)
    onChange(forecastHours[Math.round(position)])
  }

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
        aria-label="Forecast hour"
        min={0}
        max={forecastHours.length - 1}
        // Fine steps while dragging, so the map can blend between hours (#24).
        step="any"
        value={scrubPosition ?? index}
        onPointerDown={() => { dragging.current = true }}
        onPointerUp={(e) => commit(Number(e.currentTarget.value))}
        onPointerCancel={(e) => commit(Number(e.currentTarget.value))}
        onChange={(e) => {
          const position = Number(e.target.value)
          if (!dragging.current) {
            commit(position) // set without a pointer: keyboard, or a test
            return
          }
          setScrubPosition(position)
          latest.current = position
          if (!frame.current) {
            frame.current = requestAnimationFrame(() => {
              frame.current = 0
              onScrub(latest.current)
            })
          }
        }}
        onKeyDown={(e) => {
          // Arrow keys step whole hours rather than the fine drag step.
          const step = { ArrowLeft: -1, ArrowDown: -1, ArrowRight: 1, ArrowUp: 1 }[e.key]
          if (step == null) return
          e.preventDefault()
          const next = Math.min(forecastHours.length - 1, Math.max(0, index + step))
          if (next !== index) onChange(forecastHours[next])
        }}
        style={{ flex: 1, minWidth: 80, accentColor: '#58a6ff' }}
      />
    </div>
  )
}

/** Forecast hour at a fractional index, interpolated between neighbours. */
function hourAt(hours: number[], position: number): number {
  const i = Math.min(Math.floor(position), hours.length - 1)
  const next = hours[i + 1]
  return next == null ? hours[i] : hours[i] + (position - i) * (next - hours[i])
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

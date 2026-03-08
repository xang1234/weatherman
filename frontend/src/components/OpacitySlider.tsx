export interface OpacitySliderProps {
  value: number
  onChange: (value: number) => void
}

export function OpacitySlider({ value, onChange }: OpacitySliderProps) {
  return (
    <div
      style={{
        position: 'absolute',
        bottom: 30,
        left: 10,
        zIndex: 10,
        background: 'rgba(13, 17, 23, 0.85)',
        backdropFilter: 'blur(8px)',
        border: '1px solid rgba(48, 54, 61, 0.6)',
        borderRadius: 8,
        padding: '8px 12px',
        color: '#e6edf3',
        fontSize: 12,
        fontFamily: 'system-ui, -apple-system, sans-serif',
        userSelect: 'none',
        display: 'flex',
        alignItems: 'center',
        gap: 8,
      }}
    >
      <label htmlFor="wx-opacity" style={{ whiteSpace: 'nowrap' }}>
        Overlay
      </label>
      <input
        id="wx-opacity"
        type="range"
        min={0}
        max={100}
        value={Math.round(value * 100)}
        onChange={(e) => onChange(Number(e.target.value) / 100)}
        style={{ width: 80, accentColor: '#58a6ff' }}
      />
      <span style={{ color: '#8b949e', minWidth: 30, textAlign: 'right' }}>
        {Math.round(value * 100)}%
      </span>
    </div>
  )
}

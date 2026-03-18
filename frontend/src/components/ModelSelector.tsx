const MODELS = [
  { id: 'gfs', label: 'GFS', description: 'Deterministic' },
  { id: 'gefs', label: 'GEFS', description: 'Ensemble Mean' },
] as const

export type ModelId = (typeof MODELS)[number]['id']

export interface ModelSelectorProps {
  model: ModelId
  onChange: (model: ModelId) => void
}

export function ModelSelector({ model, onChange }: ModelSelectorProps) {
  return (
    <div
      style={{
        position: 'absolute',
        top: 10,
        right: 240,
        zIndex: 10,
        display: 'flex',
        background: 'rgba(13, 17, 23, 0.85)',
        backdropFilter: 'blur(8px)',
        border: '1px solid rgba(48, 54, 61, 0.6)',
        borderRadius: 8,
        padding: 3,
        fontFamily: 'system-ui, -apple-system, sans-serif',
        userSelect: 'none',
      }}
    >
      {MODELS.map(({ id, label, description }) => {
        const isActive = model === id
        return (
          <button
            key={id}
            onClick={() => onChange(id)}
            title={description}
            style={{
              padding: '4px 10px',
              border: 'none',
              borderRadius: 6,
              background: isActive ? 'rgba(56, 139, 253, 0.15)' : 'transparent',
              color: isActive ? '#58a6ff' : '#8b949e',
              cursor: 'pointer',
              fontSize: 12,
              fontWeight: isActive ? 600 : 400,
              fontFamily: 'inherit',
              lineHeight: '18px',
            }}
          >
            {label}
          </button>
        )
      })}
    </div>
  )
}

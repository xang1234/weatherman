import type { LayerConfig } from '@/types/manifest'

export interface LayerSelectorProps {
  layers: LayerConfig[]
  activeLayerId: string | null
  onSelect: (layerId: string) => void
}

function formatRange(min: number, max: number, unit: string): string {
  return `${min}–${max} ${unit}`
}

export function LayerSelector({ layers, activeLayerId, onSelect }: LayerSelectorProps) {
  if (layers.length === 0) return null

  return (
    <div
      style={{
        position: 'absolute',
        top: 10,
        right: 10,
        zIndex: 10,
        background: 'rgba(13, 17, 23, 0.85)',
        backdropFilter: 'blur(8px)',
        border: '1px solid rgba(48, 54, 61, 0.6)',
        borderRadius: 8,
        padding: '8px 0',
        color: '#e6edf3',
        fontSize: 13,
        fontFamily: 'system-ui, -apple-system, sans-serif',
        userSelect: 'none',
        minWidth: 180,
        maxHeight: 'calc(100vh - 40px)',
        overflowY: 'auto',
      }}
    >
      <div
        style={{
          padding: '2px 12px 6px',
          fontSize: 11,
          color: '#8b949e',
          textTransform: 'uppercase',
          letterSpacing: '0.05em',
          borderBottom: '1px solid rgba(48, 54, 61, 0.6)',
          marginBottom: 4,
        }}
      >
        Layers
      </div>
      {layers.map((layer) => {
        const isActive = layer.id === activeLayerId
        return (
          <button
            key={layer.id}
            onClick={() => onSelect(layer.id)}
            style={{
              display: 'flex',
              flexDirection: 'column',
              gap: 2,
              width: '100%',
              padding: '6px 12px',
              border: 'none',
              background: isActive ? 'rgba(56, 139, 253, 0.15)' : 'transparent',
              color: isActive ? '#58a6ff' : '#e6edf3',
              cursor: 'pointer',
              textAlign: 'left',
              fontSize: 13,
              fontFamily: 'inherit',
            }}
          >
            <span style={{ fontWeight: isActive ? 600 : 400 }}>
              {layer.display_name}
            </span>
            <span style={{ fontSize: 11, color: '#8b949e' }}>
              {formatRange(layer.value_range.min, layer.value_range.max, layer.unit)}
            </span>
          </button>
        )
      })}
    </div>
  )
}

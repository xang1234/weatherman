import { useMemo } from 'react'
import type { LayerConfig } from '@/types/manifest'

export interface LayerPanelProps {
  layers: LayerConfig[]
  activeLayerId: string | null
  onSelect: (layerId: string) => void
}

function formatRange(min: number, max: number, unit: string): string {
  return `${min}–${max} ${unit}`
}

export function LayerPanel({ layers, activeLayerId, onSelect }: LayerPanelProps) {
  if (layers.length === 0) return null

  const activeLayer = layers.find((l) => l.id === activeLayerId) ?? null
  const hasLegend = !!activeLayer?.color_stops && activeLayer.color_stops.length >= 2

  return (
    <div
      style={{
        position: 'absolute',
        top: 10,
        right: 10,
        zIndex: 10,
        width: 220,
        maxHeight: 'calc(100vh - 40px)',
        overflowY: 'auto',
        background: 'rgba(13, 17, 23, 0.85)',
        backdropFilter: 'blur(8px)',
        border: '1px solid rgba(48, 54, 61, 0.6)',
        borderRadius: 8,
        padding: '8px 0',
        color: '#e6edf3',
        fontSize: 13,
        fontFamily: 'system-ui, -apple-system, sans-serif',
        userSelect: 'none',
      }}
    >
      {/* Header */}
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

      {/* Layer buttons */}
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

      {/* Legend section */}
      {hasLegend && activeLayer && (
        <LegendSection layer={activeLayer} />
      )}
    </div>
  )
}

function LegendSection({ layer }: { layer: LayerConfig }) {
  const { color_stops, value_range, unit } = layer
  const { min, max } = value_range

  const gradient = useMemo(() => {
    const stops = color_stops!.map(
      (s) => `rgb(${s.color[0]}, ${s.color[1]}, ${s.color[2]}) ${(s.position * 100).toFixed(1)}%`,
    )
    return `linear-gradient(to right, ${stops.join(', ')})`
  }, [color_stops])

  const ticks = useMemo(() => {
    const count = 5
    const result: { position: number; label: string }[] = []
    for (let i = 0; i < count; i++) {
      const t = i / (count - 1)
      result.push({
        position: t * 100,
        label: `${Math.round(min + t * (max - min))}`,
      })
    }
    return result
  }, [min, max])

  return (
    <>
      <div
        style={{
          height: 1,
          background: 'rgba(48, 54, 61, 0.6)',
          margin: '8px 0',
        }}
      />
      <div style={{ padding: '0 12px 6px' }}>
        <div
          style={{
            height: 10,
            borderRadius: 3,
            background: gradient,
          }}
        />
        <div style={{ position: 'relative', height: 18, marginTop: 2 }}>
          {ticks.map((tick) => (
            <span
              key={tick.position}
              style={{
                position: 'absolute',
                left: `${tick.position}%`,
                transform: 'translateX(-50%)',
                fontSize: 9,
                color: '#8b949e',
                fontVariantNumeric: 'tabular-nums',
              }}
            >
              {tick.label}
            </span>
          ))}
        </div>
        <div style={{ textAlign: 'right', fontSize: 9, color: '#8b949e', marginTop: -2 }}>
          {unit}
        </div>
      </div>
    </>
  )
}

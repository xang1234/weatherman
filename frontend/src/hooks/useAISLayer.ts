import { useEffect, useRef } from 'react'
import type maplibregl from 'maplibre-gl'

export interface UseAISLayerOptions {
  map: React.RefObject<maplibregl.Map | null>
  isLoaded: boolean
  /** Snapshot date in YYYY-MM-DD format, or null to hide layer */
  snapshotDate: string | null
  /** Whether the AIS layer is visible (default true) */
  visible?: boolean
}

/** Bump when tile encoding changes to invalidate browser caches. */
const TILE_VERSION = 2

const SOURCE_ID = 'ais-vessels'
const ARROW_LAYER_ID = 'ais-vessel-arrows'
const CIRCLE_LAYER_ID = 'ais-vessel-circles'
const ARROW_IMAGE = 'vessel-arrow'
const CIRCLE_IMAGE = 'vessel-circle'

/** Heading value 511 means "not available" in AIS spec. */
const HEADING_UNAVAILABLE = 511

// Ship type → color mapping
const SHIPTYPE_COLORS: [string, string][] = [
  ['Bulk Carrier', '#2ecc71'],
  ['General Cargo', '#2ecc71'],
  ['Container Ship', '#2ecc71'],
  ['Cargo', '#2ecc71'],
  ['Oil Tanker', '#e67e22'],
  ['Chemical Tanker', '#e67e22'],
  ['LPG Tanker', '#e67e22'],
  ['LNG Tanker', '#e67e22'],
  ['Tanker', '#e67e22'],
]
const DEFAULT_COLOR = '#95a5a6'

/**
 * Create an arrow chevron pointing UP (0°) as a non-SDF RGBA image.
 * White fill with dark outline for visibility on any background.
 * Color is applied per-vessel via the layer paint's icon-color (requires sdf: true)
 * but sdf: true needs a proper distance field. Instead, we bake white and use
 * icon-color with sdf: false by not tinting — OR we use sdf: true with an
 * all-white RGB image where alpha encodes the shape.
 *
 * MapLibre SDF images: only the alpha channel matters. Alpha > 0.5 = inside shape.
 * The RGB channels are replaced by icon-color. So we draw white everywhere and
 * use alpha to define the shape boundary.
 */
function createArrowImage(size: number): ImageData {
  const canvas = document.createElement('canvas')
  canvas.width = size
  canvas.height = size
  const ctx = canvas.getContext('2d')!

  ctx.clearRect(0, 0, size, size)
  const cx = size / 2

  // Draw a chevron/arrow pointing up — alpha defines the shape
  ctx.fillStyle = '#ffffff'
  ctx.beginPath()
  ctx.moveTo(cx, size * 0.08)                   // tip (top center)
  ctx.lineTo(cx + size * 0.34, size * 0.72)     // right wing
  ctx.lineTo(cx, size * 0.50)                    // tail notch
  ctx.lineTo(cx - size * 0.34, size * 0.72)     // left wing
  ctx.closePath()
  ctx.fill()

  return ctx.getImageData(0, 0, size, size)
}

/** Create a circle as a non-SDF RGBA image. For vessels with unknown heading. */
function createCircleImage(size: number): ImageData {
  const canvas = document.createElement('canvas')
  canvas.width = size
  canvas.height = size
  const ctx = canvas.getContext('2d')!

  ctx.clearRect(0, 0, size, size)
  const cx = size / 2
  const r = size * 0.3

  ctx.fillStyle = '#ffffff'
  ctx.beginPath()
  ctx.arc(cx, cx, r, 0, Math.PI * 2)
  ctx.fill()

  return ctx.getImageData(0, 0, size, size)
}

function addSprites(map: maplibregl.Map): void {
  const size = 64  // hi-res for crisp rendering
  if (!map.hasImage(ARROW_IMAGE)) {
    map.addImage(ARROW_IMAGE, createArrowImage(size), {
      sdf: true,    // enables icon-color tinting; alpha = shape mask
      pixelRatio: 2,
    })
  }
  if (!map.hasImage(CIRCLE_IMAGE)) {
    map.addImage(CIRCLE_IMAGE, createCircleImage(size), {
      sdf: true,
      pixelRatio: 2,
    })
  }
}

/** Build the match expression for icon-color based on shiptype. */
function shiptypeColorExpression(): maplibregl.ExpressionSpecification {
  const expr: unknown[] = ['match', ['get', 'shiptype']]
  for (const [type, color] of SHIPTYPE_COLORS) {
    expr.push(type, color)
  }
  expr.push(DEFAULT_COLOR)
  return expr as maplibregl.ExpressionSpecification
}

export function useAISLayer({
  map,
  isLoaded,
  snapshotDate,
  visible = true,
}: UseAISLayerOptions): void {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const activeDateRef = useRef<string | null>(null)

  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return

    // If no date, remove existing layer
    if (!snapshotDate) {
      cleanup(m)
      activeDateRef.current = null
      return
    }

    // Already showing this date — nothing to do
    if (activeDateRef.current === snapshotDate) return

    // Remove old source/layers before adding new
    cleanup(m)

    // Set ref before addLayer so the visibility effect can find the active date
    activeDateRef.current = snapshotDate

    addSprites(m)

    const tileUrl = `${apiBase}/ais/tiles/${snapshotDate}/{z}/{x}/{y}.pbf?v=${TILE_VERSION}`

    m.addSource(SOURCE_ID, {
      type: 'vector',
      tiles: [tileUrl],
      minzoom: 0,
      maxzoom: 12,
      promoteId: 'imommsi',
    })

    // Layer for vessels WITH heading (directional arrows)
    m.addLayer({
      id: ARROW_LAYER_ID,
      type: 'symbol',
      source: SOURCE_ID,
      'source-layer': 'vessels',
      filter: ['all',
        ['has', 'heading'],
        ['!=', ['get', 'heading'], HEADING_UNAVAILABLE],
      ],
      layout: {
        'icon-image': ARROW_IMAGE,
        'icon-rotate': ['get', 'heading'],
        'icon-rotation-alignment': 'map',
        'icon-allow-overlap': true,
        'icon-size': [
          'interpolate', ['linear'], ['zoom'],
          0, 0.4,
          6, 0.6,
          12, 1.2,
        ],
      },
      paint: {
        'icon-color': shiptypeColorExpression(),
        'icon-opacity': visible ? 1 : 0,
      },
    })

    // Layer for vessels WITHOUT heading (small circles)
    m.addLayer({
      id: CIRCLE_LAYER_ID,
      type: 'symbol',
      source: SOURCE_ID,
      'source-layer': 'vessels',
      filter: ['any',
        ['!', ['has', 'heading']],
        ['==', ['get', 'heading'], HEADING_UNAVAILABLE],
      ],
      layout: {
        'icon-image': CIRCLE_IMAGE,
        'icon-allow-overlap': true,
        'icon-size': [
          'interpolate', ['linear'], ['zoom'],
          0, 0.3,
          6, 0.5,
          12, 0.9,
        ],
      },
      paint: {
        'icon-color': shiptypeColorExpression(),
        'icon-opacity': visible ? 1 : 0,
      },
    })

  }, [map, isLoaded, snapshotDate, apiBase, visible])

  // Update visibility on existing layers
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded || !activeDateRef.current) return
    const opacity = visible ? 1 : 0
    if (m.getLayer(ARROW_LAYER_ID)) {
      m.setPaintProperty(ARROW_LAYER_ID, 'icon-opacity', opacity)
    }
    if (m.getLayer(CIRCLE_LAYER_ID)) {
      m.setPaintProperty(CIRCLE_LAYER_ID, 'icon-opacity', opacity)
    }
  }, [map, isLoaded, visible])

  // Cleanup on unmount
  useEffect(() => {
    const activeMap = map.current
    return () => {
      if (activeMap) cleanup(activeMap)
    }
  }, [map])
}

function cleanup(m: maplibregl.Map): void {
  try {
    if (m.getLayer(ARROW_LAYER_ID)) m.removeLayer(ARROW_LAYER_ID)
    if (m.getLayer(CIRCLE_LAYER_ID)) m.removeLayer(CIRCLE_LAYER_ID)
    if (m.getSource(SOURCE_ID)) m.removeSource(SOURCE_ID)
  } catch {
    // Map may be destroyed
  }
}

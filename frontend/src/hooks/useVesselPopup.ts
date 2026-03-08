import { useEffect, useRef } from 'react'
import maplibregl from 'maplibre-gl'

export interface UseVesselPopupOptions {
  map: React.RefObject<maplibregl.Map | null>
  isLoaded: boolean
}

/** The two AIS symbol layers that should trigger a popup on click. */
const AIS_LAYERS = ['ais-vessel-arrows', 'ais-vessel-circles'] as const

/**
 * Format a heading value for display.
 * Returns "N/A" for missing/unavailable headings (511 in AIS spec).
 */
function formatHeading(heading: number | undefined | null): string {
  if (heading == null || heading === 511) return 'N/A'
  return `${Math.round(heading)}°`
}

function formatSOG(sog: number | string | undefined | null): string {
  if (sog == null) return 'N/A'
  const n = typeof sog === 'string' ? parseFloat(sog) : sog
  return isNaN(n) ? 'N/A' : `${n.toFixed(1)} kn`
}

/** Escape HTML entities in user-controlled strings (AIS data is external input). */
function esc(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')
}

/**
 * Build the popup HTML from feature properties.
 * Properties come from the MVT tile: mmsi, vessel_name, sog, heading,
 * shiptype, vessel_class, dwt, destination, destinationtidied, eta.
 */
function buildPopupHTML(props: Record<string, unknown>, lngLat: maplibregl.LngLat): string {
  const rawName = ((props.vessel_name as string | undefined) ?? '').trim() || 'Unknown Vessel'
  const name = esc(rawName)
  const mmsi = props.mmsi as number
  const shiptype = esc((props.shiptype as string) || '—')
  const heading = formatHeading(props.heading as number | undefined)
  const sog = formatSOG(props.sog as number | string | undefined)
  const lat = lngLat.lat.toFixed(4)
  const lon = lngLat.lng.toFixed(4)

  const rows = [
    ['MMSI', esc(String(mmsi ?? '—'))],
    ['Type', shiptype],
    ['Position', `${lat}, ${lon}`],
    ['Heading', heading],
    ['SOG', sog],
  ]

  // Optional fields — only show if present
  if (props.destination) rows.push(['Destination', esc(props.destination as string)])
  if (props.vessel_class) rows.push(['Class', esc(props.vessel_class as string)])

  const rowsHTML = rows
    .map(
      ([label, value]) =>
        `<tr>
          <td style="color:#8b949e;padding:2px 8px 2px 0;white-space:nowrap">${label}</td>
          <td style="color:#e6edf3;padding:2px 0">${value}</td>
        </tr>`,
    )
    .join('')

  return `
    <div style="font-family:system-ui,-apple-system,sans-serif;font-size:12px;line-height:1.5;min-width:160px">
      <div style="font-weight:600;font-size:13px;color:#e6edf3;margin-bottom:4px">${name}</div>
      <table style="border-collapse:collapse">${rowsHTML}</table>
    </div>
  `
}

/** Inject dark-themed popup styles once. */
function injectPopupStyles(): void {
  if (document.querySelector('style[data-vessel-popup]')) return
  const style = document.createElement('style')
  style.setAttribute('data-vessel-popup', '')
  style.textContent = `
    .vessel-popup .maplibregl-popup-content {
      background: rgba(13, 17, 23, 0.92);
      backdrop-filter: blur(8px);
      border: 1px solid rgba(48, 54, 61, 0.6);
      border-radius: 8px;
      padding: 10px 12px;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
      color: #e6edf3;
    }
    .vessel-popup .maplibregl-popup-close-button {
      color: #8b949e;
      font-size: 16px;
      padding: 2px 6px;
      right: 2px;
      top: 2px;
    }
    .vessel-popup .maplibregl-popup-close-button:hover {
      color: #e6edf3;
      background: transparent;
    }
    .vessel-popup .maplibregl-popup-tip {
      border-top-color: rgba(13, 17, 23, 0.92);
      border-bottom-color: rgba(13, 17, 23, 0.92);
      border-left-color: rgba(13, 17, 23, 0.92);
      border-right-color: rgba(13, 17, 23, 0.92);
    }
  `
  document.head.appendChild(style)
}

/**
 * Registers click handlers on AIS vessel layers to show a MapLibre popup
 * with vessel details. Also sets pointer cursor on hover.
 */
export function useVesselPopup({ map, isLoaded }: UseVesselPopupOptions): void {
  const popupRef = useRef<maplibregl.Popup | null>(null)

  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return

    injectPopupStyles()

    const popup = new maplibregl.Popup({
      closeButton: true,
      closeOnClick: true,
      maxWidth: '280px',
      className: 'vessel-popup',
    })
    popupRef.current = popup

    function onClick(e: maplibregl.MapLayerMouseEvent) {
      if (!e.features || e.features.length === 0) return

      const feature = e.features[0]
      const props = feature.properties ?? {}
      const lngLat = e.lngLat

      popup.setLngLat(lngLat).setHTML(buildPopupHTML(props, lngLat)).addTo(m!)
    }

    function onMouseEnter() {
      m!.getCanvas().style.cursor = 'pointer'
    }

    function onMouseLeave() {
      m!.getCanvas().style.cursor = ''
    }

    for (const layerId of AIS_LAYERS) {
      m.on('click', layerId, onClick)
      m.on('mouseenter', layerId, onMouseEnter)
      m.on('mouseleave', layerId, onMouseLeave)
    }

    return () => {
      for (const layerId of AIS_LAYERS) {
        m.off('click', layerId, onClick)
        m.off('mouseenter', layerId, onMouseEnter)
        m.off('mouseleave', layerId, onMouseLeave)
      }
      popup.remove()
      popupRef.current = null
    }
  }, [map, isLoaded])
}

import { Protocol } from 'pmtiles'
import maplibregl from 'maplibre-gl'

let protocol: Protocol | null = null

/**
 * Register the PMTiles protocol with MapLibre so that `pmtiles://` URLs
 * are resolved via HTTP range requests against a static .pmtiles file.
 *
 * Safe to call multiple times — only registers once.
 */
export function addPmtilesProtocol(): void {
  if (protocol) return
  protocol = new Protocol()
  maplibregl.addProtocol('pmtiles', protocol.tile)
}

export function removePmtilesProtocol(): void {
  if (!protocol) return
  maplibregl.removeProtocol('pmtiles')
  protocol = null
}

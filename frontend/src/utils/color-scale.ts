/** Interpolate a value to an RGB color string using manifest color stops. */

import type { ColorStop } from '@/types/manifest'

export function valueToColor(
  value: number | null,
  min: number,
  max: number,
  stops: ColorStop[],
): string {
  if (value == null || stops.length === 0) return 'rgba(0,0,0,0)'

  const range = max - min
  if (range <= 0) return rgbString(stops[0].color)

  const t = Math.max(0, Math.min(1, (value - min) / range))

  // Find surrounding stops
  let lo = stops[0]
  let hi = stops[stops.length - 1]
  for (let i = 0; i < stops.length - 1; i++) {
    if (t >= stops[i].position && t <= stops[i + 1].position) {
      lo = stops[i]
      hi = stops[i + 1]
      break
    }
  }

  const segRange = hi.position - lo.position
  const f = segRange > 0 ? (t - lo.position) / segRange : 0

  const r = Math.round(lo.color[0] + f * (hi.color[0] - lo.color[0]))
  const g = Math.round(lo.color[1] + f * (hi.color[1] - lo.color[1]))
  const b = Math.round(lo.color[2] + f * (hi.color[2] - lo.color[2]))

  return `rgb(${r},${g},${b})`
}

function rgbString(c: [number, number, number]): string {
  return `rgb(${c[0]},${c[1]},${c[2]})`
}

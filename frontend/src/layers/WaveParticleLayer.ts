/**
 * GPU wave-dash layer.
 *
 * Unlike the wind layer, waves are not rendered as long-lived advected tracers.
 * Each frame rebuilds a world-anchored dash field from wave height, period,
 * and direction-vector tiles. This keeps density uniform and avoids the
 * non-physical streaking artifact caused by particle convergence. The
 * passes, atlases and trail buffers are in ParticleLayer.
 */

import updateFragSource from './shaders/wave-particle-update.frag.glsl?raw'
import drawVertSource from './shaders/wave-particle-draw.vert.glsl?raw'
import drawFragSource from './shaders/wave-particle-draw.frag.glsl?raw'
import { waveGridSpacingPx } from './particle-motion'
import { encodingRange } from './color-ramps'
import type { DataRanges } from '@/types/manifest'
import {
  ParticleLayer,
  type ParticleFrame,
  type ParticleLayerOptions,
  type UniformLookup,
} from './ParticleLayer'

/** Trail fade factor per 1/60 s. */
const TRAIL_FADE = 0.55
// Sparse grid + long thin dashes: crest lines get breathing room instead of
// a dense twinkling field. Dash length ≈ POINT_SIZE * 1.3 * 0.68 ≈ 18px.
const GRID_SPACING_PX = 24.0
const PHASE_AMPLITUDE_PX = 22.0
const SPEED_MAX = 15.0
const POINT_SIZE = 20.0

export type WaveParticleLayerOptions = ParticleLayerOptions

export class WaveParticleLayer extends ParticleLayer {
  /** Dashes this frame: one per grid cell, up to the slots there are. */
  private _dashCount = 0
  /** The run's tile encoding ranges, from its manifest (#83). */
  private _dataRanges: DataRanges | undefined

  constructor(options: WaveParticleLayerOptions = {}) {
    super({
      kind: 'wave',
      id: 'wave-particles',
      opacity: 0.8,
      layers: ['wave_height', 'wave_period', 'wave_dir_u', 'wave_dir_v'],
      samplers: [
        ['u_waveHeight', 'u_waveHeightT1'],
        ['u_wavePeriod', 'u_wavePeriodT1'],
        ['u_waveDirU', 'u_waveDirUT1'],
        ['u_waveDirV', 'u_waveDirVT1'],
      ],
      updateFragSource,
      drawVertSource,
      drawFragSource,
      trailFade: TRAIL_FADE,
      stateSizeScale: 0.85,
    }, options)
  }

  setWaveConfig(model: string, runId: string, forecastHour: number, dataRanges?: DataRanges): void {
    this._dataRanges = dataRanges
    this._configure(model, runId, forecastHour)
  }

  protected _initialState(data: Float32Array, offset: number): void {
    data[offset + 0] = -1.0
    data[offset + 1] = -1.0
    data[offset + 2] = 0.5
    data[offset + 3] = 0.0
  }

  protected _setUpdateUniforms(gl: WebGL2RenderingContext, u: UniformLookup, frame: ParticleFrame): void {
    const { minLon, minLat, maxLon, maxLat } = frame.viewport
    // Widen the grid when the viewport has more cells than dash slots, else
    // the rows at the bottom (filled last) would get no dashes (#35).
    const spacingPx = waveGridSpacingPx(
      GRID_SPACING_PX,
      (maxLon - minLon) * frame.worldSize,
      (maxLat - minLat) * frame.worldSize,
      this._particleCount,
    )
    const gridSpacing = spacingPx / frame.worldSize
    const gridOriginX = Math.floor(minLon / gridSpacing) * gridSpacing
    const gridOriginY = Math.floor(minLat / gridSpacing) * gridSpacing
    const gridCols = Math.max(1, Math.ceil((maxLon - gridOriginX) / gridSpacing))
    const gridRows = Math.max(1, Math.ceil((maxLat - gridOriginY) / gridSpacing))
    this._dashCount = Math.min(this._particleCount, gridCols * gridRows)
    this._debug.gridTruncated = gridCols * gridRows > this._particleCount

    const height = encodingRange('wave_height', this._dataRanges)
    const period = encodingRange('wave_period', this._dataRanges)
    const dir = encodingRange('wave_dir_u', this._dataRanges) // dir_v is encoded alike
    gl.uniform1f(u('u_valueMinHeight'), height.min)
    gl.uniform1f(u('u_valueMaxHeight'), height.max)
    gl.uniform1f(u('u_valueMinPeriod'), period.min)
    gl.uniform1f(u('u_valueMaxPeriod'), period.max)
    gl.uniform1f(u('u_valueMinDir'), dir.min)
    gl.uniform1f(u('u_valueMaxDir'), dir.max)
    gl.uniform1f(u('u_time'), frame.now)
    gl.uniform1f(u('u_gridOriginX'), gridOriginX)
    gl.uniform1f(u('u_gridOriginY'), gridOriginY)
    gl.uniform1f(u('u_gridSpacing'), gridSpacing)
    gl.uniform1f(u('u_gridCols'), gridCols)
    gl.uniform1f(u('u_gridRows'), gridRows)
    gl.uniform1f(u('u_phaseAmplitude'), PHASE_AMPLITUDE_PX / frame.worldSize)
  }

  protected _drawParticles(gl: WebGL2RenderingContext, u: UniformLookup, frame: ParticleFrame): void {
    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, frame.state)
    gl.uniform1i(u('u_stateTex'), 0)
    // gl_PointSize is in device pixels; POINT_SIZE, like the grid, is in CSS pixels.
    gl.uniform1f(u('u_pointSize'), POINT_SIZE * frame.pixelRatio)
    gl.uniform1f(u('u_speedMax'), SPEED_MAX)
    gl.uniformMatrix4fv(u('u_matrix'), false, frame.mercatorMatrix)

    gl.bindVertexArray(this._drawVao)
    gl.drawArrays(gl.POINTS, 0, this._dashCount)
    gl.bindVertexArray(null)
  }
}

/**
 * GPU wind-particle layer with trails.
 *
 * Particles are advected by the wind U/V field and drawn, each frame, as a
 * segment from their previous position to their current one; the faded
 * trail buffer joins the segments into streaks. The passes, atlases and
 * trail buffers are in ParticleLayer.
 */

import updateFragSource from './shaders/particle-update.frag.glsl?raw'
import drawVertSource from './shaders/particle-draw.vert.glsl?raw'
import drawFragSource from './shaders/particle-draw.frag.glsl?raw'
import { windSpeedScale } from './particle-motion'
import {
  ParticleLayer,
  type ParticleFrame,
  type ParticleLayerOptions,
  type UniformLookup,
} from './ParticleLayer'

/** Trail fade factor per 1/60 s. With the speed in particle-motion.ts a trail lasts
 *  about 1.2 s and is about 55 px long in a 10 m/s wind. */
const TRAIL_FADE = 0.97
/** Maximum expected wind speed (m/s) for normalizing speed → alpha in the draw shader. */
const SPEED_MAX = 50.0
/** Trail width in CSS pixels — zoom-independent. Wide enough to read over the colour layer. */
const LINE_WIDTH = 2.5
/**
 * Particles drawn per CSS pixel² of viewport, so density does not depend on
 * window size (about 5,200 at 1440×900). The GPU tier only caps the total.
 */
const PARTICLES_PER_CSS_PX2 = 1 / 250

export type WindParticleLayerOptions = ParticleLayerOptions

export class WindParticleLayer extends ParticleLayer {
  private _valueMin = -50
  private _valueMax = 50

  constructor(options: WindParticleLayerOptions = {}) {
    super({
      kind: 'wind',
      id: 'wind-particles',
      opacity: 1.0,
      layers: ['wind_u', 'wind_v'],
      samplers: [['u_windU', 'u_windUT1'], ['u_windV', 'u_windVT1']],
      updateFragSource,
      drawVertSource,
      drawFragSource,
      trailFade: TRAIL_FADE,
      // The tier sets how many particles there can be; render() draws as
      // many as the viewport area calls for.
      stateSizeScale: 1,
    }, options)
  }

  /** Configure the wind dataset, and the range its U/V tiles were encoded with. */
  setWindConfig(model: string, runId: string, forecastHour: number, valueMin: number, valueMax: number): void {
    this._valueMin = valueMin
    this._valueMax = valueMax
    this._configure(model, runId, forecastHour)
  }

  protected _initialState(data: Float32Array, offset: number): void {
    data[offset + 0] = Math.random()  // lon [0,1]
    data[offset + 1] = Math.random()  // lat [0,1]
    data[offset + 2] = Math.random()  // age [0,1] — stagger spawns
    data[offset + 3] = 1.0            // reserved
  }

  protected _setUpdateUniforms(gl: WebGL2RenderingContext, u: UniformLookup, frame: ParticleFrame): void {
    gl.uniform1f(u('u_valueMin'), this._valueMin)
    gl.uniform1f(u('u_valueMax'), this._valueMax)
    // The shader multiplies by dt, so speed on screen is per second, not per frame.
    gl.uniform1f(u('u_speedScale'), windSpeedScale(frame.worldSize))
    gl.uniform1f(u('u_dt'), frame.dt)
    gl.uniform1f(u('u_seed'), (frame.now * 137.0) % 1000.0)
  }

  /**
   * Each particle is a segment from its position before this frame's update
   * to its position after it. Segments join up frame after frame, so a trail
   * stays continuous however far a particle moves.
   */
  protected _drawParticles(gl: WebGL2RenderingContext, u: UniformLookup, frame: ParticleFrame): void {
    // Draw a share of the particles proportional to the viewport's CSS area.
    // All of them are updated (cheap), so a resize only changes how many show.
    const cssArea = (frame.canvasWidth / frame.pixelRatio) * (frame.canvasHeight / frame.pixelRatio)
    const drawn = Math.min(this._particleCount, Math.max(1, Math.round(cssArea * PARTICLES_PER_CSS_PX2)))
    this._debug.drawnParticles = drawn

    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, frame.state)
    gl.activeTexture(gl.TEXTURE1)
    gl.bindTexture(gl.TEXTURE_2D, frame.prevState)
    gl.uniform1i(u('u_stateTex'), 0)
    gl.uniform1i(u('u_prevStateTex'), 1)
    gl.uniform2f(u('u_viewport'), frame.canvasWidth, frame.canvasHeight)
    gl.uniform1f(u('u_lineWidth'), LINE_WIDTH * frame.pixelRatio)
    gl.uniform1f(u('u_speedMax'), SPEED_MAX)
    gl.uniformMatrix4fv(u('u_matrix'), false, frame.mercatorMatrix)

    // MAX instead of adding: consecutive segments overlap at their round
    // ends, and summing there would bead the trail.
    gl.blendEquation(gl.MAX)
    gl.bindVertexArray(this._drawVao)
    gl.drawArrays(gl.TRIANGLES, 0, drawn * 6)
    gl.bindVertexArray(null)
    gl.blendEquation(gl.FUNC_ADD) // the composite pass after this adds
  }
}

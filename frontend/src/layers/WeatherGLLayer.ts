/**
 * MapLibre CustomLayerInterface for GPU-rendered weather data.
 *
 * This is the WebGL entry point for the weather rendering pipeline.
 * It compiles shaders, manages the GL context lifecycle, creates a
 * fullscreen quad, and integrates with MapLibre's render loop.
 *
 * Currently renders a placeholder solid color. Future beads (wx-170.3,
 * wx-170.4) will add tile management and data decode/colorize shaders.
 */

import type {
  CustomLayerInterface,
  CustomRenderMethodInput,
  Map as MaplibreMap,
} from 'maplibre-gl'

import vertexSource from './shaders/weather.vert.glsl?raw'
import fragmentSource from './shaders/weather.frag.glsl?raw'
import {
  createFullscreenQuad,
  createProgram,
  deleteProgram,
  deleteQuadGeometry,
  type GLProgram,
  type QuadGeometry,
} from './gl-utils'

export interface WeatherGLLayerOptions {
  /** Unique layer ID for MapLibre. */
  id?: string
  /**
   * Placeholder color as [R, G, B, A] in 0-1 range.
   * Default: semi-transparent blue.
   */
  color?: [number, number, number, number]
}

export class WeatherGLLayer implements CustomLayerInterface {
  readonly id: string
  readonly type = 'custom' as const
  readonly renderingMode = '2d' as const

  private _color: [number, number, number, number]
  private _map: MaplibreMap | null = null
  private _gl: WebGL2RenderingContext | null = null
  private _program: GLProgram | null = null
  private _quad: QuadGeometry | null = null
  private _colorLocation: WebGLUniformLocation | null = null

  constructor(options: WeatherGLLayerOptions = {}) {
    this.id = options.id ?? 'weather-gl'
    this._color = options.color ?? [0.0, 0.3, 0.8, 0.15]
  }

  /**
   * Called by MapLibre when the layer is added to the map.
   * Initializes shaders, buffers, and uniform locations.
   */
  onAdd(map: MaplibreMap, gl: WebGLRenderingContext | WebGL2RenderingContext): void {
    // Require WebGL2
    if (!(gl instanceof WebGL2RenderingContext)) {
      console.error('[WeatherGLLayer] WebGL2 is required but not available')
      return
    }

    this._map = map
    this._gl = gl

    try {
      this._program = createProgram(gl, vertexSource, fragmentSource)
      this._quad = createFullscreenQuad(gl)
      this._colorLocation = gl.getUniformLocation(
        this._program.program,
        'u_color',
      )
    } catch (e) {
      console.error('[WeatherGLLayer] Initialization failed:', e)
      this._cleanup()
    }
  }

  /**
   * Called each frame by MapLibre. Draws the fullscreen quad with the
   * current shader program.
   *
   * @param gl - The WebGL context.
   * @param _options - Render options including the projection matrix (unused
   *   for fullscreen quad, will be needed for geo-positioned tile data).
   */
  render(gl: WebGLRenderingContext | WebGL2RenderingContext, _options: CustomRenderMethodInput): void {
    if (!this._program || !this._quad || !(gl instanceof WebGL2RenderingContext)) {
      return
    }

    // Save MapLibre's current program so we can restore it after drawing
    const prevProgram = gl.getParameter(gl.CURRENT_PROGRAM) as WebGLProgram | null

    gl.useProgram(this._program.program)

    // Set placeholder color uniform
    if (this._colorLocation) {
      gl.uniform4fv(this._colorLocation, this._color)
    }

    // Draw fullscreen quad
    gl.bindVertexArray(this._quad.vao)
    gl.drawArrays(gl.TRIANGLES, 0, this._quad.vertexCount)
    gl.bindVertexArray(null)

    // Restore MapLibre's program to avoid corrupting its render state
    gl.useProgram(prevProgram)
  }

  /**
   * Called by MapLibre when the layer is removed.
   * Frees all GL resources.
   */
  onRemove(
    _map: MaplibreMap,
    _gl: WebGLRenderingContext | WebGL2RenderingContext,
  ): void {
    this._cleanup()
  }

  /** Update the placeholder color at runtime. */
  setColor(color: [number, number, number, number]): void {
    this._color = color
    this._map?.triggerRepaint()
  }

  /** Free all GL resources. */
  private _cleanup(): void {
    const gl = this._gl
    if (gl) {
      if (this._quad) {
        deleteQuadGeometry(gl, this._quad)
        this._quad = null
      }
      if (this._program) {
        deleteProgram(gl, this._program)
        this._program = null
      }
    }
    this._colorLocation = null
    this._gl = null
    this._map = null
  }
}

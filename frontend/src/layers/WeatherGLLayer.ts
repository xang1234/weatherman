/**
 * MapLibre CustomLayerInterface for GPU-rendered weather data.
 *
 * This is the WebGL entry point for the weather rendering pipeline.
 * It compiles shaders, manages the GL context lifecycle, creates a
 * fullscreen quad, and integrates with MapLibre's render loop.
 *
 * The fragment shader decodes 16-bit float data from data tile textures
 * and colorizes them using a 1D color ramp lookup texture.
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
import {
  COLOR_RAMPS,
  createColorRampTexture,
} from './color-ramps'

export interface WeatherGLLayerOptions {
  /** Unique layer ID for MapLibre. */
  id?: string
  /** Weather layer name for color ramp selection (e.g. 'temperature'). */
  layer?: string
  /** Overlay opacity 0-1. Default: 0.7. */
  opacity?: number
}

export class WeatherGLLayer implements CustomLayerInterface {
  readonly id: string
  readonly type = 'custom' as const
  readonly renderingMode = '2d' as const

  private _layerName: string
  private _opacity: number
  private _map: MaplibreMap | null = null
  private _gl: WebGL2RenderingContext | null = null
  private _program: GLProgram | null = null
  private _quad: QuadGeometry | null = null

  // Uniform locations
  private _uDataTile: WebGLUniformLocation | null = null
  private _uColorRamp: WebGLUniformLocation | null = null
  private _uOpacity: WebGLUniformLocation | null = null

  // Color ramp texture (256x1 RGBA)
  private _colorRampTexture: WebGLTexture | null = null

  // Data tile texture — set externally by the integration layer
  private _dataTileTexture: WebGLTexture | null = null

  constructor(options: WeatherGLLayerOptions = {}) {
    this.id = options.id ?? 'weather-gl'
    this._layerName = options.layer ?? 'temperature'
    this._opacity = options.opacity ?? 0.7
  }

  /**
   * Called by MapLibre when the layer is added to the map.
   * Initializes shaders, buffers, uniform locations, and color ramp texture.
   */
  onAdd(map: MaplibreMap, gl: WebGLRenderingContext | WebGL2RenderingContext): void {
    if (!(gl instanceof WebGL2RenderingContext)) {
      console.error('[WeatherGLLayer] WebGL2 is required but not available')
      return
    }

    this._map = map
    this._gl = gl

    try {
      this._program = createProgram(gl, vertexSource, fragmentSource)
      this._quad = createFullscreenQuad(gl)

      const prog = this._program.program
      this._uDataTile = gl.getUniformLocation(prog, 'u_dataTile')
      this._uColorRamp = gl.getUniformLocation(prog, 'u_colorRamp')
      this._uOpacity = gl.getUniformLocation(prog, 'u_opacity')

      this._createColorRamp(gl)
    } catch (e) {
      console.error('[WeatherGLLayer] Initialization failed:', e)
      this._cleanup()
    }
  }

  /**
   * Called each frame by MapLibre. Decodes the data tile texture and
   * applies color ramp colorization via the fragment shader.
   */
  render(gl: WebGLRenderingContext | WebGL2RenderingContext, _options: CustomRenderMethodInput): void {
    if (
      !this._program || !this._quad ||
      !this._colorRampTexture || !this._dataTileTexture ||
      !(gl instanceof WebGL2RenderingContext)
    ) {
      return
    }

    // Save MapLibre's current program so we can restore it after drawing
    const prevProgram = gl.getParameter(gl.CURRENT_PROGRAM) as WebGLProgram | null

    gl.useProgram(this._program.program)

    // Bind data tile texture to unit 0
    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this._dataTileTexture)
    gl.uniform1i(this._uDataTile, 0)

    // Bind color ramp texture to unit 1
    gl.activeTexture(gl.TEXTURE1)
    gl.bindTexture(gl.TEXTURE_2D, this._colorRampTexture)
    gl.uniform1i(this._uColorRamp, 1)

    // Set opacity
    gl.uniform1f(this._uOpacity, this._opacity)

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

  /** Set the data tile texture to render. Called by the integration layer. */
  setDataTileTexture(texture: WebGLTexture | null): void {
    this._dataTileTexture = texture
    this._map?.triggerRepaint()
  }

  /** Update opacity at runtime. */
  setOpacity(opacity: number): void {
    this._opacity = opacity
    this._map?.triggerRepaint()
  }

  /** Switch to a different weather layer's color ramp. */
  setLayer(layerName: string): void {
    if (layerName === this._layerName) return
    this._layerName = layerName
    if (this._gl) {
      this._createColorRamp(this._gl)
      this._map?.triggerRepaint()
    }
  }

  /** Get the current layer name. */
  get layerName(): string {
    return this._layerName
  }

  // ── Private ──────────────────────────────────────────────────────

  /** Create or replace the color ramp texture for the current layer. */
  private _createColorRamp(gl: WebGL2RenderingContext): void {
    // Delete previous ramp texture if switching layers
    if (this._colorRampTexture) {
      gl.deleteTexture(this._colorRampTexture)
      this._colorRampTexture = null
    }

    const ramp = COLOR_RAMPS[this._layerName]
    if (!ramp) {
      console.warn(`[WeatherGLLayer] No color ramp for layer '${this._layerName}'`)
      return
    }

    this._colorRampTexture = createColorRampTexture(gl, ramp)
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
      if (this._colorRampTexture) {
        gl.deleteTexture(this._colorRampTexture)
        this._colorRampTexture = null
      }
    }
    this._uDataTile = null
    this._uColorRamp = null
    this._uOpacity = null
    this._dataTileTexture = null
    this._gl = null
    this._map = null
  }
}

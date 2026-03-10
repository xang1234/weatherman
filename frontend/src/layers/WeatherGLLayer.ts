/**
 * MapLibre CustomLayerInterface for GPU-rendered weather data.
 *
 * Renders tiled weather data using WebGL2. Each visible map tile is
 * drawn as a positioned quad using the map's projection matrix.
 * The fragment shader decodes 16-bit float data from RGBA PNG tiles
 * and colorizes them using a 1D color ramp lookup texture.
 *
 * Owns a TileManager that handles tile fetching, texture upload,
 * and LRU cache eviction. The render loop computes visible tiles
 * from the current map viewport, updates the TileManager, and
 * draws all loaded tiles in a single pass.
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
import {
  TileManager,
  computeVisibleTiles,
  type TileCoord,
} from './TileManager'

export interface WeatherGLLayerOptions {
  /** Unique layer ID for MapLibre. */
  id?: string
  /** Initial weather layer name for color ramp selection. */
  layer?: string
  /** Overlay opacity 0-1. Default: 0.7. */
  opacity?: number
  /** Base URL for the data tile API. Default: '' (same-origin). */
  apiBase?: string
}

export class WeatherGLLayer implements CustomLayerInterface {
  readonly id: string
  readonly type = 'custom' as const
  readonly renderingMode = '2d' as const

  private _layerName: string
  private _opacity: number
  private _apiBase: string
  private _map: MaplibreMap | null = null
  private _gl: WebGL2RenderingContext | null = null
  private _program: GLProgram | null = null
  private _quad: QuadGeometry | null = null
  private _tileManager: TileManager | null = null
  private _colorRampTexture: WebGLTexture | null = null

  // Dataset params (forwarded to TileManager)
  private _model = ''
  private _runId = ''
  private _forecastHour = 0

  // Uniform locations
  private _uMatrix: WebGLUniformLocation | null = null
  private _uTileOffset: WebGLUniformLocation | null = null
  private _uTileScale: WebGLUniformLocation | null = null
  private _uDataTile: WebGLUniformLocation | null = null
  private _uColorRamp: WebGLUniformLocation | null = null
  private _uOpacity: WebGLUniformLocation | null = null

  constructor(options: WeatherGLLayerOptions = {}) {
    this.id = options.id ?? 'weather-gl'
    this._layerName = options.layer ?? 'temperature'
    this._opacity = options.opacity ?? 0.7
    this._apiBase = options.apiBase ?? ''
  }

  /**
   * Called by MapLibre when the layer is added to the map.
   * Initializes shaders, buffers, uniform locations, color ramp, and tile manager.
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
      this._uMatrix = gl.getUniformLocation(prog, 'u_matrix')
      this._uTileOffset = gl.getUniformLocation(prog, 'u_tileOffset')
      this._uTileScale = gl.getUniformLocation(prog, 'u_tileScale')
      this._uDataTile = gl.getUniformLocation(prog, 'u_dataTile')
      this._uColorRamp = gl.getUniformLocation(prog, 'u_colorRamp')
      this._uOpacity = gl.getUniformLocation(prog, 'u_opacity')

      this._createColorRamp(gl)

      // Create tile manager for data tile fetching + texture upload
      this._tileManager = new TileManager(gl, { apiBase: this._apiBase })
      this._tileManager.onTileLoaded = () => map.triggerRepaint()

      // Apply dataset config if already set before onAdd
      if (this._model && this._runId && this._layerName) {
        this._tileManager.setLayer(this._model, this._runId, this._layerName, this._forecastHour)
      }
    } catch (e) {
      console.error('[WeatherGLLayer] Initialization failed:', e)
      this._cleanup()
    }
  }

  /**
   * Called each frame by MapLibre. Computes visible tiles, updates the
   * tile manager, and draws each loaded tile as a projected quad.
   */
  render(gl: WebGLRenderingContext | WebGL2RenderingContext, options: CustomRenderMethodInput): void {
    if (
      !this._program || !this._quad || !this._colorRampTexture ||
      !this._tileManager || !this._map ||
      !this._model || !this._runId ||
      !(gl instanceof WebGL2RenderingContext)
    ) {
      return
    }

    // Compute visible tiles from current map viewport
    const zoom = Math.max(0, Math.min(8, Math.floor(this._map.getZoom())))
    const bounds = this._map.getBounds()
    const visibleCoords = computeVisibleTiles({
      west: bounds.getWest(),
      north: bounds.getNorth(),
      east: bounds.getEast(),
      south: bounds.getSouth(),
    }, zoom)

    // Update tile manager — starts fetches for any missing tiles
    this._tileManager.updateVisibleTiles(visibleCoords)

    // Collect tiles that have loaded textures
    const tilesToDraw: { coord: TileCoord; texture: WebGLTexture }[] = []
    for (const coord of visibleCoords) {
      const tex = this._tileManager.getTexture(coord.z, coord.x, coord.y)
      if (tex) tilesToDraw.push({ coord, texture: tex })
    }

    if (tilesToDraw.length === 0) return

    // Save MapLibre's GL state
    const prevProgram = gl.getParameter(gl.CURRENT_PROGRAM) as WebGLProgram | null
    const prevActiveTexture = gl.getParameter(gl.ACTIVE_TEXTURE) as number

    gl.useProgram(this._program.program)

    // Set shared uniforms (same for all tiles)
    gl.uniformMatrix4fv(this._uMatrix, false, options.modelViewProjectionMatrix)
    gl.uniform1f(this._uOpacity, this._opacity)

    // Bind color ramp to texture unit 1 (shared across all tiles)
    gl.activeTexture(gl.TEXTURE1)
    gl.bindTexture(gl.TEXTURE_2D, this._colorRampTexture)
    gl.uniform1i(this._uColorRamp, 1)

    // Draw each visible tile as a positioned quad
    const n = 2 ** zoom
    const scale = 1 / n

    gl.bindVertexArray(this._quad.vao)

    for (const { coord, texture } of tilesToDraw) {
      // Per-tile uniforms: position this quad in mercator space
      gl.uniform2f(this._uTileOffset, coord.x * scale, coord.y * scale)
      gl.uniform2f(this._uTileScale, scale, scale)

      // Bind data tile texture to unit 0
      gl.activeTexture(gl.TEXTURE0)
      gl.bindTexture(gl.TEXTURE_2D, texture)
      gl.uniform1i(this._uDataTile, 0)

      gl.drawArrays(gl.TRIANGLES, 0, this._quad.vertexCount)
    }

    gl.bindVertexArray(null)

    // Restore MapLibre's GL state
    gl.activeTexture(prevActiveTexture)
    gl.useProgram(prevProgram)
  }

  /**
   * Called by MapLibre when the layer is removed.
   * Frees all GL resources including tile textures.
   */
  onRemove(
    _map: MaplibreMap,
    _gl: WebGLRenderingContext | WebGL2RenderingContext,
  ): void {
    this._cleanup()
  }

  /** Update the dataset to render. Clears tile cache if params changed. */
  setConfig(model: string, runId: string, layer: string, forecastHour: number): void {
    const layerChanged = layer !== this._layerName
    this._model = model
    this._runId = runId
    this._layerName = layer
    this._forecastHour = forecastHour

    if (layerChanged && this._gl) {
      this._createColorRamp(this._gl)
    }

    this._tileManager?.setLayer(model, runId, layer, forecastHour)
    this._map?.triggerRepaint()
  }

  /** Update opacity at runtime. */
  setOpacity(opacity: number): void {
    this._opacity = opacity
    this._map?.triggerRepaint()
  }

  /** Get the current layer name. */
  get layerName(): string {
    return this._layerName
  }

  // ── Private ──────────────────────────────────────────────────────

  /** Create or replace the color ramp texture for the current layer. */
  private _createColorRamp(gl: WebGL2RenderingContext): void {
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
    if (this._tileManager) {
      this._tileManager.destroy()
      this._tileManager = null
    }
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
    this._uMatrix = null
    this._uTileOffset = null
    this._uTileScale = null
    this._uDataTile = null
    this._uColorRamp = null
    this._uOpacity = null
    this._gl = null
    this._map = null
  }
}

/**
 * MapLibre CustomLayerInterface for GPU-rendered weather data.
 *
 * Renders tiled weather data using WebGL2. Each visible map tile is
 * drawn as a positioned quad using the map's projection matrix.
 * The fragment shader decodes 16-bit float data from RGBA PNG tiles
 * and colorizes them using a 1D color ramp lookup texture.
 *
 * Supports two rendering modes:
 *   - Scalar: single data tile per cell (temperature, precipitation)
 *   - Vector: U/V component tile pairs per cell (wind). Interpolates
 *     Cartesian components then reconstructs speed for color ramp lookup.
 *
 * Owns TileManagers that handle tile fetching, texture upload,
 * and LRU cache eviction. The render loop computes visible tiles
 * from the current map viewport, updates the TileManagers, and
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

/** Layers that use U/V vector component tiles instead of scalar tiles. */
const VECTOR_LAYERS = new Set(['wind_speed'])

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
  private _colorRampTexture: WebGLTexture | null = null

  // Scalar tile managers (U component in vector mode)
  private _tileManager: TileManager | null = null
  private _tileManagerT1: TileManager | null = null

  // V-component tile managers (vector mode only)
  private _tileManagerV: TileManager | null = null
  private _tileManagerVT1: TileManager | null = null

  // Dataset params (forwarded to TileManager)
  private _model = ''
  private _runId = ''
  private _forecastHour = 0

  // Temporal interpolation state
  private _forecastHourT1 = -1
  private _temporalMix = 0

  // Uniform locations
  private _uMatrix: WebGLUniformLocation | null = null
  private _uTileOffset: WebGLUniformLocation | null = null
  private _uTileScale: WebGLUniformLocation | null = null
  private _uDataTile: WebGLUniformLocation | null = null
  private _uDataTileT1: WebGLUniformLocation | null = null
  private _uDataTileV: WebGLUniformLocation | null = null
  private _uDataTileVT1: WebGLUniformLocation | null = null
  private _uColorRamp: WebGLUniformLocation | null = null
  private _uOpacity: WebGLUniformLocation | null = null
  private _uTemporalMix: WebGLUniformLocation | null = null
  private _uIsVector: WebGLUniformLocation | null = null
  private _uValueMin: WebGLUniformLocation | null = null
  private _uValueMax: WebGLUniformLocation | null = null

  constructor(options: WeatherGLLayerOptions = {}) {
    this.id = options.id ?? 'weather-gl'
    this._layerName = options.layer ?? 'temperature'
    this._opacity = options.opacity ?? 0.7
    this._apiBase = options.apiBase ?? ''
  }

  /** Whether the current layer uses U/V vector component tiles. */
  private get _isVector(): boolean {
    return VECTOR_LAYERS.has(this._layerName)
  }

  /**
   * Called by MapLibre when the layer is added to the map.
   * Initializes shaders, buffers, uniform locations, color ramp, and tile managers.
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
      this._uDataTileT1 = gl.getUniformLocation(prog, 'u_dataTileT1')
      this._uDataTileV = gl.getUniformLocation(prog, 'u_dataTileV')
      this._uDataTileVT1 = gl.getUniformLocation(prog, 'u_dataTileVT1')
      this._uColorRamp = gl.getUniformLocation(prog, 'u_colorRamp')
      this._uOpacity = gl.getUniformLocation(prog, 'u_opacity')
      this._uTemporalMix = gl.getUniformLocation(prog, 'u_temporalMix')
      this._uIsVector = gl.getUniformLocation(prog, 'u_isVector')
      this._uValueMin = gl.getUniformLocation(prog, 'u_valueMin')
      this._uValueMax = gl.getUniformLocation(prog, 'u_valueMax')

      this._createColorRamp(gl)

      // Create tile managers for data tile fetching + texture upload.
      // T0 = current forecast hour, T1 = next hour (for temporal interpolation).
      // In vector mode, the main managers fetch U-component tiles
      // and the V managers fetch V-component tiles.
      const triggerRepaint = () => map.triggerRepaint()
      this._tileManager = new TileManager(gl, { apiBase: this._apiBase })
      this._tileManager.onTileLoaded = triggerRepaint
      this._tileManagerT1 = new TileManager(gl, { apiBase: this._apiBase })
      this._tileManagerT1.onTileLoaded = triggerRepaint
      this._tileManagerV = new TileManager(gl, { apiBase: this._apiBase })
      this._tileManagerV.onTileLoaded = triggerRepaint
      this._tileManagerVT1 = new TileManager(gl, { apiBase: this._apiBase })
      this._tileManagerVT1.onTileLoaded = triggerRepaint

      // Apply dataset config if already set before onAdd
      if (this._model && this._runId && this._layerName) {
        this._applyLayerConfig()
      }
    } catch (e) {
      console.error('[WeatherGLLayer] Initialization failed:', e)
      this._cleanup()
    }
  }

  /**
   * Called each frame by MapLibre. Computes visible tiles, updates the
   * tile managers, and draws each loaded tile as a projected quad.
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

    const isVector = this._isVector

    // Compute visible tiles from current map viewport
    const zoom = Math.max(0, Math.min(8, Math.floor(this._map.getZoom())))
    const bounds = this._map.getBounds()
    const visibleCoords = computeVisibleTiles({
      west: bounds.getWest(),
      north: bounds.getNorth(),
      east: bounds.getEast(),
      south: bounds.getSouth(),
    }, zoom)

    // Update all active tile managers — starts fetches for any missing tiles
    this._tileManager.updateVisibleTiles(visibleCoords)
    const blending = this._temporalMix > 0 && this._forecastHourT1 >= 0 && this._tileManagerT1 != null
    if (blending) {
      this._tileManagerT1!.updateVisibleTiles(visibleCoords)
    }
    if (isVector && this._tileManagerV) {
      this._tileManagerV.updateVisibleTiles(visibleCoords)
      if (blending && this._tileManagerVT1) {
        this._tileManagerVT1.updateVisibleTiles(visibleCoords)
      }
    }

    // Collect tiles that have loaded required textures
    interface TileDraw {
      coord: TileCoord
      texT0: WebGLTexture
      texT1: WebGLTexture | null
      texV: WebGLTexture | null
      texVT1: WebGLTexture | null
    }
    const tilesToDraw: TileDraw[] = []
    for (const coord of visibleCoords) {
      const texT0 = this._tileManager.getTexture(coord.z, coord.x, coord.y)
      if (!texT0) continue

      // In vector mode, both U and V must be loaded to draw
      let texV: WebGLTexture | null = null
      if (isVector) {
        texV = this._tileManagerV?.getTexture(coord.z, coord.x, coord.y) ?? null
        if (!texV) continue
      }

      const texT1 = blending
        ? this._tileManagerT1!.getTexture(coord.z, coord.x, coord.y)
        : null
      const texVT1 = (blending && isVector)
        ? this._tileManagerVT1?.getTexture(coord.z, coord.x, coord.y) ?? null
        : null

      tilesToDraw.push({ coord, texT0, texT1, texV, texVT1 })
    }

    if (tilesToDraw.length === 0) return

    // Save MapLibre's GL state (blend, program, active texture unit, and per-unit bindings)
    // We use up to 5 texture units: 0=U/scalar, 1=colorRamp, 2=T1, 3=V, 4=VT1
    const prevBlend = gl.isEnabled(gl.BLEND)
    const prevBlendSrc = gl.getParameter(gl.BLEND_SRC_RGB) as number
    const prevBlendDst = gl.getParameter(gl.BLEND_DST_RGB) as number
    const prevBlendSrcA = gl.getParameter(gl.BLEND_SRC_ALPHA) as number
    const prevBlendDstA = gl.getParameter(gl.BLEND_DST_ALPHA) as number
    const prevProgram = gl.getParameter(gl.CURRENT_PROGRAM) as WebGLProgram | null
    const prevActiveTexture = gl.getParameter(gl.ACTIVE_TEXTURE) as number
    const prevTexBindings: (WebGLTexture | null)[] = []
    for (let i = 0; i < 5; i++) {
      gl.activeTexture(gl.TEXTURE0 + i)
      prevTexBindings.push(gl.getParameter(gl.TEXTURE_BINDING_2D) as WebGLTexture | null)
    }

    // Enable premultiplied-alpha blending for correct compositing over the basemap
    gl.enable(gl.BLEND)
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA)

    gl.useProgram(this._program.program)

    // Set shared uniforms (same for all tiles)
    gl.uniformMatrix4fv(this._uMatrix, false, options.modelViewProjectionMatrix)
    gl.uniform1f(this._uOpacity, this._opacity)
    gl.uniform1i(this._uIsVector, isVector ? 1 : 0)

    // Pass value range for vector mode denormalization.
    // Wind U/V components are encoded with symmetric range [-max, +max]
    // where max = wind_speed color ramp ceiling.
    if (isVector) {
      const ramp = COLOR_RAMPS[this._layerName]
      const max = ramp?.valueMax ?? 50
      gl.uniform1f(this._uValueMin, -max)
      gl.uniform1f(this._uValueMax, max)
    } else {
      gl.uniform1f(this._uValueMin, 0)
      gl.uniform1f(this._uValueMax, 1)
    }

    // Bind color ramp to texture unit 1 (shared across all tiles)
    gl.activeTexture(gl.TEXTURE1)
    gl.bindTexture(gl.TEXTURE_2D, this._colorRampTexture)
    gl.uniform1i(this._uColorRamp, 1)

    // Assign sampler units (constant across all tiles)
    gl.uniform1i(this._uDataTile, 0)
    gl.uniform1i(this._uDataTileT1, 2)
    gl.uniform1i(this._uDataTileV, 3)
    gl.uniform1i(this._uDataTileVT1, 4)

    // Prime unused units with null for safe default reads
    gl.activeTexture(gl.TEXTURE2)
    gl.bindTexture(gl.TEXTURE_2D, null)
    gl.activeTexture(gl.TEXTURE3)
    gl.bindTexture(gl.TEXTURE_2D, null)
    gl.activeTexture(gl.TEXTURE4)
    gl.bindTexture(gl.TEXTURE_2D, null)

    // Draw each visible tile as a positioned quad
    const n = 2 ** zoom
    const scale = 1 / n

    gl.bindVertexArray(this._quad.vao)

    for (const { coord, texT0, texT1, texV, texVT1 } of tilesToDraw) {
      // Per-tile uniforms: position this quad in mercator space
      gl.uniform2f(this._uTileOffset, coord.x * scale, coord.y * scale)
      gl.uniform2f(this._uTileScale, scale, scale)

      // Bind U/scalar data tile texture to unit 0
      gl.activeTexture(gl.TEXTURE0)
      gl.bindTexture(gl.TEXTURE_2D, texT0)

      // Bind T1 data tile to unit 2 if available, otherwise null it and
      // set mix to 0 so the shader won't sample a stale binding from a
      // previous tile iteration.
      if (texT1) {
        gl.activeTexture(gl.TEXTURE2)
        gl.bindTexture(gl.TEXTURE_2D, texT1)
        gl.uniform1f(this._uTemporalMix, this._temporalMix)
      } else {
        gl.activeTexture(gl.TEXTURE2)
        gl.bindTexture(gl.TEXTURE_2D, null)
        gl.uniform1f(this._uTemporalMix, 0)
      }

      // Bind V-component tiles for vector mode
      if (isVector && texV) {
        gl.activeTexture(gl.TEXTURE3)
        gl.bindTexture(gl.TEXTURE_2D, texV)
        if (texVT1) {
          gl.activeTexture(gl.TEXTURE4)
          gl.bindTexture(gl.TEXTURE_2D, texVT1)
        } else {
          gl.activeTexture(gl.TEXTURE4)
          gl.bindTexture(gl.TEXTURE_2D, null)
        }
      }

      gl.drawArrays(gl.TRIANGLES, 0, this._quad.vertexCount)
    }

    gl.bindVertexArray(null)

    // Restore MapLibre's GL state (blend + texture bindings + active unit + program)
    if (prevBlend) {
      gl.blendFuncSeparate(prevBlendSrc, prevBlendDst, prevBlendSrcA, prevBlendDstA)
    } else {
      gl.disable(gl.BLEND)
    }
    for (let i = 0; i < 5; i++) {
      gl.activeTexture(gl.TEXTURE0 + i)
      gl.bindTexture(gl.TEXTURE_2D, prevTexBindings[i])
    }
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

    this._applyLayerConfig()
    this._map?.triggerRepaint()
  }

  /**
   * Set temporal interpolation parameters.
   * @param forecastHourT1 — The next forecast hour to blend toward, or -1 to disable.
   * @param mix — Blend factor: 0.0 = T0 only, 1.0 = T1 only.
   */
  setTemporalBlend(forecastHourT1: number, mix: number): void {
    this._forecastHourT1 = forecastHourT1
    this._temporalMix = Math.max(0, Math.min(1, mix))

    if (forecastHourT1 >= 0 && this._model && this._runId && this._layerName) {
      const t1Layer = this._isVector ? 'wind_u' : this._layerName
      this._tileManagerT1?.setLayer(this._model, this._runId, t1Layer, forecastHourT1)
      if (this._isVector) {
        this._tileManagerVT1?.setLayer(this._model, this._runId, 'wind_v', forecastHourT1)
      }
    }
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

  /**
   * Apply the current dataset config to all tile managers.
   * In vector mode, the main manager fetches wind_u and the V manager
   * fetches wind_v. In scalar mode, only the main manager is used.
   */
  private _applyLayerConfig(): void {
    if (!this._model || !this._runId) return

    if (this._isVector) {
      // Vector mode: split into U and V component tile fetches
      this._tileManager?.setLayer(this._model, this._runId, 'wind_u', this._forecastHour)
      this._tileManagerV?.setLayer(this._model, this._runId, 'wind_v', this._forecastHour)
      if (this._forecastHourT1 >= 0) {
        this._tileManagerT1?.setLayer(this._model, this._runId, 'wind_u', this._forecastHourT1)
        this._tileManagerVT1?.setLayer(this._model, this._runId, 'wind_v', this._forecastHourT1)
      }
    } else {
      // Scalar mode: single tile per cell
      this._tileManager?.setLayer(this._model, this._runId, this._layerName, this._forecastHour)
      if (this._forecastHourT1 >= 0) {
        this._tileManagerT1?.setLayer(this._model, this._runId, this._layerName, this._forecastHourT1)
      } else {
        // Clear T1 so it doesn't hold stale tiles from previous vector mode
        this._tileManagerT1?.clear()
      }
      // Clear V managers so they don't hold stale wind tiles
      this._tileManagerV?.clear()
      this._tileManagerVT1?.clear()
    }
  }

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
    if (this._tileManagerT1) {
      this._tileManagerT1.destroy()
      this._tileManagerT1 = null
    }
    if (this._tileManagerV) {
      this._tileManagerV.destroy()
      this._tileManagerV = null
    }
    if (this._tileManagerVT1) {
      this._tileManagerVT1.destroy()
      this._tileManagerVT1 = null
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
    this._uDataTileT1 = null
    this._uDataTileV = null
    this._uDataTileVT1 = null
    this._uColorRamp = null
    this._uOpacity = null
    this._uTemporalMix = null
    this._uIsVector = null
    this._uValueMin = null
    this._uValueMax = null
    this._gl = null
    this._map = null
  }
}

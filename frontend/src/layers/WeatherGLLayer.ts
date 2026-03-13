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
import blurVertSource from './shaders/particle-update.vert.glsl?raw'
import blurFragSource from './shaders/blur.frag.glsl?raw'
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
  type TileFormat,
} from './TileManager'
import { getTileFetchClient } from '@/workers/TileFetchClient'

/** Layers that use U/V vector component tiles instead of scalar tiles. */
const VECTOR_LAYERS = new Set(['wind_speed'])

/** Layers that only have data over ocean — enables shader coastal fallback. */
const OCEAN_ONLY_LAYERS = new Set(['wave_height', 'wave_period', 'wave_direction'])

export interface WeatherGLLayerOptions {
  /** Unique layer ID for MapLibre. */
  id?: string
  /** Initial weather layer name for color ramp selection. */
  layer?: string
  /** Overlay opacity 0-1. Default: 0.7. */
  opacity?: number
  /** Base URL for the data tile API. Default: '' (same-origin). */
  apiBase?: string
  /** Tile format: 'png' (default) or 'f16' (Float16 binary). */
  tileFormat?: TileFormat
}

export class WeatherGLLayer implements CustomLayerInterface {
  readonly id: string
  readonly type = 'custom' as const
  readonly renderingMode = '2d' as const

  private _layerName: string
  private _opacity: number
  private _apiBase: string
  private _tileFormat: TileFormat
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

  // Diagnostic warning guards (one-shot to avoid console spam)
  private _renderSkipWarned = false
  private _noTilesWarned = false
  private _renderSuccessLogged = false

  // Blur post-processing
  private _blurProgram: GLProgram | null = null
  private _fboTexture: WebGLTexture | null = null
  private _fbo: WebGLFramebuffer | null = null
  private _fboWidth = 0
  private _fboHeight = 0
  private _uBlurTexture: WebGLUniformLocation | null = null
  private _uBlurTexelSize: WebGLUniformLocation | null = null
  private _uBlurRadius: WebGLUniformLocation | null = null
  private _uBlurOpacity: WebGLUniformLocation | null = null

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
  private _uOceanOnly: WebGLUniformLocation | null = null
  private _uIsFloat16: WebGLUniformLocation | null = null

  constructor(options: WeatherGLLayerOptions = {}) {
    this.id = options.id ?? 'weather-gl'
    this._layerName = options.layer ?? 'temperature'
    this._opacity = options.opacity ?? 0.7
    this._apiBase = options.apiBase ?? ''
    this._tileFormat = options.tileFormat ?? 'png'
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
      this._uOceanOnly = gl.getUniformLocation(prog, 'u_oceanOnly')
      this._uIsFloat16 = gl.getUniformLocation(prog, 'u_isFloat16')

      // Compile blur post-processing program
      this._blurProgram = createProgram(gl, blurVertSource, blurFragSource)
      const blurProg = this._blurProgram.program
      this._uBlurTexture = gl.getUniformLocation(blurProg, 'u_texture')
      this._uBlurTexelSize = gl.getUniformLocation(blurProg, 'u_texelSize')
      this._uBlurRadius = gl.getUniformLocation(blurProg, 'u_blurRadius')
      this._uBlurOpacity = gl.getUniformLocation(blurProg, 'u_opacity')

      this._createColorRamp(gl)

      // Create tile managers for data tile fetching + texture upload.
      // T0 = current forecast hour, T1 = next hour (for temporal interpolation).
      // In vector mode, the main managers fetch U-component tiles
      // and the V managers fetch V-component tiles.
      //
      // All managers share a single TileFetchClient (Web Worker) so that
      // network fetch callbacks never block the main thread render loop.
      const triggerRepaint = () => map.triggerRepaint()
      const fetchClient = getTileFetchClient()
      const tmOpts = { apiBase: this._apiBase, format: this._tileFormat, fetchClient }
      this._tileManager = new TileManager(gl, tmOpts)
      this._tileManager.onTileLoaded = triggerRepaint
      this._tileManagerT1 = new TileManager(gl, tmOpts)
      this._tileManagerT1.onTileLoaded = triggerRepaint
      this._tileManagerV = new TileManager(gl, tmOpts)
      this._tileManagerV.onTileLoaded = triggerRepaint
      this._tileManagerVT1 = new TileManager(gl, tmOpts)
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
    if (!(gl instanceof WebGL2RenderingContext)) return
    if (!this._program || !this._quad || !this._colorRampTexture ||
      !this._tileManager || !this._map ||
      !this._model || !this._runId
    ) {
      if (!this._renderSkipWarned) {
        this._renderSkipWarned = true
        const reason = !this._program ? 'shader compilation failed (_program is null)'
          : !this._colorRampTexture ? 'color ramp texture missing'
          : !this._tileManager ? 'tile manager not initialized'
          : !this._model || !this._runId ? 'no dataset config (model/runId not set)'
          : 'unknown'
        console.error(`[WeatherGLLayer] render() skipped: ${reason}`)
      }
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

    if (tilesToDraw.length === 0) {
      if (!this._noTilesWarned) {
        this._noTilesWarned = true
        console.warn(
          `[WeatherGLLayer] No tiles ready to draw (${visibleCoords.length} requested). Tiles may still be loading or all failed.`,
        )
      }
      return
    }
    // Reset so warning fires again if tiles disappear after being available
    this._noTilesWarned = false

    if (!this._renderSuccessLogged) {
      this._renderSuccessLogged = true
      console.info(
        `[WeatherGLLayer] Drawing ${tilesToDraw.length}/${visibleCoords.length} tiles at opacity=${this._opacity}`,
      )
    }

    // Save MapLibre's GL state (blend, program, active texture unit, per-unit bindings, FBO, viewport)
    // We use up to 5 texture units: 0=U/scalar, 1=colorRamp, 2=T1, 3=V, 4=VT1
    const prevFbo = gl.getParameter(gl.FRAMEBUFFER_BINDING) as WebGLFramebuffer | null
    const prevViewport = gl.getParameter(gl.VIEWPORT) as Int32Array
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

    // ── Phase 1: Render tiles to offscreen FBO ──────────────────

    // Resize FBO if canvas dimensions changed
    const fbW = gl.drawingBufferWidth
    const fbH = gl.drawingBufferHeight
    if (fbW !== this._fboWidth || fbH !== this._fboHeight) {
      this._resizeFBO(gl, fbW, fbH)
    }

    // Bind offscreen FBO and clear
    gl.bindFramebuffer(gl.FRAMEBUFFER, this._fbo)
    gl.viewport(0, 0, this._fboWidth, this._fboHeight)
    gl.clearColor(0, 0, 0, 0)
    gl.clear(gl.COLOR_BUFFER_BIT)

    // Disable blending for the tile pass — tiles composite additively in the FBO
    gl.disable(gl.BLEND)

    gl.useProgram(this._program.program)

    // MapLibre's modelViewProjectionMatrix transforms from world-space pixels
    // [0, worldSize] to clip space. Our vertex shader uses mercator [0, 1]
    // coordinates, so we right-multiply by diag(worldSize) to convert
    // mercator → world-space before the projection.
    const worldSize = 512 * Math.pow(2, this._map!.getZoom())
    const mvp = options.modelViewProjectionMatrix
    const mercatorMatrix = new Float64Array(16)
    // Right-multiply columns 0,1 by worldSize; column 2 by 1; column 3 unchanged
    for (let i = 0; i < 4; i++) {
      mercatorMatrix[i]      = mvp[i]      * worldSize  // column 0 (x)
      mercatorMatrix[4 + i]  = mvp[4 + i]  * worldSize  // column 1 (y)
      mercatorMatrix[8 + i]  = mvp[8 + i]               // column 2 (z, unchanged)
      mercatorMatrix[12 + i] = mvp[12 + i]              // column 3 (translation, unchanged)
    }

    // Set shared uniforms (same for all tiles)
    gl.uniformMatrix4fv(this._uMatrix, false, mercatorMatrix)
    gl.uniform1f(this._uOpacity, 1.0) // opacity applied in blur pass
    gl.uniform1i(this._uIsVector, isVector ? 1 : 0)
    gl.uniform1i(this._uOceanOnly, OCEAN_ONLY_LAYERS.has(this._layerName) ? 1 : 0)
    gl.uniform1i(this._uIsFloat16, this._tileFormat === 'f16' ? 1 : 0)

    // Pass value range for denormalization.
    // Vector mode: Wind U/V components use symmetric range [-max, +max].
    // Scalar mode with Float16: pass physical range for normalization.
    // Scalar mode with PNG: values are pre-normalized [0,1].
    if (isVector) {
      const ramp = COLOR_RAMPS[this._layerName]
      const max = ramp?.valueMax ?? 50
      gl.uniform1f(this._uValueMin, -max)
      gl.uniform1f(this._uValueMax, max)
    } else if (this._tileFormat === 'f16') {
      const ramp = COLOR_RAMPS[this._layerName]
      gl.uniform1f(this._uValueMin, ramp?.valueMin ?? 0)
      gl.uniform1f(this._uValueMax, ramp?.valueMax ?? 1)
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
      // In vector mode, require BOTH U T1 and V T1 for temporal blending.
      // If V T1 hasn't loaded yet, sampling null TEXTURE4 decodes as 0.0
      // (not nodata), which denormalizes to -valueMax → huge speed → red flash.
      const canBlendT1 = texT1 != null && (!isVector || texVT1 != null)
      if (canBlendT1) {
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

    // ── Phase 2: Blur composite to screen ───────────────────────

    // Restore MapLibre's FBO and viewport
    gl.bindFramebuffer(gl.FRAMEBUFFER, prevFbo)
    gl.viewport(prevViewport[0], prevViewport[1], prevViewport[2], prevViewport[3])

    // Compute zoom-dependent blur radius: z3→3.0, z4→2.25, z5→1.5, z6→0.75, z7+→0.0
    const blurRadius = Math.max(0, Math.min(3, (7 - zoom) * 0.75))

    // Enable premultiplied-alpha blending for compositing over the basemap
    gl.enable(gl.BLEND)
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA)

    gl.useProgram(this._blurProgram!.program)

    // Bind the offscreen FBO texture to unit 0
    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this._fboTexture)
    gl.uniform1i(this._uBlurTexture, 0)
    gl.uniform2f(this._uBlurTexelSize, 1.0 / this._fboWidth, 1.0 / this._fboHeight)
    gl.uniform1f(this._uBlurRadius, blurRadius)
    gl.uniform1f(this._uBlurOpacity, this._opacity)

    // Draw fullscreen quad (same VAO — compatible attribute layout)
    gl.bindVertexArray(this._quad.vao)
    gl.drawArrays(gl.TRIANGLES, 0, this._quad.vertexCount)
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
    // If advanceForecastHour already set all these fields synchronously,
    // skip _applyLayerConfig to avoid clearing T0's freshly-swapped tiles.
    const configUnchanged = model === this._model && runId === this._runId &&
      layer === this._layerName && forecastHour === this._forecastHour
    this._model = model
    this._runId = runId
    this._layerName = layer
    this._forecastHour = forecastHour

    if (layerChanged && this._gl) {
      this._createColorRamp(this._gl)
    }

    // Reset render-skip warning so future failures are not silenced
    this._renderSkipWarned = false

    if (!configUnchanged) {
      this._applyLayerConfig()
    }
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

  /**
   * Synchronously advance the forecast hour, swapping T0↔T1 if T1 has
   * the target hour's tiles pre-fetched. Called from the RAF playback loop
   * BEFORE setTemporalBlend reconfigures T1 for the next-next hour —
   * this prevents the race where React's async effect fires too late and
   * finds T1 already pointing at a different hour.
   */
  advanceForecastHour(newHour: number): void {
    this._forecastHour = newHour
    const targetLayer = this._isVector ? 'wind_u' : this._layerName

    // T1 should have newHour's tiles (pre-fetched during blend).
    // Swap T0↔T1 so T0 now serves newHour immediately.
    if (
      this._tileManagerT1 &&
      this._tileManagerT1.cacheSize > 0 &&
      this._tileManagerT1.currentLayer === targetLayer &&
      this._tileManagerT1.currentForecastHour === newHour &&
      // Vector mode: V T1 must also be ready — otherwise swapping
      // produces U/V mismatch (U has tiles, V doesn't).
      (!this._isVector || (
        this._tileManagerVT1 != null &&
        this._tileManagerVT1.cacheSize > 0 &&
        this._tileManagerVT1.currentForecastHour === newHour
      ))
    ) {
      ;[this._tileManager, this._tileManagerT1] = [this._tileManagerT1, this._tileManager]
      if (this._isVector) {
        ;[this._tileManagerV, this._tileManagerVT1] = [this._tileManagerVT1, this._tileManagerV]
      }
    }
    // If swap failed: T0 keeps old tiles. render() still draws them
    // (they're loaded textures). setConfig will clear + re-fetch later.
    this._temporalMix = 0
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

    const targetLayer = this._isVector ? 'wind_u' : this._layerName

    // During playback advancement, T1 already has the target forecast hour's
    // tiles loaded (they were pre-fetched for temporal blending). Swap T0↔T1
    // instead of clearing and re-fetching — gives instant transitions.
    if (
      this._tileManagerT1 &&
      this._tileManagerT1.cacheSize > 0 &&
      this._tileManagerT1.currentLayer === targetLayer &&
      this._tileManagerT1.currentForecastHour === this._forecastHour &&
      (!this._isVector || (
        this._tileManagerVT1 != null &&
        this._tileManagerVT1.cacheSize > 0 &&
        this._tileManagerVT1.currentForecastHour === this._forecastHour
      ))
    ) {
      const tmpT = this._tileManager
      this._tileManager = this._tileManagerT1
      this._tileManagerT1 = tmpT

      if (this._isVector) {
        const tmpV = this._tileManagerV
        this._tileManagerV = this._tileManagerVT1
        this._tileManagerVT1 = tmpV
      }
      // Old T0 (now T1) will be reconfigured by the next setTemporalBlend call.
      return
    }

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

  /** Create or resize the offscreen FBO for blur post-processing. */
  private _resizeFBO(gl: WebGL2RenderingContext, width: number, height: number): void {
    if (this._fboTexture) gl.deleteTexture(this._fboTexture)
    if (this._fbo) gl.deleteFramebuffer(this._fbo)

    const tex = gl.createTexture()
    if (!tex) throw new Error('Failed to create blur FBO texture')
    gl.bindTexture(gl.TEXTURE_2D, tex)
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, width, height, 0, gl.RGBA, gl.UNSIGNED_BYTE, null)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
    gl.bindTexture(gl.TEXTURE_2D, null)

    const fbo = gl.createFramebuffer()
    if (!fbo) throw new Error('Failed to create blur FBO')
    gl.bindFramebuffer(gl.FRAMEBUFFER, fbo)
    gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, tex, 0)
    const status = gl.checkFramebufferStatus(gl.FRAMEBUFFER)
    if (status !== gl.FRAMEBUFFER_COMPLETE) {
      throw new Error(`Blur FBO incomplete: 0x${status.toString(16)}`)
    }
    gl.bindFramebuffer(gl.FRAMEBUFFER, null)

    this._fboTexture = tex
    this._fbo = fbo
    this._fboWidth = width
    this._fboHeight = height
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
      if (this._fbo) {
        gl.deleteFramebuffer(this._fbo)
        this._fbo = null
      }
      if (this._fboTexture) {
        gl.deleteTexture(this._fboTexture)
        this._fboTexture = null
      }
      if (this._blurProgram) {
        deleteProgram(gl, this._blurProgram)
        this._blurProgram = null
      }
    }
    this._fboWidth = 0
    this._fboHeight = 0
    this._uBlurTexture = null
    this._uBlurTexelSize = null
    this._uBlurRadius = null
    this._uBlurOpacity = null
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
    this._uOceanOnly = null
    this._uIsFloat16 = null
    this._gl = null
    this._map = null
  }
}

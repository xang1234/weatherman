/**
 * GPU wind-particle layer with trail rendering.
 *
 * Implements a three-pass pipeline per frame inside MapLibre's render loop:
 *
 *   Pass 1 — State Update (ping-pong)
 *     Read particle positions from state texture A, write updated positions
 *     to state texture B via a fullscreen-quad fragment shader, then swap.
 *     Particles are advected by sampling wind U/V data from a tile atlas
 *     that packs all visible tiles into a single texture per component.
 *
 *   Pass 2 — Trail Composite (ping-pong)
 *     Bind trail write FBO. Draw the previous trail texture at 95% opacity
 *     (exponential decay ≈ 2s fade at 60fps). Then draw particles on top
 *     as GL_POINTS using gl_VertexID to read from the state texture.
 *
 *   Pass 3 — Map Composite
 *     Restore MapLibre's FBO. Draw the trail texture as a screen-aligned
 *     fullscreen quad with premultiplied alpha blending.
 *
 * All state stays on the GPU — zero CPU readback in the hot path.
 */

import type {
  CustomLayerInterface,
  CustomRenderMethodInput,
  Map as MaplibreMap,
} from 'maplibre-gl'

import updateVertSource from './shaders/particle-update.vert.glsl?raw'
import updateFragSource from './shaders/particle-update.frag.glsl?raw'
import drawVertSource from './shaders/particle-draw.vert.glsl?raw'
import drawFragSource from './shaders/particle-draw.frag.glsl?raw'
import trailFragSource from './shaders/trail-composite.frag.glsl?raw'
import {
  createFullscreenQuad,
  createProgram,
  deleteProgram,
  deleteQuadGeometry,
  type GLProgram,
  type QuadGeometry,
} from './gl-utils'
import {
  TileManager,
  PanVelocityTracker,
  computeVisibleTiles,
  computePanPrefetchTiles,
  type TileCoord,
  type TileFormat,
} from './TileManager'
import { getTileFetchClient } from '@/workers/TileFetchClient'
import { detectGpuTier, clampStateSize, type GpuTier } from './gpu-tier'
import { ensureParticleDebugState, type ParticleDebugState } from './particleDebug'

/** Default particles per axis (used if no stateSize option and detection unavailable). */
const DEFAULT_STATE_SIZE = 50
/** Trail fade factor per frame. 0.93^60 ≈ 0.013 → ~1.5s trails for broken drop effect. */
const TRAIL_FADE = 0.93
/**
 * Target particle displacement in screen pixels per frame for a reference 10 m/s wind.
 * speedScale = TARGET_DISP_PX / (REF_WIND * dt * worldSize)
 * This produces zoom-independent apparent speed.
 */
const TARGET_DISP_PX = 0.5
/** Reference wind speed (m/s) for the target displacement calculation. */
const REF_WIND_MPS = 10.0

/** Maximum expected wind speed (m/s) for normalizing speed → alpha in the draw shader. */
const SPEED_MAX = 50.0

/** Fixed point size in CSS pixels — zoom-independent. */
const POINT_SIZE = 1.7

/** Frames of frame-time history for the performance watchdog. */
const PERF_WINDOW = 60

/** Frame time threshold in ms — if rolling average exceeds this, log a warning. */
const PERF_WARN_THRESHOLD_MS = 20

/** Tile texture dimensions (standard web map tiles). */
const TILE_SIZE = 256

export interface WindParticleLayerOptions {
  /** Unique layer ID for MapLibre. */
  id?: string
  /** Overlay opacity 0-1. Default: 1.0. */
  opacity?: number
  /** Base URL for the data tile API. Default: '' (same-origin). */
  apiBase?: string
  /** Tile format: 'png' (default) or 'f16' (Float16 binary). */
  tileFormat?: TileFormat
  /**
   * Override particle state texture size (particles = stateSize²).
   * Must be a power of 2. If omitted, auto-detected from GPU capability.
   * Valid range: 16-512 (rounded to nearest multiple of 8).
   */
  stateSize?: number
}

interface AtlasLayout {
  cols: number
  rows: number
  originX: number
  originY: number
  zoom: number
  hasAnyTile: boolean
}

interface AtlasSlot {
  col: number
  row: number
}

export class WindParticleLayer implements CustomLayerInterface {
  readonly id: string
  readonly type = 'custom' as const
  readonly renderingMode = '2d' as const

  private _map: MaplibreMap | null = null
  private _gl: WebGL2RenderingContext | null = null
  private _opacity: number
  private _active = false
  private _apiBase: string
  private _tileFormat: TileFormat
  private _maxTextureSize = 4096
  private _debug: ParticleDebugState

  // ── Adaptive particle count ───────────────────────────────────────
  private _stateSize: number
  private _particleCount: number
  private _gpuTier: GpuTier = 'medium'
  private _stateSizeOverride: number | undefined

  // ── Wind data tile managers ─────────────────────────────────────────
  private _windUManager: TileManager | null = null
  private _windVManager: TileManager | null = null
  private _windUT1Manager: TileManager | null = null
  private _windVT1Manager: TileManager | null = null
  private _windConfigured = false

  // Wind dataset config
  private _model = ''
  private _runId = ''
  private _forecastHourT1 = -1
  private _temporalMix = 0
  private _valueMin = -50
  private _valueMax = 50

  // ── State update pass ───────────────────────────────────────────────
  private _updateProgram: GLProgram | null = null
  private _quad: QuadGeometry | null = null
  private _stateTextures: [WebGLTexture | null, WebGLTexture | null] | null = null
  private _stateFbos: [WebGLFramebuffer | null, WebGLFramebuffer | null] | null = null
  private _stateReadIndex = 0

  // Update uniforms
  private _uUpdateStateTex: WebGLUniformLocation | null = null
  private _uUpdateWindU: WebGLUniformLocation | null = null
  private _uUpdateWindV: WebGLUniformLocation | null = null
  private _uUpdateWindUT1: WebGLUniformLocation | null = null
  private _uUpdateWindVT1: WebGLUniformLocation | null = null
  private _uUpdateTemporalMix: WebGLUniformLocation | null = null
  private _uUpdateHasWindData: WebGLUniformLocation | null = null
  private _uUpdateIsFloat16: WebGLUniformLocation | null = null
  private _uUpdateValueMin: WebGLUniformLocation | null = null
  private _uUpdateValueMax: WebGLUniformLocation | null = null
  private _uUpdateSpeedScale: WebGLUniformLocation | null = null
  private _uUpdateDt: WebGLUniformLocation | null = null
  private _uUpdateSeed: WebGLUniformLocation | null = null
  private _uUpdateViewportBounds: WebGLUniformLocation | null = null
  private _uUpdateAtlasOriginX: WebGLUniformLocation | null = null
  private _uUpdateAtlasOriginY: WebGLUniformLocation | null = null
  private _uUpdateAtlasZoom: WebGLUniformLocation | null = null
  private _uUpdateAtlasCols: WebGLUniformLocation | null = null
  private _uUpdateAtlasRows: WebGLUniformLocation | null = null

  // ── Particle draw pass ──────────────────────────────────────────────
  private _drawProgram: GLProgram | null = null
  private _drawVao: WebGLVertexArrayObject | null = null

  // Draw uniforms
  private _uDrawStateTex: WebGLUniformLocation | null = null
  private _uDrawMatrix: WebGLUniformLocation | null = null
  private _uDrawPointSize: WebGLUniformLocation | null = null
  private _uDrawSpeedMax: WebGLUniformLocation | null = null

  // ── Trail composite pass ────────────────────────────────────────────
  private _compositeProgram: GLProgram | null = null

  // Composite uniforms
  private _uCompositeTexture: WebGLUniformLocation | null = null
  private _uCompositeOpacity: WebGLUniformLocation | null = null

  // Trail ping-pong (canvas-sized RGBA8)
  private _trailTextures: [WebGLTexture | null, WebGLTexture | null] | null = null
  private _trailFbos: [WebGLFramebuffer | null, WebGLFramebuffer | null] | null = null
  private _trailReadIndex = 0
  private _trailWidth = 0
  private _trailHeight = 0

  // ── Tile atlas ──────────────────────────────────────────────────────
  private _atlasU: WebGLTexture | null = null
  private _atlasV: WebGLTexture | null = null
  private _atlasUT1: WebGLTexture | null = null
  private _atlasVT1: WebGLTexture | null = null
  private _atlasWidth = 0
  private _atlasHeight = 0
  private _copyFbo: WebGLFramebuffer | null = null
  private _atlasFbo: WebGLFramebuffer | null = null
  private _atlasLayout: AtlasLayout | null = null
  private _atlasLayoutKey = ''
  private _atlasSlotsByKey = new Map<string, AtlasSlot[]>()
  private _atlasVisibleKeys: string[] = []
  private _atlasDirtyT0 = new Set<string>()
  private _atlasDirtyT1 = new Set<string>()

  // ── Pan prefetch ───────────────────────────────────────────────────
  private _panTracker = new PanVelocityTracker()

  // ── Timing & performance watchdog ──────────────────────────────────
  private _lastFrameTime = 0
  private _frameCount = 0
  private _frameTimes: number[] = []
  private _perfWarned = false

  constructor(options: WindParticleLayerOptions = {}) {
    this.id = options.id ?? 'wind-particles'
    this._opacity = options.opacity ?? 1.0
    this._apiBase = options.apiBase ?? ''
    this._tileFormat = options.tileFormat ?? 'png'
    this._debug = ensureParticleDebugState('wind')
    this._debug.active = this._active
    this._stateSizeOverride = options.stateSize
    // Temporary defaults — overwritten by GPU detection in onAdd()
    this._stateSize = DEFAULT_STATE_SIZE
    this._particleCount = DEFAULT_STATE_SIZE * DEFAULT_STATE_SIZE
  }

  // ── CustomLayerInterface ────────────────────────────────────────────

  onAdd(map: MaplibreMap, gl: WebGLRenderingContext | WebGL2RenderingContext): void {
    if (!(gl instanceof WebGL2RenderingContext)) {
      console.error('[WindParticleLayer] WebGL2 is required')
      return
    }

    this._map = map
    this._gl = gl
    this._debug.mounts += 1
    this._debug.active = this._active

    const ext = gl.getExtension('EXT_color_buffer_float')
    if (!ext) {
      console.error('[WindParticleLayer] EXT_color_buffer_float not supported')
      return
    }

    this._maxTextureSize = gl.getParameter(gl.MAX_TEXTURE_SIZE) as number

    // ── Adaptive particle count: detect GPU tier ──
    if (this._stateSizeOverride != null) {
      this._stateSize = clampStateSize(this._stateSizeOverride)
    } else {
      const tier = detectGpuTier(gl)
      this._gpuTier = tier.tier
      // Wind-specific: ~30% of tier baseline (sqrt(0.3) ≈ 0.548 on stateSize axis)
      // for a calmer, less dense field. Waves use the unscaled tier value.
      this._stateSize = clampStateSize(Math.round(tier.stateSize * 0.548))
      console.info(
        `[WindParticleLayer] GPU: "${tier.renderer}" → tier=${tier.tier}, ` +
        `stateSize=${this._stateSize} (${this._stateSize ** 2} particles)`
      )
    }
    this._particleCount = this._stateSize * this._stateSize

    try {
      this._initResources(gl)
      this._initTileManagers(gl)
    } catch (e) {
      console.error('[WindParticleLayer] Initialization failed:', e)
      this._cleanup()
    }
  }

  render(
    gl: WebGLRenderingContext | WebGL2RenderingContext,
    options: CustomRenderMethodInput,
  ): void {
    if (
      !this._updateProgram || !this._drawProgram || !this._compositeProgram ||
      !this._quad || !this._drawVao ||
      !this._stateTextures || !this._stateFbos ||
      !this._map || !(gl instanceof WebGL2RenderingContext)
    ) {
      return
    }

    if (!this._active || this._opacity <= 0 || !this._windConfigured) {
      return
    }

    // ── Timing & performance watchdog ──
    const now = performance.now() / 1000
    const dt = this._lastFrameTime > 0 ? Math.min(now - this._lastFrameTime, 0.1) : 0.016
    this._lastFrameTime = now
    this._frameCount++

    // Track frame times for performance monitoring
    const frameTimeMs = dt * 1000
    this._frameTimes.push(frameTimeMs)
    if (this._frameTimes.length > PERF_WINDOW) {
      this._frameTimes.shift()
    }
    if (!this._perfWarned && this._frameTimes.length === PERF_WINDOW) {
      const avg = this._frameTimes.reduce((a, b) => a + b, 0) / PERF_WINDOW
      if (avg > PERF_WARN_THRESHOLD_MS) {
        console.warn(
          `[WindParticleLayer] Low FPS detected: avg frame time ${avg.toFixed(1)}ms ` +
          `(${(1000 / avg).toFixed(0)} fps) with ${this._particleCount} particles ` +
          `(tier=${this._gpuTier}). Consider reducing stateSize.`
        )
        this._perfWarned = true
      }
    }

    // ── Update wind tile managers with current viewport ──
    const zoom = Math.max(0, Math.min(8, Math.floor(this._map.getZoom())))
    const mapBounds = this._map.getBounds()
    const visibleCoords = computeVisibleTiles({
      west: mapBounds.getWest(),
      north: mapBounds.getNorth(),
      east: mapBounds.getEast(),
      south: mapBounds.getSouth(),
    }, zoom)

    // Pan prefetch: detect movement and prefetch one tile ring ahead
    let panEast = mapBounds.getEast()
    const panWest = mapBounds.getWest()
    if (panEast < panWest) panEast += 360
    const centerLng = (panWest + panEast) / 2
    const centerLat = (mapBounds.getNorth() + mapBounds.getSouth()) / 2
    const panDir = this._panTracker.update(centerLng, centerLat)
    const prefetchCoords = panDir
      ? computePanPrefetchTiles(visibleCoords, panDir, zoom)
      : []

    if (this._windConfigured) {
      // Priority: P0 = visible current time, P1 = visible next time, P2 = prefetch
      this._windUManager?.updateVisibleTiles(visibleCoords, 0)
      this._windVManager?.updateVisibleTiles(visibleCoords, 0)
      if (prefetchCoords.length > 0) {
        this._windUManager?.updateVisibleTiles(prefetchCoords, 2)
        this._windVManager?.updateVisibleTiles(prefetchCoords, 2)
      }
      if (this._forecastHourT1 >= 0 && this._temporalMix > 0) {
        this._windUT1Manager?.updateVisibleTiles(visibleCoords, 1)
        this._windVT1Manager?.updateVisibleTiles(visibleCoords, 1)
        if (prefetchCoords.length > 0) {
          this._windUT1Manager?.updateVisibleTiles(prefetchCoords, 2)
          this._windVT1Manager?.updateVisibleTiles(prefetchCoords, 2)
        }
      }
    }

    // ── Save MapLibre GL state BEFORE any GL calls ──
    const prevProgram = gl.getParameter(gl.CURRENT_PROGRAM) as WebGLProgram | null
    const prevFbo = gl.getParameter(gl.FRAMEBUFFER_BINDING) as WebGLFramebuffer | null
    const prevActiveTexture = gl.getParameter(gl.ACTIVE_TEXTURE) as number
    const prevViewport = gl.getParameter(gl.VIEWPORT) as Int32Array
    const prevBlend = gl.getParameter(gl.BLEND) as boolean
    const prevBlendSrc = gl.getParameter(gl.BLEND_SRC_RGB) as number
    const prevBlendDst = gl.getParameter(gl.BLEND_DST_RGB) as number
    const prevBlendSrcA = gl.getParameter(gl.BLEND_SRC_ALPHA) as number
    const prevBlendDstA = gl.getParameter(gl.BLEND_DST_ALPHA) as number

    // ── Pack visible tiles into atlas textures ──
    const atlas = this._packAtlas(gl, visibleCoords)
    const hasWindData = atlas?.hasAnyTile ?? false

    // ── Resize trail textures if canvas size changed ──
    const canvasW = gl.drawingBufferWidth
    const canvasH = gl.drawingBufferHeight
    if (canvasW !== this._trailWidth || canvasH !== this._trailHeight) {
      this._resizeTrailTextures(gl, canvasW, canvasH)
    }

    if (!this._trailTextures || !this._trailFbos) return

    // Compute worldSize once — used in both update (speed scale) and draw (matrix) passes.
    const worldSize = 512 * Math.pow(2, this._map.getZoom())

    // ────────────────────────────────────────────────────────────────
    // Pass 1: State Update (ping-pong)
    // ────────────────────────────────────────────────────────────────
    const stateRead = this._stateReadIndex
    const stateWrite = 1 - stateRead

    gl.bindFramebuffer(gl.FRAMEBUFFER, this._stateFbos[stateWrite])
    gl.viewport(0, 0, this._stateSize, this._stateSize)
    gl.disable(gl.BLEND)

    gl.useProgram(this._updateProgram.program)

    // Bind state texture to unit 0
    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this._stateTextures[stateRead])
    gl.uniform1i(this._uUpdateStateTex, 0)

    // Bind atlas textures to units 1-4
    gl.activeTexture(gl.TEXTURE1)
    gl.bindTexture(gl.TEXTURE_2D, atlas ? this._atlasU : null)
    gl.uniform1i(this._uUpdateWindU, 1)

    gl.activeTexture(gl.TEXTURE2)
    gl.bindTexture(gl.TEXTURE_2D, atlas ? this._atlasV : null)
    gl.uniform1i(this._uUpdateWindV, 2)

    const hasT1 = this._forecastHourT1 >= 0 && this._temporalMix > 0

    gl.activeTexture(gl.TEXTURE3)
    gl.bindTexture(gl.TEXTURE_2D, atlas && hasT1 ? this._atlasUT1 : null)
    gl.uniform1i(this._uUpdateWindUT1, 3)

    gl.activeTexture(gl.TEXTURE4)
    gl.bindTexture(gl.TEXTURE_2D, atlas && hasT1 ? this._atlasVT1 : null)
    gl.uniform1i(this._uUpdateWindVT1, 4)

    // Set uniforms
    gl.uniform1f(this._uUpdateTemporalMix, hasWindData && hasT1 ? this._temporalMix : 0)
    gl.uniform1i(this._uUpdateHasWindData, hasWindData ? 1 : 0)
    gl.uniform1i(this._uUpdateIsFloat16, this._tileFormat === 'f16' ? 1 : 0)
    gl.uniform1f(this._uUpdateValueMin, this._valueMin)
    gl.uniform1f(this._uUpdateValueMax, this._valueMax)
    // Zoom-adaptive speed scale: ensure particles move TARGET_DISP_PX per frame for REF_WIND_MPS.
    // speedScale = TARGET_DISP_PX / (refWind * dt * worldSize)  [mercator units / (m/s · s)]
    // The shader receives the same clamped dt used in the speedScale formula.
    const safeDt = Math.max(dt, 0.004)
    const speedScale = TARGET_DISP_PX / (REF_WIND_MPS * safeDt * worldSize)
    gl.uniform1f(this._uUpdateSpeedScale, speedScale)
    gl.uniform1f(this._uUpdateDt, safeDt)
    gl.uniform1f(this._uUpdateSeed, (now * 137.0) % 1000.0)

    const bounds = this._map.getBounds()
    // When crossing the antimeridian, east < west (e.g. west=170°, east=-170°).
    // Adding 360° to east gives a contiguous range (e.g. vpMinLon=0.97, vpMaxLon=1.03)
    // so the shader's out-of-bounds check works correctly.
    let vpEast = bounds.getEast()
    const vpWest = bounds.getWest()
    if (vpEast < vpWest) vpEast += 360
    const vpMinLon = (vpWest + 180) / 360
    const vpMaxLon = (vpEast + 180) / 360
    const vpMinLat = this._latToMercatorY(bounds.getNorth())
    const vpMaxLat = this._latToMercatorY(bounds.getSouth())

    // Particles spawn across full viewport — atlas covers all visible tiles
    gl.uniform4f(this._uUpdateViewportBounds, vpMinLon, vpMinLat, vpMaxLon, vpMaxLat)

    // Atlas uniforms
    if (atlas) {
      gl.uniform1f(this._uUpdateAtlasOriginX, atlas.originX)
      gl.uniform1f(this._uUpdateAtlasOriginY, atlas.originY)
      gl.uniform1f(this._uUpdateAtlasZoom, Math.pow(2, atlas.zoom))
      gl.uniform1f(this._uUpdateAtlasCols, atlas.cols)
      gl.uniform1f(this._uUpdateAtlasRows, atlas.rows)
    } else {
      gl.uniform1f(this._uUpdateAtlasOriginX, 0)
      gl.uniform1f(this._uUpdateAtlasOriginY, 0)
      gl.uniform1f(this._uUpdateAtlasZoom, 1)
      gl.uniform1f(this._uUpdateAtlasCols, 1)
      gl.uniform1f(this._uUpdateAtlasRows, 1)
    }

    gl.bindVertexArray(this._quad.vao)
    gl.drawArrays(gl.TRIANGLES, 0, this._quad.vertexCount)
    gl.bindVertexArray(null)

    this._stateReadIndex = stateWrite

    // ────────────────────────────────────────────────────────────────
    // Pass 2: Trail Composite (ping-pong)
    //   2a. Draw faded previous trail
    //   2b. Draw particles on top
    // ────────────────────────────────────────────────────────────────
    const trailRead = this._trailReadIndex
    const trailWrite = 1 - trailRead

    gl.bindFramebuffer(gl.FRAMEBUFFER, this._trailFbos[trailWrite])
    gl.viewport(0, 0, this._trailWidth, this._trailHeight)
    gl.clearColor(0, 0, 0, 0)
    gl.clear(gl.COLOR_BUFFER_BIT)

    // 2a: Draw previous trail at TRAIL_FADE opacity
    gl.enable(gl.BLEND)
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA) // premultiplied alpha

    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this._trailTextures[trailRead])

    gl.useProgram(this._compositeProgram.program)
    gl.uniform1i(this._uCompositeTexture, 0)
    gl.uniform1f(this._uCompositeOpacity, TRAIL_FADE)

    gl.bindVertexArray(this._quad.vao)
    gl.drawArrays(gl.TRIANGLES, 0, this._quad.vertexCount)
    gl.bindVertexArray(null)

    // 2b: Draw particles as GL_POINTS — trail fade shows movement direction
    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this._stateTextures![this._stateReadIndex])

    gl.useProgram(this._drawProgram.program)
    gl.uniform1i(this._uDrawStateTex, 0)
    gl.uniform1f(this._uDrawPointSize, POINT_SIZE)
    gl.uniform1f(this._uDrawSpeedMax, SPEED_MAX)

    // MapLibre's modelViewProjectionMatrix transforms from world coordinates
    // [0, worldSize] to clip space. Our particles use mercator [0, 1], so we
    // right-multiply columns 0,1 by worldSize to convert the matrix.
    const mvp = options.modelViewProjectionMatrix
    const mercatorMatrix = new Float32Array(16)
    for (let i = 0; i < 4; i++) {
      mercatorMatrix[i]      = mvp[i]      * worldSize  // column 0 (x)
      mercatorMatrix[4 + i]  = mvp[4 + i]  * worldSize  // column 1 (y)
      mercatorMatrix[8 + i]  = mvp[8 + i]               // column 2 (z)
      mercatorMatrix[12 + i] = mvp[12 + i]              // column 3 (translation)
    }
    gl.uniformMatrix4fv(this._uDrawMatrix, false, mercatorMatrix)

    gl.bindVertexArray(this._drawVao)
    gl.drawArrays(gl.POINTS, 0, this._particleCount)
    gl.bindVertexArray(null)

    this._trailReadIndex = trailWrite

    // ────────────────────────────────────────────────────────────────
    // Pass 3: Map Composite
    //   Draw trail texture onto MapLibre's framebuffer
    // ────────────────────────────────────────────────────────────────
    gl.bindFramebuffer(gl.FRAMEBUFFER, prevFbo)
    gl.viewport(prevViewport[0], prevViewport[1], prevViewport[2], prevViewport[3])

    gl.enable(gl.BLEND)
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA) // premultiplied alpha

    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this._trailTextures[this._trailReadIndex])

    gl.useProgram(this._compositeProgram.program)
    gl.uniform1i(this._uCompositeTexture, 0)
    gl.uniform1f(this._uCompositeOpacity, this._opacity)

    gl.bindVertexArray(this._quad.vao)
    gl.drawArrays(gl.TRIANGLES, 0, this._quad.vertexCount)
    gl.bindVertexArray(null)

    // ── Restore MapLibre GL state ──
    gl.activeTexture(prevActiveTexture)
    if (prevBlend) {
      gl.enable(gl.BLEND)
      gl.blendFuncSeparate(prevBlendSrc, prevBlendDst, prevBlendSrcA, prevBlendDstA)
    } else {
      gl.disable(gl.BLEND)
    }
    gl.useProgram(prevProgram)

    // Request next frame only when animating (wind data active or mid-transition)
    if (hasWindData || this._temporalMix > 0) {
      this._map.triggerRepaint()
    }
  }

  onRemove(
    _map: MaplibreMap,
    _gl: WebGLRenderingContext | WebGL2RenderingContext,
  ): void {
    this._cleanup()
  }

  // ── Public API ──────────────────────────────────────────────────────

  /** Configure the wind dataset to fetch tiles from. */
  setWindConfig(model: string, runId: string, forecastHour: number, valueMin: number, valueMax: number): void {
    this._model = model
    this._runId = runId
    this._valueMin = valueMin
    this._valueMax = valueMax

    this._windUManager?.setLayer(model, runId, 'wind_u', forecastHour)
    this._windVManager?.setLayer(model, runId, 'wind_v', forecastHour)
    this._windConfigured = true
    this._invalidateAtlas()
    this._map?.triggerRepaint()
  }

  /** Set temporal interpolation for the particle wind field. */
  setTemporalBlend(forecastHourT1: number, mix: number): void {
    const shouldInvalidateAtlas =
      forecastHourT1 !== this._forecastHourT1 ||
      (forecastHourT1 >= 0) !== (this._forecastHourT1 >= 0)
    this._forecastHourT1 = forecastHourT1
    this._temporalMix = Math.max(0, Math.min(1, mix))

    if (forecastHourT1 >= 0 && this._model && this._runId) {
      this._windUT1Manager?.setLayer(this._model, this._runId, 'wind_u', forecastHourT1)
      this._windVT1Manager?.setLayer(this._model, this._runId, 'wind_v', forecastHourT1)
    }
    if (shouldInvalidateAtlas) {
      this._invalidateAtlas()
    }
    this._map?.triggerRepaint()
  }

  /** Synchronously advance the forecast hour, swapping T0↔T1. */
  advanceForecastHour(newHour: number): void {
    if (
      this._windUT1Manager &&
      this._windUT1Manager.cacheSize > 0 &&
      this._windUT1Manager.currentForecastHour === newHour &&
      this._windVT1Manager &&
      this._windVT1Manager.cacheSize > 0 &&
      this._windVT1Manager.currentForecastHour === newHour
    ) {
      // T1 tiles are ready — swap T0 ↔ T1 for seamless transition
      ;[this._windUManager, this._windUT1Manager] = [this._windUT1Manager, this._windUManager]
      ;[this._windVManager, this._windVT1Manager] = [this._windVT1Manager, this._windVManager]
    } else {
      // T1 tiles not ready — reconfigure T0 for the new hour (starts fresh fetch)
      console.debug(
        `[WindParticleLayer] advanceForecastHour: T1 tiles not ready for hour ${newHour}, reconfiguring T0`
      )
      if (this._model && this._runId) {
        this._windUManager?.setLayer(this._model, this._runId, 'wind_u', newHour)
        this._windVManager?.setLayer(this._model, this._runId, 'wind_v', newHour)
      }
    }
    this._temporalMix = 0
    this._invalidateAtlas()
    this._map?.triggerRepaint()
  }

  /** Check if T1 tiles have at least partial data for seamless playback advance. */
  isT1Ready(): boolean {
    return (
      (this._windUT1Manager?.cacheSize ?? 0) > 0 &&
      (this._windVT1Manager?.cacheSize ?? 0) > 0
    )
  }

  /** Get the current "read" state texture. */
  get stateTexture(): WebGLTexture | null {
    return this._stateTextures?.[this._stateReadIndex] ?? null
  }

  /** State texture dimensions. */
  get stateSize(): number {
    return this._stateSize
  }

  /** Total particle count. */
  get particleCount(): number {
    return this._particleCount
  }

  /** Detected or configured GPU tier. */
  get gpuTier(): GpuTier {
    return this._gpuTier
  }

  /** Update overlay opacity at runtime. */
  setOpacity(opacity: number): void {
    this._opacity = Math.max(0, Math.min(1, opacity))
    this._map?.triggerRepaint()
  }

  setActive(active: boolean): void {
    if (active === this._active) return
    this._active = active
    this._debug.active = active
    if (active) {
      this._map?.triggerRepaint()
    }
  }

  // ── Private ─────────────────────────────────────────────────────────

  /** Convert latitude to web mercator Y in [0,1]. */
  private _latToMercatorY(lat: number): number {
    const sinLat = Math.sin((lat * Math.PI) / 180)
    const y = 0.5 - (Math.log((1 + sinLat) / (1 - sinLat)) / (4 * Math.PI))
    return Math.max(0, Math.min(1, y))
  }

  /** Create tile managers for wind data fetching. */
  private _initTileManagers(gl: WebGL2RenderingContext): void {
    const handleTileLoaded = (key: string) => {
      this._markAtlasTileDirty(key)
      this._map?.triggerRepaint()
    }
    const fetchClient = getTileFetchClient()
    const tmOpts = { apiBase: this._apiBase, format: this._tileFormat, fetchClient }
    this._windUManager = new TileManager(gl, tmOpts)
    this._windUManager.onTileLoaded = handleTileLoaded
    this._windVManager = new TileManager(gl, tmOpts)
    this._windVManager.onTileLoaded = handleTileLoaded
    this._windUT1Manager = new TileManager(gl, tmOpts)
    this._windUT1Manager.onTileLoaded = handleTileLoaded
    this._windVT1Manager = new TileManager(gl, tmOpts)
    this._windVT1Manager.onTileLoaded = handleTileLoaded
  }

  /** Create all GL resources: programs, textures, FBOs. */
  private _initResources(gl: WebGL2RenderingContext): void {
    // ── Programs ──
    this._updateProgram = createProgram(gl, updateVertSource, updateFragSource)
    this._drawProgram = createProgram(gl, drawVertSource, drawFragSource)
    // Trail composite reuses the update vertex shader (passthrough fullscreen quad)
    this._compositeProgram = createProgram(gl, updateVertSource, trailFragSource)

    this._quad = createFullscreenQuad(gl)

    // Empty VAO for particle draw (uses gl_VertexID, no vertex attributes)
    this._drawVao = gl.createVertexArray()
    if (!this._drawVao) throw new Error('Failed to create draw VAO')

    // ── Cache uniform locations ──
    const up = this._updateProgram.program
    this._uUpdateStateTex = gl.getUniformLocation(up, 'u_stateTex')
    this._uUpdateWindU = gl.getUniformLocation(up, 'u_windU')
    this._uUpdateWindV = gl.getUniformLocation(up, 'u_windV')
    this._uUpdateWindUT1 = gl.getUniformLocation(up, 'u_windUT1')
    this._uUpdateWindVT1 = gl.getUniformLocation(up, 'u_windVT1')
    this._uUpdateTemporalMix = gl.getUniformLocation(up, 'u_temporalMix')
    this._uUpdateHasWindData = gl.getUniformLocation(up, 'u_hasWindData')
    this._uUpdateIsFloat16 = gl.getUniformLocation(up, 'u_isFloat16')
    this._uUpdateValueMin = gl.getUniformLocation(up, 'u_valueMin')
    this._uUpdateValueMax = gl.getUniformLocation(up, 'u_valueMax')
    this._uUpdateSpeedScale = gl.getUniformLocation(up, 'u_speedScale')
    this._uUpdateDt = gl.getUniformLocation(up, 'u_dt')
    this._uUpdateSeed = gl.getUniformLocation(up, 'u_seed')
    this._uUpdateViewportBounds = gl.getUniformLocation(up, 'u_viewportBounds')
    this._uUpdateAtlasOriginX = gl.getUniformLocation(up, 'u_atlasOriginX')
    this._uUpdateAtlasOriginY = gl.getUniformLocation(up, 'u_atlasOriginY')
    this._uUpdateAtlasZoom = gl.getUniformLocation(up, 'u_atlasZoom')
    this._uUpdateAtlasCols = gl.getUniformLocation(up, 'u_atlasCols')
    this._uUpdateAtlasRows = gl.getUniformLocation(up, 'u_atlasRows')

    const dp = this._drawProgram.program
    this._uDrawStateTex = gl.getUniformLocation(dp, 'u_stateTex')
    this._uDrawMatrix = gl.getUniformLocation(dp, 'u_matrix')
    this._uDrawPointSize = gl.getUniformLocation(dp, 'u_pointSize')
    this._uDrawSpeedMax = gl.getUniformLocation(dp, 'u_speedMax')

    const cp = this._compositeProgram.program
    this._uCompositeTexture = gl.getUniformLocation(cp, 'u_texture')
    this._uCompositeOpacity = gl.getUniformLocation(cp, 'u_opacity')

    // ── State textures (RGBA32F, stateSize × stateSize) ──
    this._stateTextures = [null, null]
    this._stateFbos = [null, null]

    this._stateTextures[0] = this._createStateTexture(gl)
    this._stateTextures[1] = this._createStateTexture(gl)
    this._initializeParticles(gl, this._stateTextures[0])
    this._initializeParticles(gl, this._stateTextures[1])
    this._stateFbos[0] = this._createFBO(gl, this._stateTextures[0])
    this._stateFbos[1] = this._createFBO(gl, this._stateTextures[1])
    this._stateReadIndex = 0

    // Trail textures are created lazily on first render (need canvas size)

    // ── Scratch FBOs for atlas tile copy ──
    this._copyFbo = gl.createFramebuffer()
    if (!this._copyFbo) throw new Error('Failed to create copy FBO')
    this._atlasFbo = gl.createFramebuffer()
    if (!this._atlasFbo) throw new Error('Failed to create atlas FBO')
  }

  /** Create an RGBA32F texture at stateSize × stateSize. */
  private _createStateTexture(gl: WebGL2RenderingContext): WebGLTexture {
    const tex = gl.createTexture()
    if (!tex) throw new Error('Failed to create state texture')

    gl.bindTexture(gl.TEXTURE_2D, tex)
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA32F, this._stateSize, this._stateSize, 0, gl.RGBA, gl.FLOAT, null)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
    gl.bindTexture(gl.TEXTURE_2D, null)
    return tex
  }

  /** Fill state texture with random particle positions. */
  private _initializeParticles(gl: WebGL2RenderingContext, tex: WebGLTexture): void {
    const data = new Float32Array(this._particleCount * 4)
    for (let i = 0; i < this._particleCount; i++) {
      const offset = i * 4
      data[offset + 0] = Math.random()  // lon [0,1]
      data[offset + 1] = Math.random()  // lat [0,1]
      data[offset + 2] = Math.random()  // age [0,1] — stagger spawns
      data[offset + 3] = 1.0            // reserved
    }
    gl.bindTexture(gl.TEXTURE_2D, tex)
    gl.texSubImage2D(gl.TEXTURE_2D, 0, 0, 0, this._stateSize, this._stateSize, gl.RGBA, gl.FLOAT, data)
    gl.bindTexture(gl.TEXTURE_2D, null)
  }

  /** Create an FBO that renders to the given texture. */
  private _createFBO(gl: WebGL2RenderingContext, tex: WebGLTexture): WebGLFramebuffer {
    const fbo = gl.createFramebuffer()
    if (!fbo) throw new Error('Failed to create framebuffer')

    gl.bindFramebuffer(gl.FRAMEBUFFER, fbo)
    gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, tex, 0)

    const status = gl.checkFramebufferStatus(gl.FRAMEBUFFER)
    if (status !== gl.FRAMEBUFFER_COMPLETE) {
      gl.deleteFramebuffer(fbo)
      throw new Error(`Framebuffer incomplete: 0x${status.toString(16)}`)
    }

    gl.bindFramebuffer(gl.FRAMEBUFFER, null)
    return fbo
  }

  /** Create or recreate trail textures to match canvas dimensions. */
  private _resizeTrailTextures(gl: WebGL2RenderingContext, width: number, height: number): void {
    // Clean up old trail resources
    if (this._trailFbos) {
      for (const fbo of this._trailFbos) {
        if (fbo) gl.deleteFramebuffer(fbo)
      }
    }
    if (this._trailTextures) {
      for (const tex of this._trailTextures) {
        if (tex) gl.deleteTexture(tex)
      }
    }

    this._trailWidth = width
    this._trailHeight = height
    this._trailTextures = [null, null]
    this._trailFbos = [null, null]

    for (let i = 0; i < 2; i++) {
      const tex = gl.createTexture()
      if (!tex) throw new Error('Failed to create trail texture')

      gl.bindTexture(gl.TEXTURE_2D, tex)
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, width, height, 0, gl.RGBA, gl.UNSIGNED_BYTE, null)
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR)
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR)
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
      gl.bindTexture(gl.TEXTURE_2D, null)

      this._trailTextures[i] = tex
      this._trailFbos[i] = this._createFBO(gl, tex)
    }

    // Clear both trail textures to transparent
    for (let i = 0; i < 2; i++) {
      gl.bindFramebuffer(gl.FRAMEBUFFER, this._trailFbos[i])
      gl.viewport(0, 0, width, height)
      gl.clearColor(0, 0, 0, 0)
      gl.clear(gl.COLOR_BUFFER_BIT)
    }
    gl.bindFramebuffer(gl.FRAMEBUFFER, null)

    this._trailReadIndex = 0
  }

  // ── Tile Atlas ─────────────────────────────────────────────────────

  private _invalidateAtlas(): void {
    this._atlasLayout = null
    this._atlasLayoutKey = ''
    this._atlasSlotsByKey.clear()
    this._atlasVisibleKeys = []
    this._atlasDirtyT0.clear()
    this._atlasDirtyT1.clear()
    this._updateDebugPendingDirtyTiles()
  }

  private _markAtlasTileDirty(key: string): void {
    if (!this._atlasSlotsByKey.has(key)) return
    this._atlasDirtyT0.add(key)
    if (this._forecastHourT1 >= 0) {
      this._atlasDirtyT1.add(key)
    }
    this._updateDebugPendingDirtyTiles()
  }

  private _updateDebugPendingDirtyTiles(): void {
    this._debug.pendingDirtyTiles = this._atlasDirtyT0.size + this._atlasDirtyT1.size
  }

  private _computeAtlasLayout(visibleCoords: TileCoord[]): {
    layout: Omit<AtlasLayout, 'hasAnyTile'>
    layoutKey: string
    slotsByKey: Map<string, AtlasSlot[]>
    visibleKeys: string[]
  } | null {
    if (visibleCoords.length === 0) return null

    const zoom = visibleCoords[0].z
    const n = 2 ** zoom
    let minRX = Infinity
    let maxRX = -Infinity
    let minY = Infinity
    let maxY = -Infinity

    for (const c of visibleCoords) {
      const rx = c.x + c.wrap * n
      if (rx < minRX) minRX = rx
      if (rx > maxRX) maxRX = rx
      if (c.y < minY) minY = c.y
      if (c.y > maxY) maxY = c.y
    }

    const cols = maxRX - minRX + 1
    const rows = maxY - minY + 1

    if (cols * TILE_SIZE > this._maxTextureSize || rows * TILE_SIZE > this._maxTextureSize) {
      return null
    }

    const slotsByKey = new Map<string, AtlasSlot[]>()
    const visibleKeys: string[] = []
    for (const c of visibleCoords) {
      const rx = c.x + c.wrap * n
      const key = tileKey(c.z, c.x, c.y)
      const slots = slotsByKey.get(key)
      const slot = { col: rx - minRX, row: c.y - minY }
      if (slots) {
        slots.push(slot)
      } else {
        slotsByKey.set(key, [slot])
        visibleKeys.push(key)
      }
    }

    return {
      layout: { cols, rows, originX: minRX, originY: minY, zoom },
      layoutKey: `${zoom}:${minRX}:${minY}:${cols}:${rows}`,
      slotsByKey,
      visibleKeys,
    }
  }

  /**
   * Pack all visible tile textures into atlas textures for GPU sampling.
   * Returns the atlas layout for setting shader uniforms.
   */
  private _packAtlas(
    gl: WebGL2RenderingContext,
    visibleCoords: TileCoord[],
  ): AtlasLayout | null {
    const computed = this._computeAtlasLayout(visibleCoords)
    if (!computed) {
      this._invalidateAtlas()
      return null
    }

    const layoutChanged = computed.layoutKey !== this._atlasLayoutKey || !this._atlasLayout
    this._atlasSlotsByKey = computed.slotsByKey
    this._atlasVisibleKeys = computed.visibleKeys

    if (layoutChanged) {
      this._ensureAtlas(gl, computed.layout.cols, computed.layout.rows)
      this._clearAtlas(gl)
      this._debug.atlasClears += 1
      this._debug.atlasFlushes += 1
      this._atlasLayout = {
        ...computed.layout,
        hasAnyTile: false,
      }
      this._atlasLayoutKey = computed.layoutKey
      this._atlasDirtyT0 = new Set(computed.visibleKeys)
      this._atlasDirtyT1 = this._forecastHourT1 >= 0
        ? new Set(computed.visibleKeys)
        : new Set()
    } else if (this._forecastHourT1 < 0) {
      this._atlasDirtyT1.clear()
    }

    let blits = 0
    blits += this._flushDirtyWindAtlas(
      gl,
      this._atlasDirtyT0,
      this._windUManager,
      this._windVManager,
      this._atlasU,
      this._atlasV,
    )
    if (this._forecastHourT1 >= 0) {
      blits += this._flushDirtyWindAtlas(
        gl,
        this._atlasDirtyT1,
        this._windUT1Manager,
        this._windVT1Manager,
        this._atlasUT1,
        this._atlasVT1,
      )
    } else {
      this._atlasDirtyT1.clear()
    }
    if (!layoutChanged && blits > 0) {
      this._debug.atlasFlushes += 1
    }
    if (blits > 0) {
      this._debug.atlasBlits += blits
    }
    this._updateDebugPendingDirtyTiles()

    if (!this._atlasLayout) return null
    this._atlasLayout.hasAnyTile = this._hasVisibleWindTile()
    return this._atlasLayout
  }

  private _flushDirtyWindAtlas(
    gl: WebGL2RenderingContext,
    dirtyKeys: Set<string>,
    uManager: TileManager | null,
    vManager: TileManager | null,
    atlasU: WebGLTexture | null,
    atlasV: WebGLTexture | null,
  ): number {
    if (dirtyKeys.size === 0) return 0

    let blits = 0
    for (const key of dirtyKeys) {
      const slots = this._atlasSlotsByKey.get(key)
      if (!slots || slots.length === 0) continue

      const { z, x, y } = parseTileKey(key)
      const uTex = uManager?.getTexture(z, x, y) ?? null
      const vTex = vManager?.getTexture(z, x, y) ?? null

      for (const slot of slots) {
        if (uTex && atlasU) {
          this._copyTileToAtlas(gl, uTex, atlasU, slot.col, slot.row)
          blits += 1
        }
        if (vTex && atlasV) {
          this._copyTileToAtlas(gl, vTex, atlasV, slot.col, slot.row)
          blits += 1
        }
      }
    }

    dirtyKeys.clear()
    gl.bindFramebuffer(gl.READ_FRAMEBUFFER, null)
    gl.bindFramebuffer(gl.DRAW_FRAMEBUFFER, null)
    return blits
  }

  private _hasVisibleWindTile(): boolean {
    for (const key of this._atlasVisibleKeys) {
      const { z, x, y } = parseTileKey(key)
      if (this._windUManager?.getTexture(z, x, y)) {
        return true
      }
    }
    return false
  }

  /** Ensure atlas textures are allocated at the required dimensions. */
  private _ensureAtlas(gl: WebGL2RenderingContext, cols: number, rows: number): void {
    const w = cols * TILE_SIZE
    const h = rows * TILE_SIZE
    if (w === this._atlasWidth && h === this._atlasHeight) return

    this._deleteAtlasTextures(gl)

    this._atlasWidth = w
    this._atlasHeight = h

    this._atlasU = this._createAtlasTexture(gl, w, h)
    this._atlasV = this._createAtlasTexture(gl, w, h)
    this._atlasUT1 = this._createAtlasTexture(gl, w, h)
    this._atlasVT1 = this._createAtlasTexture(gl, w, h)
  }

  /** Create a single atlas texture with the appropriate format. */
  private _createAtlasTexture(gl: WebGL2RenderingContext, w: number, h: number): WebGLTexture {
    const tex = gl.createTexture()
    if (!tex) throw new Error('Failed to create atlas texture')

    gl.bindTexture(gl.TEXTURE_2D, tex)
    if (this._tileFormat === 'f16') {
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.R16F, w, h, 0, gl.RED, gl.HALF_FLOAT, null)
    } else {
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, w, h, 0, gl.RGBA, gl.UNSIGNED_BYTE, null)
    }
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
    gl.bindTexture(gl.TEXTURE_2D, null)
    return tex
  }

  /** Clear all atlas textures to nodata values. */
  private _clearAtlas(gl: WebGL2RenderingContext): void {
    const atlases = [this._atlasU, this._atlasV, this._atlasUT1, this._atlasVT1]
    // For PNG: B > 0.5 signals nodata. For F16: R < -9000 signals nodata.
    // Use clearBufferfv for portability — clearColor may clamp to [0,1] on
    // some implementations (Safari/mobile), which would write 0 instead of
    // -9999 for F16, causing unloaded atlas cells to be treated as 0 m/s wind.
    const clearValue = this._tileFormat === 'f16'
      ? new Float32Array([-9999, 0, 0, 0])
      : new Float32Array([0, 0, 1, 0])
    for (const atlas of atlases) {
      if (!atlas) continue
      gl.bindFramebuffer(gl.FRAMEBUFFER, this._atlasFbo)
      gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, atlas, 0)
      gl.viewport(0, 0, this._atlasWidth, this._atlasHeight)
      gl.clearBufferfv(gl.COLOR, 0, clearValue)
    }
    gl.bindFramebuffer(gl.FRAMEBUFFER, null)
  }

  /** Copy a single tile texture into the atlas at the given grid position. */
  private _copyTileToAtlas(
    gl: WebGL2RenderingContext,
    src: WebGLTexture,
    dst: WebGLTexture,
    col: number,
    row: number,
  ): void {
    const dx = col * TILE_SIZE
    const dy = row * TILE_SIZE

    // Source: bind tile texture to scratch READ FBO
    gl.bindFramebuffer(gl.READ_FRAMEBUFFER, this._copyFbo)
    gl.framebufferTexture2D(gl.READ_FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, src, 0)

    // Dest: bind atlas texture to scratch DRAW FBO
    gl.bindFramebuffer(gl.DRAW_FRAMEBUFFER, this._atlasFbo)
    gl.framebufferTexture2D(gl.DRAW_FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, dst, 0)

    gl.blitFramebuffer(
      0, 0, TILE_SIZE, TILE_SIZE,
      dx, dy, dx + TILE_SIZE, dy + TILE_SIZE,
      gl.COLOR_BUFFER_BIT, gl.NEAREST,
    )
  }

  /** Delete atlas textures (called on resize or cleanup). */
  private _deleteAtlasTextures(gl: WebGL2RenderingContext): void {
    if (this._atlasU) { gl.deleteTexture(this._atlasU); this._atlasU = null }
    if (this._atlasV) { gl.deleteTexture(this._atlasV); this._atlasV = null }
    if (this._atlasUT1) { gl.deleteTexture(this._atlasUT1); this._atlasUT1 = null }
    if (this._atlasVT1) { gl.deleteTexture(this._atlasVT1); this._atlasVT1 = null }
    this._atlasWidth = 0
    this._atlasHeight = 0
  }

  /** Free all GL resources. */
  private _cleanup(): void {
    this._active = false
    this._debug.active = false
    this._invalidateAtlas()

    // Destroy tile managers
    this._windUManager?.destroy()
    this._windUManager = null
    this._windVManager?.destroy()
    this._windVManager = null
    this._windUT1Manager?.destroy()
    this._windUT1Manager = null
    this._windVT1Manager?.destroy()
    this._windVT1Manager = null

    const gl = this._gl
    if (gl) {
      // Trail resources
      if (this._trailFbos) {
        for (const fbo of this._trailFbos) {
          if (fbo) gl.deleteFramebuffer(fbo)
        }
        this._trailFbos = null
      }
      if (this._trailTextures) {
        for (const tex of this._trailTextures) {
          if (tex) gl.deleteTexture(tex)
        }
        this._trailTextures = null
      }

      // State resources
      if (this._stateFbos) {
        for (const fbo of this._stateFbos) {
          if (fbo) gl.deleteFramebuffer(fbo)
        }
        this._stateFbos = null
      }
      if (this._stateTextures) {
        for (const tex of this._stateTextures) {
          if (tex) gl.deleteTexture(tex)
        }
        this._stateTextures = null
      }

      // Atlas resources
      this._deleteAtlasTextures(gl)
      if (this._copyFbo) { gl.deleteFramebuffer(this._copyFbo); this._copyFbo = null }
      if (this._atlasFbo) { gl.deleteFramebuffer(this._atlasFbo); this._atlasFbo = null }

      // Draw VAO
      if (this._drawVao) {
        gl.deleteVertexArray(this._drawVao)
        this._drawVao = null
      }

      // Geometry
      if (this._quad) {
        deleteQuadGeometry(gl, this._quad)
        this._quad = null
      }

      // Programs
      if (this._updateProgram) {
        deleteProgram(gl, this._updateProgram)
        this._updateProgram = null
      }
      if (this._drawProgram) {
        deleteProgram(gl, this._drawProgram)
        this._drawProgram = null
      }
      if (this._compositeProgram) {
        deleteProgram(gl, this._compositeProgram)
        this._compositeProgram = null
      }
    }

    this._uUpdateStateTex = null
    this._uUpdateWindU = null
    this._uUpdateWindV = null
    this._uUpdateWindUT1 = null
    this._uUpdateWindVT1 = null
    this._uUpdateTemporalMix = null
    this._uUpdateHasWindData = null
    this._uUpdateIsFloat16 = null
    this._uUpdateValueMin = null
    this._uUpdateValueMax = null
    this._uUpdateSpeedScale = null
    this._uUpdateDt = null
    this._uUpdateSeed = null
    this._uUpdateViewportBounds = null
    this._uUpdateAtlasOriginX = null
    this._uUpdateAtlasOriginY = null
    this._uUpdateAtlasZoom = null
    this._uUpdateAtlasCols = null
    this._uUpdateAtlasRows = null
    this._uDrawStateTex = null
    this._uDrawMatrix = null
    this._uDrawPointSize = null
    this._uDrawSpeedMax = null
    this._uCompositeTexture = null
    this._uCompositeOpacity = null
    this._gl = null
    this._map = null
  }
}

function tileKey(z: number, x: number, y: number): string {
  return `${z}/${x}/${y}`
}

function parseTileKey(key: string): { z: number; x: number; y: number } {
  const [z, x, y] = key.split('/').map(Number)
  return { z, x, y }
}

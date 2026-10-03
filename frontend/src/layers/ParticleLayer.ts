/**
 * Shared machinery of the GPU particle layers (wind, wave).
 *
 * Each frame runs three passes inside MapLibre's render loop:
 *
 *   Pass 1 — State Update (ping-pong)
 *     Read particle state from texture A, write the next state to texture B
 *     with a fullscreen-quad fragment shader, then swap. The shader samples
 *     the data layers from tile atlases: all visible tiles of one layer and
 *     forecast hour packed into a single texture.
 *
 *   Pass 2 — Trail Composite (ping-pong)
 *     Draw the previous trail texture, faded, into the other trail texture,
 *     then draw the particles on top.
 *
 *   Pass 3 — Map Composite
 *     Draw the trail texture onto MapLibre's framebuffer.
 *
 * A subclass names its data layers and shaders, gives the particles' first
 * state, sets its own update uniforms, and draws the particles. All state
 * stays on the GPU — no CPU readback in the hot path.
 */

import type {
  CustomLayerInterface,
  CustomRenderMethodInput,
  Map as MaplibreMap,
} from 'maplibre-gl'

import updateVertSource from './shaders/particle-update.vert.glsl?raw'
import trailFragSource from './shaders/trail-composite.frag.glsl?raw'
import {
  createFullscreenQuad,
  createProgram,
  deleteProgram,
  deleteQuadGeometry,
  matrixChanged,
  type GLProgram,
  type QuadGeometry,
} from './gl-utils'
import {
  TileManager,
  PanVelocityTracker,
  computeVisibleTiles,
  computePanPrefetchTiles,
  dataTileZoom,
  type TileCoord,
  type TileFormat,
} from './TileManager'
import { getTileFetchClient } from '@/workers/TileFetchClient'
import { createTrailDecay } from './particle-motion'
import { TILE_SIZE, tileGutters } from './shared-tiles'
import { detectGpuTier, clampStateSize, type GpuTier } from './gpu-tier'
import { ensureParticleDebugState, type ParticleDebugLayer, type ParticleDebugState } from './particleDebug'

/** Particles per axis until the GPU tier is known. */
const DEFAULT_STATE_SIZE = 50
/** Trail buffer pixels per CSS pixel (#93). Particles are a few CSS pixels wide, and the upscale onto the map keeps them so. */
const TRAIL_PIXEL_RATIO = 1
/** Frames of frame-time history for the performance watchdog. */
const PERF_WINDOW = 60
/** Frame time in ms above which the watchdog warns. */
const PERF_WARN_THRESHOLD_MS = 20

export interface ParticleLayerOptions {
  /** Unique layer ID for MapLibre. */
  id?: string
  /** Overlay opacity 0-1. */
  opacity?: number
  /** Base URL for the data tile API. Default: '' (same-origin). */
  apiBase?: string
  /** Tile format: 'png' (default) or 'f16' (Float16 binary). */
  tileFormat?: TileFormat
  /**
   * Override the particle state texture size (particles = stateSize²).
   * If omitted, chosen from the GPU tier. Valid range: 16-512.
   */
  stateSize?: number
}

/** What a subclass is made of. */
export interface ParticleLayerSpec {
  /** Debug-state key; also names the layer in logs. */
  kind: ParticleDebugLayer
  /** Default MapLibre layer ID. */
  id: string
  /** Default opacity. */
  opacity: number
  /** Data layers sampled by the update shader, e.g. ['wind_u', 'wind_v']. */
  layers: string[]
  /** For each data layer, its current-hour and next-hour sampler uniforms. */
  samplers: [string, string][]
  updateFragSource: string
  drawVertSource: string
  drawFragSource: string
  /** Trail fade factor per 1/60 s. */
  trailFade: number
  /** Share of the GPU tier's state size to use. */
  stateSizeScale: number
}

/** The viewport in mercator [0,1], east of west even across the antimeridian. */
export interface MercatorBounds {
  minLon: number
  minLat: number
  maxLon: number
  maxLat: number
}

/** What the subclass hooks get each frame. */
export interface ParticleFrame {
  /** Seconds, from performance.now(). */
  now: number
  /** Seconds since the previous frame, at most 0.1. */
  dt: number
  /** Pixels per mercator unit at the current zoom. */
  worldSize: number
  /** Device pixels per CSS pixel. */
  pixelRatio: number
  viewport: MercatorBounds
  /** The map's drawing buffer, in device pixels. */
  canvasWidth: number
  canvasHeight: number
  /** The trail buffer the particles are drawn into, upscaled onto the map. */
  trailWidth: number
  trailHeight: number
  /** Trail pixels per CSS pixel: sizes given in CSS pixels times this. */
  trailPixelRatio: number
  /** MapLibre's matrix, rescaled for positions in mercator [0,1]. */
  mercatorMatrix: Float32Array
  /** State after this frame's update. */
  state: WebGLTexture
  /** State before it. */
  prevState: WebGLTexture
}

/** Uniform location by name, looked up once per program. */
export type UniformLookup = (name: string) => WebGLUniformLocation | null

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

type Slots = [WebGLTexture | null, WebGLTexture | null]

export abstract class ParticleLayer implements CustomLayerInterface {
  readonly id: string
  readonly type = 'custom' as const
  readonly renderingMode = '2d' as const

  protected _debug: ParticleDebugState
  /** Particles there are slots for (stateSize²). */
  protected _particleCount: number

  private _spec: ParticleLayerSpec
  private _map: MaplibreMap | null = null
  private _gl: WebGL2RenderingContext | null = null
  private _opacity: number
  private _active = false
  private _tileFormat: TileFormat
  private _apiBase: string
  private _maxTextureSize = 4096
  private _stateSize: number
  private _gpuTier: GpuTier = 'medium'
  private _stateSizeOverride: number | undefined

  // ── Data: per data layer, the current hour (T0) and the next (T1) ──
  private _t0: (TileManager | null)[] = []
  private _t1: (TileManager | null)[] = []
  private _configured = false
  /** Data tiles of the last drawn frame. */
  private _lastVisible: TileCoord[] = []
  private _model = ''
  private _runId = ''
  private _forecastHourT1 = -1
  private _temporalMix = 0

  // ── Programs ──
  private _updateProgram: GLProgram | null = null
  private _drawProgram: GLProgram | null = null
  private _compositeProgram: GLProgram | null = null
  private _uUpdate: UniformLookup = () => null
  private _uDraw: UniformLookup = () => null
  private _uComposite: UniformLookup = () => null
  private _quad: QuadGeometry | null = null
  /** Empty VAO: the draw shaders work from gl_VertexID. */
  protected _drawVao: WebGLVertexArrayObject | null = null

  // ── State ping-pong (RGBA32F, stateSize²) ──
  private _stateTextures: Slots | null = null
  private _stateFbos: [WebGLFramebuffer | null, WebGLFramebuffer | null] | null = null
  private _stateReadIndex = 0

  // ── Trail ping-pong (canvas-sized RGBA8) ──
  private _trailTextures: Slots | null = null
  private _trailFbos: [WebGLFramebuffer | null, WebGLFramebuffer | null] | null = null
  private _trailReadIndex = 0
  private _trailWidth = 0
  private _trailHeight = 0
  // View matrix of the previous frame — trails are screen-space, so they are
  // dropped whenever the view moves
  private _lastMvp = new Float64Array(16)
  private _trailDecay: ReturnType<typeof createTrailDecay>

  // ── Tile atlases, one per data layer and hour ──
  private _atlasT0: (WebGLTexture | null)[] = []
  private _atlasT1: (WebGLTexture | null)[] = []
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

  private _panTracker = new PanVelocityTracker()

  // ── Timing & performance watchdog ──
  private _lastFrameTime = 0
  private _frameTimes: number[] = []
  private _perfWarned = false

  protected constructor(spec: ParticleLayerSpec, options: ParticleLayerOptions) {
    this._spec = spec
    this.id = options.id ?? spec.id
    this._opacity = options.opacity ?? spec.opacity
    this._apiBase = options.apiBase ?? ''
    this._tileFormat = options.tileFormat ?? 'png'
    this._debug = ensureParticleDebugState(spec.kind)
    this._debug.active = this._active
    this._stateSizeOverride = options.stateSize
    // Temporary defaults — overwritten by GPU detection in onAdd()
    this._stateSize = DEFAULT_STATE_SIZE
    this._particleCount = DEFAULT_STATE_SIZE * DEFAULT_STATE_SIZE
    this._trailDecay = createTrailDecay(spec.trailFade)
  }

  // ── Subclass hooks ──────────────────────────────────────────────────

  /** A particle's first state, written to data[offset..offset+3]. */
  protected abstract _initialState(data: Float32Array, offset: number): void

  /** Set the update uniforms beyond the shared ones (atlas, viewport, samplers…). */
  protected abstract _setUpdateUniforms(gl: WebGL2RenderingContext, u: UniformLookup, frame: ParticleFrame): void

  /** Draw the particles into the trail; the draw program is in use. */
  protected abstract _drawParticles(gl: WebGL2RenderingContext, u: UniformLookup, frame: ParticleFrame): void

  // ── CustomLayerInterface ────────────────────────────────────────────

  private get _logPrefix(): string {
    return `[${this._spec.kind} particles]`
  }

  onAdd(map: MaplibreMap, gl: WebGLRenderingContext | WebGL2RenderingContext): void {
    if (!(gl instanceof WebGL2RenderingContext)) {
      console.error(`${this._logPrefix} WebGL2 is required`)
      return
    }

    this._map = map
    this._gl = gl
    this._debug.mounts += 1
    this._debug.active = this._active

    if (!gl.getExtension('EXT_color_buffer_float')) {
      console.error(`${this._logPrefix} EXT_color_buffer_float not supported`)
      return
    }

    this._maxTextureSize = gl.getParameter(gl.MAX_TEXTURE_SIZE) as number

    if (this._stateSizeOverride != null) {
      this._stateSize = clampStateSize(this._stateSizeOverride)
    } else {
      const tier = detectGpuTier(gl)
      this._gpuTier = tier.tier
      this._stateSize = clampStateSize(Math.round(tier.stateSize * this._spec.stateSizeScale))
      console.info(
        `${this._logPrefix} GPU: "${tier.renderer}" → tier=${tier.tier}, ` +
        `stateSize=${this._stateSize} (${this._stateSize ** 2} particles)`
      )
    }
    this._particleCount = this._stateSize * this._stateSize

    try {
      this._initResources(gl)
      this._initTileManagers(gl)
    } catch (e) {
      console.error(`${this._logPrefix} Initialization failed:`, e)
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

    if (!this._active || this._opacity <= 0 || !this._configured) {
      return
    }

    // ── Timing & performance watchdog ──
    const now = performance.now() / 1000
    const dt = this._lastFrameTime > 0 ? Math.min(now - this._lastFrameTime, 0.1) : 0.016
    this._lastFrameTime = now
    this._watchFrameTime(dt * 1000)

    // ── Update tile managers with the current viewport ──
    const zoom = dataTileZoom(this._map.getZoom())
    const bounds = this._map.getBounds()
    const visibleCoords = computeVisibleTiles({
      west: bounds.getWest(),
      north: bounds.getNorth(),
      east: bounds.getEast(),
      south: bounds.getSouth(),
    }, zoom)

    // When crossing the antimeridian, east < west (e.g. west=170°, east=-170°).
    // Adding 360° to east gives a contiguous range, so the shaders'
    // out-of-bounds checks and the pan tracker work across it.
    let east = bounds.getEast()
    const west = bounds.getWest()
    if (east < west) east += 360

    // Pan prefetch: detect movement and prefetch one tile ring ahead
    const panDir = this._panTracker.update((west + east) / 2, (bounds.getNorth() + bounds.getSouth()) / 2)
    const prefetchCoords = panDir ? computePanPrefetchTiles(visibleCoords, panDir, zoom) : []
    this._updateManagers(visibleCoords, prefetchCoords)

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
    const prevBlendEqRgb = gl.getParameter(gl.BLEND_EQUATION_RGB) as number
    const prevBlendEqAlpha = gl.getParameter(gl.BLEND_EQUATION_ALPHA) as number

    // ── Pack visible tiles into atlas textures ──
    const atlas = this._packAtlas(gl, visibleCoords)
    const hasData = atlas?.hasAnyTile ?? false

    // ── Resize trail textures if canvas size changed ──
    // The trails are drawn at CSS resolution and upscaled onto the map
    // (#93): at DPR 2 that is a quarter of the pixels to fade and draw each
    // frame. Never more than the drawing buffer.
    const canvasWidth = gl.drawingBufferWidth
    const canvasHeight = gl.drawingBufferHeight
    const pixelRatio = this._map.getPixelRatio()
    const trailScale = Math.min(1, TRAIL_PIXEL_RATIO / pixelRatio)
    const trailWidth = Math.max(1, Math.round(canvasWidth * trailScale))
    const trailHeight = Math.max(1, Math.round(canvasHeight * trailScale))
    if (!this._trailTextures || trailWidth !== this._trailWidth || trailHeight !== this._trailHeight) {
      this._resizeTrailTextures(gl, trailWidth, trailHeight)
    }
    if (!this._trailTextures || !this._trailFbos) return
    const trailPixelRatio = trailWidth / (canvasWidth / pixelRatio)
    this._debug.trailWidth = trailWidth
    this._debug.trailHeight = trailHeight
    this._debug.trailPixelRatio = trailPixelRatio

    this._debug.hour = this._t0[0]?.currentForecastHour
    const worldSize = 512 * Math.pow(2, this._map.getZoom())

    // MapLibre's modelViewProjectionMatrix transforms from world coordinates
    // [0, worldSize] to clip space. Particles use mercator [0, 1], so
    // columns 0 and 1 are scaled by worldSize.
    const mvp = options.modelViewProjectionMatrix
    const mercatorMatrix = new Float32Array(16)
    for (let i = 0; i < 4; i++) {
      mercatorMatrix[i]      = mvp[i]      * worldSize  // column 0 (x)
      mercatorMatrix[4 + i]  = mvp[4 + i]  * worldSize  // column 1 (y)
      mercatorMatrix[8 + i]  = mvp[8 + i]               // column 2 (z)
      mercatorMatrix[12 + i] = mvp[12 + i]              // column 3 (translation)
    }

    const stateRead = this._stateReadIndex
    const stateWrite = 1 - stateRead
    const frame: ParticleFrame = {
      now,
      dt,
      worldSize,
      pixelRatio,
      viewport: {
        minLon: (west + 180) / 360,
        maxLon: (east + 180) / 360,
        minLat: latToMercatorY(bounds.getNorth()),
        maxLat: latToMercatorY(bounds.getSouth()),
      },
      canvasWidth,
      canvasHeight,
      trailWidth,
      trailHeight,
      trailPixelRatio,
      mercatorMatrix,
      state: this._stateTextures[stateWrite]!,
      prevState: this._stateTextures[stateRead]!,
    }

    // ────────────────────────────────────────────────────────────────
    // Pass 1: State Update (ping-pong)
    // ────────────────────────────────────────────────────────────────
    gl.bindFramebuffer(gl.FRAMEBUFFER, this._stateFbos[stateWrite])
    gl.viewport(0, 0, this._stateSize, this._stateSize)
    gl.disable(gl.BLEND)
    gl.useProgram(this._updateProgram.program)
    const u = this._uUpdate

    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, frame.prevState)
    gl.uniform1i(u('u_stateTex'), 0)

    // Blend only once all of T1 is in: a partly filled T1 atlas would blend
    // some tiles towards the next hour and others towards nothing (#36).
    const hasT1 = this._forecastHourT1 >= 0 && this._temporalMix > 0 && this._t1Covers(false)

    // Atlases on units 1.., current then next hour for each data layer.
    this._spec.samplers.forEach(([t0Name, t1Name], i) => {
      gl.activeTexture(gl.TEXTURE1 + 2 * i)
      gl.bindTexture(gl.TEXTURE_2D, atlas ? this._atlasT0[i] : null)
      gl.uniform1i(u(t0Name), 1 + 2 * i)
      gl.activeTexture(gl.TEXTURE2 + 2 * i)
      gl.bindTexture(gl.TEXTURE_2D, atlas && hasT1 ? this._atlasT1[i] : null)
      gl.uniform1i(u(t1Name), 2 + 2 * i)
    })

    gl.uniform1f(u('u_temporalMix'), hasData && hasT1 ? this._temporalMix : 0)
    gl.uniform1i(u('u_hasData'), hasData ? 1 : 0)
    gl.uniform1i(u('u_isFloat16'), this._tileFormat === 'f16' ? 1 : 0)
    const vp = frame.viewport
    gl.uniform4f(u('u_viewportBounds'), vp.minLon, vp.minLat, vp.maxLon, vp.maxLat)
    gl.uniform1f(u('u_atlasOriginX'), atlas?.originX ?? 0)
    gl.uniform1f(u('u_atlasOriginY'), atlas?.originY ?? 0)
    gl.uniform1f(u('u_atlasZoom'), atlas ? Math.pow(2, atlas.zoom) : 1)
    gl.uniform1f(u('u_atlasCols'), atlas?.cols ?? 1)
    gl.uniform1f(u('u_atlasRows'), atlas?.rows ?? 1)
    this._setUpdateUniforms(gl, u, frame)

    gl.bindVertexArray(this._quad.vao)
    gl.drawArrays(gl.TRIANGLES, 0, this._quad.vertexCount)
    gl.bindVertexArray(null)
    this._stateReadIndex = stateWrite

    // ────────────────────────────────────────────────────────────────
    // Pass 2: Trail Composite (ping-pong)
    // ────────────────────────────────────────────────────────────────
    const trailRead = this._trailReadIndex
    const trailWrite = 1 - trailRead

    gl.bindFramebuffer(gl.FRAMEBUFFER, this._trailFbos[trailWrite])
    gl.viewport(0, 0, this._trailWidth, this._trailHeight)
    gl.clearColor(0, 0, 0, 0)
    gl.clear(gl.COLOR_BUFFER_BIT)

    gl.enable(gl.BLEND)
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA) // premultiplied alpha

    // 2a: The faded previous trail — unless the view moved since the last
    // frame. The trail is screen-space, so after a pan or zoom it would sit
    // at the old positions and smear; start it afresh instead.
    const decay = this._trailDecay(dt)
    if (!matrixChanged(this._lastMvp, options.modelViewProjectionMatrix)) {
      // defeats the RGBA8 decay stall
      this._drawTexture(gl, this._trailTextures[trailRead], decay.fade, decay.epsilon)
    }

    // 2b: The particles on top.
    gl.useProgram(this._drawProgram.program)
    this._drawParticles(gl, this._uDraw, frame)
    this._trailReadIndex = trailWrite

    // ────────────────────────────────────────────────────────────────
    // Pass 3: Map Composite
    // ────────────────────────────────────────────────────────────────
    gl.bindFramebuffer(gl.FRAMEBUFFER, prevFbo)
    gl.viewport(prevViewport[0], prevViewport[1], prevViewport[2], prevViewport[3])
    gl.enable(gl.BLEND)
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA) // premultiplied alpha
    this._drawTexture(gl, this._trailTextures[this._trailReadIndex], this._opacity, 0)

    // ── Restore MapLibre GL state ──
    gl.activeTexture(prevActiveTexture)
    if (prevBlend) {
      gl.enable(gl.BLEND)
      gl.blendFuncSeparate(prevBlendSrc, prevBlendDst, prevBlendSrcA, prevBlendDstA)
    } else {
      gl.disable(gl.BLEND)
    }
    gl.blendEquationSeparate(prevBlendEqRgb, prevBlendEqAlpha)
    gl.useProgram(prevProgram)

    // Request the next frame only while animating
    if (hasData || this._temporalMix > 0) {
      this._map.triggerRepaint()
    }
  }

  onRemove(): void {
    this._cleanup()
  }

  // ── Public API ──────────────────────────────────────────────────────

  /** Point the data layers at a run and forecast hour. */
  protected _configure(model: string, runId: string, forecastHour: number): void {
    this._model = model
    this._runId = runId
    this._spec.layers.forEach((layer, i) => this._t0[i]?.setLayer(model, runId, layer, forecastHour))
    this._configured = true
    this._invalidateAtlas()
    this._map?.triggerRepaint()
  }

  /** Blend towards forecastHourT1 by `mix` (0-1); a negative hour turns blending off. */
  setTemporalBlend(forecastHourT1: number, mix: number): void {
    const shouldInvalidateAtlas =
      forecastHourT1 !== this._forecastHourT1 ||
      (forecastHourT1 >= 0) !== (this._forecastHourT1 >= 0)
    this._forecastHourT1 = forecastHourT1
    this._temporalMix = Math.max(0, Math.min(1, mix))

    if (forecastHourT1 >= 0 && this._model && this._runId) {
      this._spec.layers.forEach((layer, i) => this._t1[i]?.setLayer(this._model, this._runId, layer, forecastHourT1))
    }
    if (shouldInvalidateAtlas) {
      this._invalidateAtlas()
    }
    this._map?.triggerRepaint()
  }

  /** Synchronously advance the forecast hour, swapping T0↔T1 when T1 is ready. */
  advanceForecastHour(newHour: number): void {
    if (this._t1.every((m) => m?.currentForecastHour === newHour) && this._t1Covers(true)) {
      ;[this._t0, this._t1] = [this._t1, this._t0]
    } else {
      // T1 not ready — reconfigure T0 for the new hour (starts a fresh fetch)
      console.debug(`${this._logPrefix} advanceForecastHour: T1 tiles not ready for hour ${newHour}, reconfiguring T0`)
      if (this._model && this._runId) {
        this._spec.layers.forEach((layer, i) => this._t0[i]?.setLayer(this._model, this._runId, layer, newHour))
      }
    }
    this._temporalMix = 0
    this._invalidateAtlas()
    this._map?.triggerRepaint()
  }

  /**
   * Whether the next hour has finished loading for the viewport (failed
   * tiles count as finished). Always true while the layer is not drawn.
   */
  isT1Ready(): boolean {
    return !this._active || this._forecastHourT1 < 0 || this._t1Covers(true)
  }

  /** Update overlay opacity at runtime. */
  setOpacity(opacity: number): void {
    this._opacity = Math.max(0, Math.min(1, opacity))
    this._debug.opacity = this._opacity
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

  /** Whether T1 has every tile of the last drawn viewport loaded (or failed, with `orFailed`). */
  private _t1Covers(orFailed: boolean): boolean {
    return this._t1.every((m) => m != null && m.allLoaded(this._lastVisible, orFailed))
  }

  private _watchFrameTime(frameTimeMs: number): void {
    this._frameTimes.push(frameTimeMs)
    if (this._frameTimes.length > PERF_WINDOW) this._frameTimes.shift()
    if (this._perfWarned || this._frameTimes.length < PERF_WINDOW) return
    const avg = this._frameTimes.reduce((a, b) => a + b, 0) / PERF_WINDOW
    if (avg > PERF_WARN_THRESHOLD_MS) {
      console.warn(
        `${this._logPrefix} Low FPS detected: avg frame time ${avg.toFixed(1)}ms ` +
        `(${(1000 / avg).toFixed(0)} fps) with ${this._particleCount} particles ` +
        `(tier=${this._gpuTier}).`
      )
      this._perfWarned = true
    }
  }

  private _updateManagers(visibleCoords: TileCoord[], prefetchCoords: TileCoord[]): void {
    this._lastVisible = visibleCoords
    // Priority: P0 = visible current time, P1 = visible next time, P2 = prefetch.
    // T1 is fetched as soon as it is configured, not only once blending
    // starts, so the playback gate (isT1Ready) has something to wait for.
    for (const m of this._t0) {
      m?.updateVisibleTiles(visibleCoords, 0)
      if (prefetchCoords.length > 0) m?.updateVisibleTiles(prefetchCoords, 2)
    }
    if (this._forecastHourT1 < 0) return
    for (const m of this._t1) {
      m?.updateVisibleTiles(visibleCoords, 1)
      if (prefetchCoords.length > 0) m?.updateVisibleTiles(prefetchCoords, 2)
    }
  }

  /** Draw a texture over the bound framebuffer, scaled by `opacity` less `epsilon`. */
  private _drawTexture(gl: WebGL2RenderingContext, texture: WebGLTexture | null, opacity: number, epsilon: number): void {
    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, texture)
    gl.useProgram(this._compositeProgram!.program)
    gl.uniform1i(this._uComposite('u_texture'), 0)
    gl.uniform1f(this._uComposite('u_opacity'), opacity)
    gl.uniform1f(this._uComposite('u_fadeEpsilon'), epsilon)
    gl.bindVertexArray(this._quad!.vao)
    gl.drawArrays(gl.TRIANGLES, 0, this._quad!.vertexCount)
    gl.bindVertexArray(null)
  }

  private _initTileManagers(gl: WebGL2RenderingContext): void {
    const handleTileLoaded = (key: string) => {
      this._markAtlasTileDirty(key)
      this._map?.triggerRepaint()
    }
    const fetchClient = getTileFetchClient()
    const requestRender = () => this._map?.triggerRepaint()
    const tmOpts = { apiBase: this._apiBase, format: this._tileFormat, fetchClient, requestRender }
    const create = () => {
      const manager = new TileManager(gl, tmOpts)
      manager.onTileLoaded = handleTileLoaded
      return manager
    }
    this._t0 = this._spec.layers.map(create)
    this._t1 = this._spec.layers.map(create)
  }

  /** Create all GL resources: programs, textures, FBOs. */
  private _initResources(gl: WebGL2RenderingContext): void {
    // The update and composite passes share the passthrough fullscreen-quad vertex shader.
    this._updateProgram = createProgram(gl, updateVertSource, this._spec.updateFragSource)
    this._drawProgram = createProgram(gl, this._spec.drawVertSource, this._spec.drawFragSource)
    this._compositeProgram = createProgram(gl, updateVertSource, trailFragSource)
    this._uUpdate = uniformLookup(gl, this._updateProgram.program)
    this._uDraw = uniformLookup(gl, this._drawProgram.program)
    this._uComposite = uniformLookup(gl, this._compositeProgram.program)

    this._quad = createFullscreenQuad(gl)
    this._drawVao = gl.createVertexArray()
    if (!this._drawVao) throw new Error('Failed to create draw VAO')

    // Trail textures are created on the first render (they need the canvas size).
    const state0 = this._createStateTexture(gl)
    const state1 = this._createStateTexture(gl)
    this._stateTextures = [state0, state1]
    this._stateFbos = [createFBO(gl, state0), createFBO(gl, state1)]
    this._stateReadIndex = 0

    // Scratch FBOs for atlas tile copies
    this._copyFbo = gl.createFramebuffer()
    if (!this._copyFbo) throw new Error('Failed to create copy FBO')
    this._atlasFbo = gl.createFramebuffer()
    if (!this._atlasFbo) throw new Error('Failed to create atlas FBO')
  }

  /** An RGBA32F stateSize² texture holding every particle's first state. */
  private _createStateTexture(gl: WebGL2RenderingContext): WebGLTexture {
    const data = new Float32Array(this._particleCount * 4)
    for (let i = 0; i < this._particleCount; i++) this._initialState(data, i * 4)
    return createTexture(gl, gl.RGBA32F, this._stateSize, this._stateSize, gl.RGBA, gl.FLOAT, gl.NEAREST, data)
  }

  /** Create or recreate the trail textures at the given size. */
  private _resizeTrailTextures(gl: WebGL2RenderingContext, width: number, height: number): void {
    this._deleteTrail(gl)
    this._trailWidth = width
    this._trailHeight = height
    const textures: Slots = [null, null]
    const fbos: [WebGLFramebuffer | null, WebGLFramebuffer | null] = [null, null]
    for (let i = 0; i < 2; i++) {
      textures[i] = createTexture(gl, gl.RGBA8, width, height, gl.RGBA, gl.UNSIGNED_BYTE, gl.LINEAR, null)
      fbos[i] = createFBO(gl, textures[i]!)
      // Start transparent
      gl.bindFramebuffer(gl.FRAMEBUFFER, fbos[i])
      gl.viewport(0, 0, width, height)
      gl.clearColor(0, 0, 0, 0)
      gl.clear(gl.COLOR_BUFFER_BIT)
    }
    gl.bindFramebuffer(gl.FRAMEBUFFER, null)
    this._trailTextures = textures
    this._trailFbos = fbos
    this._trailReadIndex = 0
  }

  private _deleteTrail(gl: WebGL2RenderingContext): void {
    for (const fbo of this._trailFbos ?? []) if (fbo) gl.deleteFramebuffer(fbo)
    for (const tex of this._trailTextures ?? []) if (tex) gl.deleteTexture(tex)
    this._trailFbos = null
    this._trailTextures = null
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
      const slot = { col: rx - minRX, row: c.y - minY }
      const slots = slotsByKey.get(key)
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
   * Pack all visible tile textures into the atlas textures for GPU sampling.
   * Returns the atlas layout for setting shader uniforms.
   */
  private _packAtlas(gl: WebGL2RenderingContext, visibleCoords: TileCoord[]): AtlasLayout | null {
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
      this._atlasLayout = { ...computed.layout, hasAnyTile: false }
      this._atlasLayoutKey = computed.layoutKey
      this._atlasDirtyT0 = new Set(computed.visibleKeys)
      this._atlasDirtyT1 = this._forecastHourT1 >= 0 ? new Set(computed.visibleKeys) : new Set()
    }

    let blits = this._flushDirtyAtlas(gl, this._atlasDirtyT0, this._t0, this._atlasT0)
    if (this._forecastHourT1 >= 0) {
      blits += this._flushDirtyAtlas(gl, this._atlasDirtyT1, this._t1, this._atlasT1)
    } else {
      this._atlasDirtyT1.clear()
    }
    if (!layoutChanged && blits > 0) {
      this._debug.atlasFlushes += 1
    }
    this._debug.atlasBlits += blits
    this._updateDebugPendingDirtyTiles()

    if (!this._atlasLayout) return null
    this._atlasLayout.hasAnyTile = this._hasVisibleTile()
    return this._atlasLayout
  }

  /** Copy the dirty tiles of each data layer into its atlas; returns the blit count. */
  private _flushDirtyAtlas(
    gl: WebGL2RenderingContext,
    dirtyKeys: Set<string>,
    managers: (TileManager | null)[],
    atlases: (WebGLTexture | null)[],
  ): number {
    if (dirtyKeys.size === 0) return 0

    let blits = 0
    for (const key of dirtyKeys) {
      const slots = this._atlasSlotsByKey.get(key)
      if (!slots || slots.length === 0) continue

      const { z, x, y } = parseTileKey(key)
      managers.forEach((manager, i) => {
        const tex = manager?.getTexture(z, x, y)
        const atlas = atlases[i]
        if (!tex || !atlas) return
        for (const slot of slots) {
          this._copyTileToAtlas(gl, tex, atlas, slot.col, slot.row)
          blits += 1
        }
      })
    }

    dirtyKeys.clear()
    gl.bindFramebuffer(gl.READ_FRAMEBUFFER, null)
    gl.bindFramebuffer(gl.DRAW_FRAMEBUFFER, null)
    return blits
  }

  /** Whether some visible tile has every data layer loaded for the current hour. */
  private _hasVisibleTile(): boolean {
    return this._atlasVisibleKeys.some((key) => {
      const { z, x, y } = parseTileKey(key)
      return this._t0.every((m) => m?.getTexture(z, x, y))
    })
  }

  /** Allocate the atlas textures at the required dimensions. */
  private _ensureAtlas(gl: WebGL2RenderingContext, cols: number, rows: number): void {
    const width = cols * TILE_SIZE
    const height = rows * TILE_SIZE
    if (width === this._atlasWidth && height === this._atlasHeight) return

    this._deleteAtlasTextures(gl)
    this._atlasWidth = width
    this._atlasHeight = height
    const create = () => this._tileFormat === 'f16'
      ? createTexture(gl, gl.R16F, width, height, gl.RED, gl.HALF_FLOAT, gl.NEAREST, null)
      : createTexture(gl, gl.RGBA8, width, height, gl.RGBA, gl.UNSIGNED_BYTE, gl.NEAREST, null)
    this._atlasT0 = this._spec.layers.map(create)
    this._atlasT1 = this._spec.layers.map(create)
  }

  /** Clear all atlas textures to nodata values. */
  private _clearAtlas(gl: WebGL2RenderingContext): void {
    // For PNG: B > 0.5 signals nodata. For F16: R < -9000 signals nodata.
    // Use clearBufferfv for portability — clearColor may clamp to [0,1] on
    // some implementations (Safari/mobile), which would write 0 instead of
    // -9999 for F16, so unloaded atlas cells would read as real values.
    const clearValue = this._tileFormat === 'f16'
      ? new Float32Array([-9999, 0, 0, 0])
      : new Float32Array([0, 0, 1, 0])
    for (const atlas of [...this._atlasT0, ...this._atlasT1]) {
      if (!atlas) continue
      gl.bindFramebuffer(gl.FRAMEBUFFER, this._atlasFbo)
      gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, atlas, 0)
      gl.viewport(0, 0, this._atlasWidth, this._atlasHeight)
      gl.clearBufferfv(gl.COLOR, 0, clearValue)
    }
    gl.bindFramebuffer(gl.FRAMEBUFFER, null)
  }

  /** Copy a tile's own pixels (not its gutter) into an atlas at the given grid position. */
  private _copyTileToAtlas(
    gl: WebGL2RenderingContext,
    src: WebGLTexture,
    dst: WebGLTexture,
    col: number,
    row: number,
  ): void {
    const dx = col * TILE_SIZE
    const dy = row * TILE_SIZE
    // Neighbouring tiles sit next to each other in the atlas, so the
    // update shader interpolates across their edges without the gutter.
    const g = tileGutters.get(src) ?? 0
    gl.bindFramebuffer(gl.READ_FRAMEBUFFER, this._copyFbo)
    gl.framebufferTexture2D(gl.READ_FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, src, 0)
    gl.bindFramebuffer(gl.DRAW_FRAMEBUFFER, this._atlasFbo)
    gl.framebufferTexture2D(gl.DRAW_FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, dst, 0)
    gl.blitFramebuffer(
      g, g, g + TILE_SIZE, g + TILE_SIZE,
      dx, dy, dx + TILE_SIZE, dy + TILE_SIZE,
      gl.COLOR_BUFFER_BIT, gl.NEAREST,
    )
  }

  private _deleteAtlasTextures(gl: WebGL2RenderingContext): void {
    for (const tex of [...this._atlasT0, ...this._atlasT1]) if (tex) gl.deleteTexture(tex)
    this._atlasT0 = []
    this._atlasT1 = []
    this._atlasWidth = 0
    this._atlasHeight = 0
  }

  /** Free all GL resources. */
  private _cleanup(): void {
    this._active = false
    this._debug.active = false
    this._invalidateAtlas()

    for (const m of [...this._t0, ...this._t1]) m?.destroy()
    this._t0 = []
    this._t1 = []

    const gl = this._gl
    if (gl) {
      this._deleteTrail(gl)
      for (const fbo of this._stateFbos ?? []) if (fbo) gl.deleteFramebuffer(fbo)
      for (const tex of this._stateTextures ?? []) if (tex) gl.deleteTexture(tex)
      this._deleteAtlasTextures(gl)
      if (this._copyFbo) gl.deleteFramebuffer(this._copyFbo)
      if (this._atlasFbo) gl.deleteFramebuffer(this._atlasFbo)
      if (this._drawVao) gl.deleteVertexArray(this._drawVao)
      if (this._quad) deleteQuadGeometry(gl, this._quad)
      for (const program of [this._updateProgram, this._drawProgram, this._compositeProgram]) {
        if (program) deleteProgram(gl, program)
      }
    }

    this._stateFbos = null
    this._stateTextures = null
    this._copyFbo = null
    this._atlasFbo = null
    this._drawVao = null
    this._quad = null
    this._updateProgram = null
    this._drawProgram = null
    this._compositeProgram = null
    this._uUpdate = this._uDraw = this._uComposite = () => null
    this._gl = null
    this._map = null
  }
}

function uniformLookup(gl: WebGL2RenderingContext, program: WebGLProgram): UniformLookup {
  const cache = new Map<string, WebGLUniformLocation | null>()
  return (name) => {
    let location = cache.get(name)
    if (location === undefined) {
      location = gl.getUniformLocation(program, name)
      cache.set(name, location)
    }
    return location
  }
}

/** A clamped 2D texture; `data` null leaves it uninitialised. */
function createTexture(
  gl: WebGL2RenderingContext,
  internalFormat: number,
  width: number,
  height: number,
  format: number,
  type: number,
  filter: number,
  data: ArrayBufferView | null,
): WebGLTexture {
  const tex = gl.createTexture()
  if (!tex) throw new Error('Failed to create texture')
  gl.bindTexture(gl.TEXTURE_2D, tex)
  gl.texImage2D(gl.TEXTURE_2D, 0, internalFormat, width, height, 0, format, type, data)
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, filter)
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, filter)
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
  gl.bindTexture(gl.TEXTURE_2D, null)
  return tex
}

/** An FBO that renders to the given texture. */
function createFBO(gl: WebGL2RenderingContext, tex: WebGLTexture): WebGLFramebuffer {
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

/** Latitude to web mercator Y in [0,1]. */
function latToMercatorY(lat: number): number {
  const sinLat = Math.sin((lat * Math.PI) / 180)
  const y = 0.5 - (Math.log((1 + sinLat) / (1 - sinLat)) / (4 * Math.PI))
  return Math.max(0, Math.min(1, y))
}

function tileKey(z: number, x: number, y: number): string {
  return `${z}/${x}/${y}`
}

function parseTileKey(key: string): { z: number; x: number; y: number } {
  const [z, x, y] = key.split('/').map(Number)
  return { z, x, y }
}

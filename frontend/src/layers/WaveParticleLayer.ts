/**
 * GPU wave-dash layer.
 *
 * Unlike the wind layer, waves are not rendered as long-lived advected tracers.
 * Each frame rebuilds a world-anchored dash field from wave height, period,
 * and direction-vector tiles. This keeps density uniform and avoids the
 * non-physical streaking artifact caused by particle convergence.
 */

import type {
  CustomLayerInterface,
  CustomRenderMethodInput,
  Map as MaplibreMap,
} from 'maplibre-gl'

import updateVertSource from './shaders/particle-update.vert.glsl?raw'
import updateFragSource from './shaders/wave-particle-update.frag.glsl?raw'
import drawVertSource from './shaders/wave-particle-draw.vert.glsl?raw'
import drawFragSource from './shaders/wave-particle-draw.frag.glsl?raw'
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

const DEFAULT_STATE_SIZE = 50
const TRAIL_FADE = 0.92
const GRID_SPACING_PX = 24.0
const PHASE_AMPLITUDE_PX = 22.0
const SPEED_MAX = 15.0
const POINT_SIZE = 14.0
const PERF_WINDOW = 60
const PERF_WARN_THRESHOLD_MS = 20
const TILE_SIZE = 256

export interface WaveParticleLayerOptions {
  id?: string
  opacity?: number
  apiBase?: string
  tileFormat?: TileFormat
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

export class WaveParticleLayer implements CustomLayerInterface {
  readonly id: string
  readonly type = 'custom' as const
  readonly renderingMode = '2d' as const

  private _map: MaplibreMap | null = null
  private _gl: WebGL2RenderingContext | null = null
  private _opacity: number
  private _apiBase: string
  private _tileFormat: TileFormat
  private _maxTextureSize = 4096

  private _stateSize: number
  private _particleCount: number
  private _gpuTier: GpuTier = 'medium'
  private _stateSizeOverride: number | undefined

  private _waveHeightManager: TileManager | null = null
  private _wavePeriodManager: TileManager | null = null
  private _waveDirUManager: TileManager | null = null
  private _waveDirVManager: TileManager | null = null
  private _waveHeightT1Manager: TileManager | null = null
  private _wavePeriodT1Manager: TileManager | null = null
  private _waveDirUT1Manager: TileManager | null = null
  private _waveDirVT1Manager: TileManager | null = null
  private _waveConfigured = false

  private _model = ''
  private _runId = ''
  private _forecastHourT1 = -1
  private _temporalMix = 0

  private _updateProgram: GLProgram | null = null
  private _quad: QuadGeometry | null = null
  private _stateTextures: [WebGLTexture | null, WebGLTexture | null] | null = null
  private _stateFbos: [WebGLFramebuffer | null, WebGLFramebuffer | null] | null = null
  private _stateReadIndex = 0

  private _uUpdateStateTex: WebGLUniformLocation | null = null
  private _uUpdateWaveHeight: WebGLUniformLocation | null = null
  private _uUpdateWaveHeightT1: WebGLUniformLocation | null = null
  private _uUpdateWavePeriod: WebGLUniformLocation | null = null
  private _uUpdateWavePeriodT1: WebGLUniformLocation | null = null
  private _uUpdateWaveDirU: WebGLUniformLocation | null = null
  private _uUpdateWaveDirUT1: WebGLUniformLocation | null = null
  private _uUpdateWaveDirV: WebGLUniformLocation | null = null
  private _uUpdateWaveDirVT1: WebGLUniformLocation | null = null
  private _uUpdateTemporalMix: WebGLUniformLocation | null = null
  private _uUpdateHasWaveData: WebGLUniformLocation | null = null
  private _uUpdateIsFloat16: WebGLUniformLocation | null = null
  private _uUpdateValueMinHeight: WebGLUniformLocation | null = null
  private _uUpdateValueMaxHeight: WebGLUniformLocation | null = null
  private _uUpdateValueMinPeriod: WebGLUniformLocation | null = null
  private _uUpdateValueMaxPeriod: WebGLUniformLocation | null = null
  private _uUpdateValueMinDir: WebGLUniformLocation | null = null
  private _uUpdateValueMaxDir: WebGLUniformLocation | null = null
  private _uUpdateTime: WebGLUniformLocation | null = null
  private _uUpdateViewportBounds: WebGLUniformLocation | null = null
  private _uUpdateGridOriginX: WebGLUniformLocation | null = null
  private _uUpdateGridOriginY: WebGLUniformLocation | null = null
  private _uUpdateGridSpacing: WebGLUniformLocation | null = null
  private _uUpdateGridCols: WebGLUniformLocation | null = null
  private _uUpdateGridRows: WebGLUniformLocation | null = null
  private _uUpdatePhaseAmplitude: WebGLUniformLocation | null = null
  private _uUpdateAtlasOriginX: WebGLUniformLocation | null = null
  private _uUpdateAtlasOriginY: WebGLUniformLocation | null = null
  private _uUpdateAtlasZoom: WebGLUniformLocation | null = null
  private _uUpdateAtlasCols: WebGLUniformLocation | null = null
  private _uUpdateAtlasRows: WebGLUniformLocation | null = null

  private _drawProgram: GLProgram | null = null
  private _drawVao: WebGLVertexArrayObject | null = null

  private _uDrawStateTex: WebGLUniformLocation | null = null
  private _uDrawMatrix: WebGLUniformLocation | null = null
  private _uDrawPointSize: WebGLUniformLocation | null = null
  private _uDrawSpeedMax: WebGLUniformLocation | null = null

  private _compositeProgram: GLProgram | null = null
  private _uCompositeTexture: WebGLUniformLocation | null = null
  private _uCompositeOpacity: WebGLUniformLocation | null = null

  private _trailTextures: [WebGLTexture | null, WebGLTexture | null] | null = null
  private _trailFbos: [WebGLFramebuffer | null, WebGLFramebuffer | null] | null = null
  private _trailReadIndex = 0
  private _trailWidth = 0
  private _trailHeight = 0

  private _atlasWaveH: WebGLTexture | null = null
  private _atlasPeriod: WebGLTexture | null = null
  private _atlasDirU: WebGLTexture | null = null
  private _atlasDirV: WebGLTexture | null = null
  private _atlasHeightT1: WebGLTexture | null = null
  private _atlasPeriodT1: WebGLTexture | null = null
  private _atlasDirUT1: WebGLTexture | null = null
  private _atlasDirVT1: WebGLTexture | null = null
  private _atlasWidth = 0
  private _atlasHeight = 0
  private _copyFbo: WebGLFramebuffer | null = null
  private _atlasFbo: WebGLFramebuffer | null = null

  private _panTracker = new PanVelocityTracker()

  private _lastFrameTime = 0
  private _frameTimes: number[] = []
  private _perfWarned = false

  constructor(options: WaveParticleLayerOptions = {}) {
    this.id = options.id ?? 'wave-particles'
    this._opacity = options.opacity ?? 0.8
    this._apiBase = options.apiBase ?? ''
    this._tileFormat = options.tileFormat ?? 'png'
    this._stateSizeOverride = options.stateSize
    this._stateSize = DEFAULT_STATE_SIZE
    this._particleCount = DEFAULT_STATE_SIZE * DEFAULT_STATE_SIZE
  }

  onAdd(map: MaplibreMap, gl: WebGLRenderingContext | WebGL2RenderingContext): void {
    if (!(gl instanceof WebGL2RenderingContext)) {
      console.error('[WaveParticleLayer] WebGL2 is required')
      return
    }

    this._map = map
    this._gl = gl

    if (!gl.getExtension('EXT_color_buffer_float')) {
      console.error('[WaveParticleLayer] EXT_color_buffer_float not supported')
      return
    }

    this._maxTextureSize = gl.getParameter(gl.MAX_TEXTURE_SIZE) as number

    if (this._stateSizeOverride != null) {
      this._stateSize = clampStateSize(this._stateSizeOverride)
    } else {
      const tier = detectGpuTier(gl)
      this._gpuTier = tier.tier
      this._stateSize = clampStateSize(Math.round(tier.stateSize * 0.5))
      console.info(
        `[WaveParticleLayer] GPU: "${tier.renderer}" → tier=${tier.tier}, ` +
        `stateSize=${this._stateSize} (${this._stateSize ** 2} dash slots)`
      )
    }
    this._particleCount = this._stateSize * this._stateSize

    try {
      this._initResources(gl)
      this._initTileManagers(gl)
    } catch (e) {
      console.error('[WaveParticleLayer] Initialization failed:', e)
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

    const now = performance.now() / 1000
    const dt = this._lastFrameTime > 0 ? Math.min(now - this._lastFrameTime, 0.1) : 0.016
    this._lastFrameTime = now

    const frameTimeMs = dt * 1000
    this._frameTimes.push(frameTimeMs)
    if (this._frameTimes.length > PERF_WINDOW) this._frameTimes.shift()
    if (!this._perfWarned && this._frameTimes.length === PERF_WINDOW) {
      const avg = this._frameTimes.reduce((a, b) => a + b, 0) / PERF_WINDOW
      if (avg > PERF_WARN_THRESHOLD_MS) {
        console.warn(
          `[WaveParticleLayer] Low FPS detected: avg frame time ${avg.toFixed(1)}ms ` +
          `(${(1000 / avg).toFixed(0)} fps) with ${this._particleCount} dash slots ` +
          `(tier=${this._gpuTier}).`
        )
        this._perfWarned = true
      }
    }

    const zoom = Math.max(0, Math.min(8, Math.floor(this._map.getZoom())))
    const mapBounds = this._map.getBounds()
    const visibleCoords = computeVisibleTiles({
      west: mapBounds.getWest(),
      north: mapBounds.getNorth(),
      east: mapBounds.getEast(),
      south: mapBounds.getSouth(),
    }, zoom)

    let panEast = mapBounds.getEast()
    const panWest = mapBounds.getWest()
    if (panEast < panWest) panEast += 360
    const centerLng = (panWest + panEast) / 2
    const centerLat = (mapBounds.getNorth() + mapBounds.getSouth()) / 2
    const panDir = this._panTracker.update(centerLng, centerLat)
    const prefetchCoords = panDir
      ? computePanPrefetchTiles(visibleCoords, panDir, zoom)
      : []

    if (this._waveConfigured) {
      this._updateManagers(visibleCoords, prefetchCoords)
    }

    const prevProgram = gl.getParameter(gl.CURRENT_PROGRAM) as WebGLProgram | null
    const prevFbo = gl.getParameter(gl.FRAMEBUFFER_BINDING) as WebGLFramebuffer | null
    const prevActiveTexture = gl.getParameter(gl.ACTIVE_TEXTURE) as number
    const prevViewport = gl.getParameter(gl.VIEWPORT) as Int32Array
    const prevBlend = gl.getParameter(gl.BLEND) as boolean
    const prevBlendSrc = gl.getParameter(gl.BLEND_SRC_RGB) as number
    const prevBlendDst = gl.getParameter(gl.BLEND_DST_RGB) as number
    const prevBlendSrcA = gl.getParameter(gl.BLEND_SRC_ALPHA) as number
    const prevBlendDstA = gl.getParameter(gl.BLEND_DST_ALPHA) as number

    const atlas = this._waveConfigured ? this._packAtlas(gl, visibleCoords) : null
    const hasWaveData = atlas?.hasAnyTile ?? false

    const canvasW = gl.drawingBufferWidth
    const canvasH = gl.drawingBufferHeight
    if (canvasW !== this._trailWidth || canvasH !== this._trailHeight) {
      this._resizeTrailTextures(gl, canvasW, canvasH)
    }
    if (!this._trailTextures || !this._trailFbos) return

    const worldSize = 512 * Math.pow(2, this._map.getZoom())
    const bounds = this._map.getBounds()
    let vpEast = bounds.getEast()
    const vpWest = bounds.getWest()
    if (vpEast < vpWest) vpEast += 360
    const vpMinLon = (vpWest + 180) / 360
    const vpMaxLon = (vpEast + 180) / 360
    const vpMinLat = this._latToMercatorY(bounds.getNorth())
    const vpMaxLat = this._latToMercatorY(bounds.getSouth())
    const gridSpacing = GRID_SPACING_PX / worldSize
    const gridOriginX = Math.floor(vpMinLon / gridSpacing) * gridSpacing
    const gridOriginY = Math.floor(vpMinLat / gridSpacing) * gridSpacing
    const gridCols = Math.max(1, Math.ceil((vpMaxLon - gridOriginX) / gridSpacing))
    const gridRows = Math.max(1, Math.ceil((vpMaxLat - gridOriginY) / gridSpacing))
    const activeParticleCount = Math.min(this._particleCount, gridCols * gridRows)

    const stateRead = this._stateReadIndex
    const stateWrite = 1 - stateRead

    gl.bindFramebuffer(gl.FRAMEBUFFER, this._stateFbos[stateWrite])
    gl.viewport(0, 0, this._stateSize, this._stateSize)
    gl.disable(gl.BLEND)
    gl.useProgram(this._updateProgram.program)

    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this._stateTextures[stateRead])
    gl.uniform1i(this._uUpdateStateTex, 0)

    const hasT1 = this._forecastHourT1 >= 0 && this._temporalMix > 0

    gl.activeTexture(gl.TEXTURE1)
    gl.bindTexture(gl.TEXTURE_2D, atlas ? this._atlasWaveH : null)
    gl.uniform1i(this._uUpdateWaveHeight, 1)
    gl.activeTexture(gl.TEXTURE2)
    gl.bindTexture(gl.TEXTURE_2D, atlas && hasT1 ? this._atlasHeightT1 : null)
    gl.uniform1i(this._uUpdateWaveHeightT1, 2)

    gl.activeTexture(gl.TEXTURE3)
    gl.bindTexture(gl.TEXTURE_2D, atlas ? this._atlasPeriod : null)
    gl.uniform1i(this._uUpdateWavePeriod, 3)
    gl.activeTexture(gl.TEXTURE4)
    gl.bindTexture(gl.TEXTURE_2D, atlas && hasT1 ? this._atlasPeriodT1 : null)
    gl.uniform1i(this._uUpdateWavePeriodT1, 4)

    gl.activeTexture(gl.TEXTURE5)
    gl.bindTexture(gl.TEXTURE_2D, atlas ? this._atlasDirU : null)
    gl.uniform1i(this._uUpdateWaveDirU, 5)
    gl.activeTexture(gl.TEXTURE6)
    gl.bindTexture(gl.TEXTURE_2D, atlas && hasT1 ? this._atlasDirUT1 : null)
    gl.uniform1i(this._uUpdateWaveDirUT1, 6)

    gl.activeTexture(gl.TEXTURE7)
    gl.bindTexture(gl.TEXTURE_2D, atlas ? this._atlasDirV : null)
    gl.uniform1i(this._uUpdateWaveDirV, 7)
    gl.activeTexture(gl.TEXTURE8)
    gl.bindTexture(gl.TEXTURE_2D, atlas && hasT1 ? this._atlasDirVT1 : null)
    gl.uniform1i(this._uUpdateWaveDirVT1, 8)

    gl.uniform1f(this._uUpdateTemporalMix, hasWaveData && hasT1 ? this._temporalMix : 0)
    gl.uniform1i(this._uUpdateHasWaveData, hasWaveData ? 1 : 0)
    gl.uniform1i(this._uUpdateIsFloat16, this._tileFormat === 'f16' ? 1 : 0)
    gl.uniform1f(this._uUpdateValueMinHeight, 0)
    gl.uniform1f(this._uUpdateValueMaxHeight, 15)
    gl.uniform1f(this._uUpdateValueMinPeriod, 0)
    gl.uniform1f(this._uUpdateValueMaxPeriod, 25)
    gl.uniform1f(this._uUpdateValueMinDir, -1)
    gl.uniform1f(this._uUpdateValueMaxDir, 1)
    gl.uniform1f(this._uUpdateTime, now)
    gl.uniform4f(this._uUpdateViewportBounds, vpMinLon, vpMinLat, vpMaxLon, vpMaxLat)
    gl.uniform1f(this._uUpdateGridOriginX, gridOriginX)
    gl.uniform1f(this._uUpdateGridOriginY, gridOriginY)
    gl.uniform1f(this._uUpdateGridSpacing, gridSpacing)
    gl.uniform1f(this._uUpdateGridCols, gridCols)
    gl.uniform1f(this._uUpdateGridRows, gridRows)
    gl.uniform1f(this._uUpdatePhaseAmplitude, PHASE_AMPLITUDE_PX / worldSize)

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

    const trailRead = this._trailReadIndex
    const trailWrite = 1 - trailRead
    gl.bindFramebuffer(gl.FRAMEBUFFER, this._trailFbos[trailWrite])
    gl.viewport(0, 0, this._trailWidth, this._trailHeight)
    gl.clearColor(0, 0, 0, 0)
    gl.clear(gl.COLOR_BUFFER_BIT)

    gl.enable(gl.BLEND)
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA)

    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this._trailTextures[trailRead])
    gl.useProgram(this._compositeProgram.program)
    gl.uniform1i(this._uCompositeTexture, 0)
    gl.uniform1f(this._uCompositeOpacity, TRAIL_FADE)
    gl.bindVertexArray(this._quad.vao)
    gl.drawArrays(gl.TRIANGLES, 0, this._quad.vertexCount)
    gl.bindVertexArray(null)

    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this._stateTextures[this._stateReadIndex])
    gl.useProgram(this._drawProgram.program)
    gl.uniform1i(this._uDrawStateTex, 0)
    gl.uniform1f(this._uDrawPointSize, POINT_SIZE)
    gl.uniform1f(this._uDrawSpeedMax, SPEED_MAX)

    const mvp = options.modelViewProjectionMatrix
    const mercatorMatrix = new Float32Array(16)
    for (let i = 0; i < 4; i++) {
      mercatorMatrix[i] = mvp[i] * worldSize
      mercatorMatrix[4 + i] = mvp[4 + i] * worldSize
      mercatorMatrix[8 + i] = mvp[8 + i]
      mercatorMatrix[12 + i] = mvp[12 + i]
    }
    gl.uniformMatrix4fv(this._uDrawMatrix, false, mercatorMatrix)

    gl.bindVertexArray(this._drawVao)
    gl.drawArrays(gl.POINTS, 0, activeParticleCount)
    gl.bindVertexArray(null)
    this._trailReadIndex = trailWrite

    gl.bindFramebuffer(gl.FRAMEBUFFER, prevFbo)
    gl.viewport(prevViewport[0], prevViewport[1], prevViewport[2], prevViewport[3])
    gl.enable(gl.BLEND)
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA)

    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this._trailTextures[this._trailReadIndex])
    gl.useProgram(this._compositeProgram.program)
    gl.uniform1i(this._uCompositeTexture, 0)
    gl.uniform1f(this._uCompositeOpacity, this._opacity)
    gl.bindVertexArray(this._quad.vao)
    gl.drawArrays(gl.TRIANGLES, 0, this._quad.vertexCount)
    gl.bindVertexArray(null)

    gl.activeTexture(prevActiveTexture)
    if (prevBlend) {
      gl.enable(gl.BLEND)
      gl.blendFuncSeparate(prevBlendSrc, prevBlendDst, prevBlendSrcA, prevBlendDstA)
    } else {
      gl.disable(gl.BLEND)
    }
    gl.useProgram(prevProgram)

    if (hasWaveData || this._temporalMix > 0) {
      this._map.triggerRepaint()
    }
  }

  onRemove(): void {
    this._cleanup()
  }

  setWaveConfig(model: string, runId: string, forecastHour: number): void {
    this._model = model
    this._runId = runId

    this._waveHeightManager?.setLayer(model, runId, 'wave_height', forecastHour)
    this._wavePeriodManager?.setLayer(model, runId, 'wave_period', forecastHour)
    this._waveDirUManager?.setLayer(model, runId, 'wave_dir_u', forecastHour)
    this._waveDirVManager?.setLayer(model, runId, 'wave_dir_v', forecastHour)
    this._waveConfigured = true
    this._map?.triggerRepaint()
  }

  setTemporalBlend(forecastHourT1: number, mix: number): void {
    this._forecastHourT1 = forecastHourT1
    this._temporalMix = Math.max(0, Math.min(1, mix))

    if (forecastHourT1 >= 0 && this._model && this._runId) {
      this._waveHeightT1Manager?.setLayer(this._model, this._runId, 'wave_height', forecastHourT1)
      this._wavePeriodT1Manager?.setLayer(this._model, this._runId, 'wave_period', forecastHourT1)
      this._waveDirUT1Manager?.setLayer(this._model, this._runId, 'wave_dir_u', forecastHourT1)
      this._waveDirVT1Manager?.setLayer(this._model, this._runId, 'wave_dir_v', forecastHourT1)
    }
    this._map?.triggerRepaint()
  }

  advanceForecastHour(newHour: number): void {
    if (
      this._waveHeightT1Manager &&
      this._waveHeightT1Manager.cacheSize > 0 &&
      this._waveHeightT1Manager.currentForecastHour === newHour &&
      this._wavePeriodT1Manager &&
      this._wavePeriodT1Manager.cacheSize > 0 &&
      this._wavePeriodT1Manager.currentForecastHour === newHour &&
      this._waveDirUT1Manager &&
      this._waveDirUT1Manager.cacheSize > 0 &&
      this._waveDirUT1Manager.currentForecastHour === newHour &&
      this._waveDirVT1Manager &&
      this._waveDirVT1Manager.cacheSize > 0 &&
      this._waveDirVT1Manager.currentForecastHour === newHour
    ) {
      ;[this._waveHeightManager, this._waveHeightT1Manager] = [this._waveHeightT1Manager, this._waveHeightManager]
      ;[this._wavePeriodManager, this._wavePeriodT1Manager] = [this._wavePeriodT1Manager, this._wavePeriodManager]
      ;[this._waveDirUManager, this._waveDirUT1Manager] = [this._waveDirUT1Manager, this._waveDirUManager]
      ;[this._waveDirVManager, this._waveDirVT1Manager] = [this._waveDirVT1Manager, this._waveDirVManager]
    } else if (this._model && this._runId) {
      this._waveHeightManager?.setLayer(this._model, this._runId, 'wave_height', newHour)
      this._wavePeriodManager?.setLayer(this._model, this._runId, 'wave_period', newHour)
      this._waveDirUManager?.setLayer(this._model, this._runId, 'wave_dir_u', newHour)
      this._waveDirVManager?.setLayer(this._model, this._runId, 'wave_dir_v', newHour)
    }
    this._temporalMix = 0
    this._map?.triggerRepaint()
  }

  isT1Ready(): boolean {
    return (
      (this._waveHeightT1Manager?.cacheSize ?? 0) > 0 &&
      (this._wavePeriodT1Manager?.cacheSize ?? 0) > 0 &&
      (this._waveDirUT1Manager?.cacheSize ?? 0) > 0 &&
      (this._waveDirVT1Manager?.cacheSize ?? 0) > 0
    )
  }

  get stateTexture(): WebGLTexture | null {
    return this._stateTextures?.[this._stateReadIndex] ?? null
  }

  get stateSize(): number {
    return this._stateSize
  }

  get particleCount(): number {
    return this._particleCount
  }

  get gpuTier(): GpuTier {
    return this._gpuTier
  }

  setOpacity(opacity: number): void {
    this._opacity = Math.max(0, Math.min(1, opacity))
    this._map?.triggerRepaint()
  }

  private _latToMercatorY(lat: number): number {
    const sinLat = Math.sin((lat * Math.PI) / 180)
    const y = 0.5 - (Math.log((1 + sinLat) / (1 - sinLat)) / (4 * Math.PI))
    return Math.max(0, Math.min(1, y))
  }

  private _updateManagers(visibleCoords: TileCoord[], prefetchCoords: TileCoord[]): void {
    this._waveHeightManager?.updateVisibleTiles(visibleCoords, 0)
    this._wavePeriodManager?.updateVisibleTiles(visibleCoords, 0)
    this._waveDirUManager?.updateVisibleTiles(visibleCoords, 0)
    this._waveDirVManager?.updateVisibleTiles(visibleCoords, 0)
    if (prefetchCoords.length > 0) {
      this._waveHeightManager?.updateVisibleTiles(prefetchCoords, 2)
      this._wavePeriodManager?.updateVisibleTiles(prefetchCoords, 2)
      this._waveDirUManager?.updateVisibleTiles(prefetchCoords, 2)
      this._waveDirVManager?.updateVisibleTiles(prefetchCoords, 2)
    }
    if (this._forecastHourT1 >= 0 && this._temporalMix > 0) {
      this._waveHeightT1Manager?.updateVisibleTiles(visibleCoords, 1)
      this._wavePeriodT1Manager?.updateVisibleTiles(visibleCoords, 1)
      this._waveDirUT1Manager?.updateVisibleTiles(visibleCoords, 1)
      this._waveDirVT1Manager?.updateVisibleTiles(visibleCoords, 1)
      if (prefetchCoords.length > 0) {
        this._waveHeightT1Manager?.updateVisibleTiles(prefetchCoords, 2)
        this._wavePeriodT1Manager?.updateVisibleTiles(prefetchCoords, 2)
        this._waveDirUT1Manager?.updateVisibleTiles(prefetchCoords, 2)
        this._waveDirVT1Manager?.updateVisibleTiles(prefetchCoords, 2)
      }
    }
  }

  private _initTileManagers(gl: WebGL2RenderingContext): void {
    const triggerRepaint = () => this._map?.triggerRepaint()
    const fetchClient = getTileFetchClient()
    const tmOpts = { apiBase: this._apiBase, format: this._tileFormat, fetchClient }

    this._waveHeightManager = new TileManager(gl, tmOpts)
    this._waveHeightManager.onTileLoaded = triggerRepaint
    this._wavePeriodManager = new TileManager(gl, tmOpts)
    this._wavePeriodManager.onTileLoaded = triggerRepaint
    this._waveDirUManager = new TileManager(gl, tmOpts)
    this._waveDirUManager.onTileLoaded = triggerRepaint
    this._waveDirVManager = new TileManager(gl, tmOpts)
    this._waveDirVManager.onTileLoaded = triggerRepaint

    this._waveHeightT1Manager = new TileManager(gl, tmOpts)
    this._waveHeightT1Manager.onTileLoaded = triggerRepaint
    this._wavePeriodT1Manager = new TileManager(gl, tmOpts)
    this._wavePeriodT1Manager.onTileLoaded = triggerRepaint
    this._waveDirUT1Manager = new TileManager(gl, tmOpts)
    this._waveDirUT1Manager.onTileLoaded = triggerRepaint
    this._waveDirVT1Manager = new TileManager(gl, tmOpts)
    this._waveDirVT1Manager.onTileLoaded = triggerRepaint
  }

  private _initResources(gl: WebGL2RenderingContext): void {
    this._updateProgram = createProgram(gl, updateVertSource, updateFragSource)
    this._drawProgram = createProgram(gl, drawVertSource, drawFragSource)
    this._compositeProgram = createProgram(gl, updateVertSource, trailFragSource)
    this._quad = createFullscreenQuad(gl)

    this._drawVao = gl.createVertexArray()
    if (!this._drawVao) throw new Error('Failed to create draw VAO')

    const up = this._updateProgram.program
    this._uUpdateStateTex = gl.getUniformLocation(up, 'u_stateTex')
    this._uUpdateWaveHeight = gl.getUniformLocation(up, 'u_waveHeight')
    this._uUpdateWaveHeightT1 = gl.getUniformLocation(up, 'u_waveHeightT1')
    this._uUpdateWavePeriod = gl.getUniformLocation(up, 'u_wavePeriod')
    this._uUpdateWavePeriodT1 = gl.getUniformLocation(up, 'u_wavePeriodT1')
    this._uUpdateWaveDirU = gl.getUniformLocation(up, 'u_waveDirU')
    this._uUpdateWaveDirUT1 = gl.getUniformLocation(up, 'u_waveDirUT1')
    this._uUpdateWaveDirV = gl.getUniformLocation(up, 'u_waveDirV')
    this._uUpdateWaveDirVT1 = gl.getUniformLocation(up, 'u_waveDirVT1')
    this._uUpdateTemporalMix = gl.getUniformLocation(up, 'u_temporalMix')
    this._uUpdateHasWaveData = gl.getUniformLocation(up, 'u_hasWaveData')
    this._uUpdateIsFloat16 = gl.getUniformLocation(up, 'u_isFloat16')
    this._uUpdateValueMinHeight = gl.getUniformLocation(up, 'u_valueMinHeight')
    this._uUpdateValueMaxHeight = gl.getUniformLocation(up, 'u_valueMaxHeight')
    this._uUpdateValueMinPeriod = gl.getUniformLocation(up, 'u_valueMinPeriod')
    this._uUpdateValueMaxPeriod = gl.getUniformLocation(up, 'u_valueMaxPeriod')
    this._uUpdateValueMinDir = gl.getUniformLocation(up, 'u_valueMinDir')
    this._uUpdateValueMaxDir = gl.getUniformLocation(up, 'u_valueMaxDir')
    this._uUpdateTime = gl.getUniformLocation(up, 'u_time')
    this._uUpdateViewportBounds = gl.getUniformLocation(up, 'u_viewportBounds')
    this._uUpdateGridOriginX = gl.getUniformLocation(up, 'u_gridOriginX')
    this._uUpdateGridOriginY = gl.getUniformLocation(up, 'u_gridOriginY')
    this._uUpdateGridSpacing = gl.getUniformLocation(up, 'u_gridSpacing')
    this._uUpdateGridCols = gl.getUniformLocation(up, 'u_gridCols')
    this._uUpdateGridRows = gl.getUniformLocation(up, 'u_gridRows')
    this._uUpdatePhaseAmplitude = gl.getUniformLocation(up, 'u_phaseAmplitude')
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

    const stateTexture0 = this._createStateTexture(gl)
    const stateTexture1 = this._createStateTexture(gl)
    this._stateTextures = [stateTexture0, stateTexture1]
    this._stateFbos = [
      this._createFBO(gl, stateTexture0),
      this._createFBO(gl, stateTexture1),
    ]
    this._initializeParticles(gl, stateTexture0)
    this._initializeParticles(gl, stateTexture1)
    this._stateReadIndex = 0

    this._copyFbo = gl.createFramebuffer()
    if (!this._copyFbo) throw new Error('Failed to create copy FBO')
    this._atlasFbo = gl.createFramebuffer()
    if (!this._atlasFbo) throw new Error('Failed to create atlas FBO')
  }

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

  private _initializeParticles(gl: WebGL2RenderingContext, tex: WebGLTexture): void {
    const data = new Float32Array(this._particleCount * 4)
    for (let i = 0; i < this._particleCount; i++) {
      const offset = i * 4
      data[offset + 0] = -1.0
      data[offset + 1] = -1.0
      data[offset + 2] = 0.5
      data[offset + 3] = 0.0
    }
    gl.bindTexture(gl.TEXTURE_2D, tex)
    gl.texSubImage2D(gl.TEXTURE_2D, 0, 0, 0, this._stateSize, this._stateSize, gl.RGBA, gl.FLOAT, data)
    gl.bindTexture(gl.TEXTURE_2D, null)
  }

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

  private _resizeTrailTextures(gl: WebGL2RenderingContext, width: number, height: number): void {
    if (this._trailFbos) {
      for (const fbo of this._trailFbos) if (fbo) gl.deleteFramebuffer(fbo)
    }
    if (this._trailTextures) {
      for (const tex of this._trailTextures) if (tex) gl.deleteTexture(tex)
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

    for (let i = 0; i < 2; i++) {
      gl.bindFramebuffer(gl.FRAMEBUFFER, this._trailFbos[i])
      gl.viewport(0, 0, width, height)
      gl.clearColor(0, 0, 0, 0)
      gl.clear(gl.COLOR_BUFFER_BIT)
    }
    gl.bindFramebuffer(gl.FRAMEBUFFER, null)
    this._trailReadIndex = 0
  }

  private _packAtlas(gl: WebGL2RenderingContext, visibleCoords: TileCoord[]): AtlasLayout | null {
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

    this._ensureAtlas(gl, cols, rows)
    this._clearAtlas(gl)

    let hasAnyTile = false
    for (const c of visibleCoords) {
      const rx = c.x + c.wrap * n
      const col = rx - minRX
      const row = c.y - minY

      const heightTex = this._waveHeightManager?.getTexture(c.z, c.x, c.y) ?? null
      const periodTex = this._wavePeriodManager?.getTexture(c.z, c.x, c.y) ?? null
      const dirUTex = this._waveDirUManager?.getTexture(c.z, c.x, c.y) ?? null
      const dirVTex = this._waveDirVManager?.getTexture(c.z, c.x, c.y) ?? null

      if (heightTex && this._atlasWaveH) this._copyTileToAtlas(gl, heightTex, this._atlasWaveH, col, row)
      if (periodTex && this._atlasPeriod) this._copyTileToAtlas(gl, periodTex, this._atlasPeriod, col, row)
      if (dirUTex && this._atlasDirU) this._copyTileToAtlas(gl, dirUTex, this._atlasDirU, col, row)
      if (dirVTex && this._atlasDirV) this._copyTileToAtlas(gl, dirVTex, this._atlasDirV, col, row)

      if (heightTex && periodTex && dirUTex && dirVTex) {
        hasAnyTile = true
      }
    }

    if (this._forecastHourT1 >= 0 && this._temporalMix > 0) {
      for (const c of visibleCoords) {
        const rx = c.x + c.wrap * n
        const col = rx - minRX
        const row = c.y - minY

        const heightT1 = this._waveHeightT1Manager?.getTexture(c.z, c.x, c.y) ?? null
        const periodT1 = this._wavePeriodT1Manager?.getTexture(c.z, c.x, c.y) ?? null
        const dirUT1 = this._waveDirUT1Manager?.getTexture(c.z, c.x, c.y) ?? null
        const dirVT1 = this._waveDirVT1Manager?.getTexture(c.z, c.x, c.y) ?? null

        if (heightT1 && this._atlasHeightT1) this._copyTileToAtlas(gl, heightT1, this._atlasHeightT1, col, row)
        if (periodT1 && this._atlasPeriodT1) this._copyTileToAtlas(gl, periodT1, this._atlasPeriodT1, col, row)
        if (dirUT1 && this._atlasDirUT1) this._copyTileToAtlas(gl, dirUT1, this._atlasDirUT1, col, row)
        if (dirVT1 && this._atlasDirVT1) this._copyTileToAtlas(gl, dirVT1, this._atlasDirVT1, col, row)
      }
    }

    gl.bindFramebuffer(gl.READ_FRAMEBUFFER, null)
    gl.bindFramebuffer(gl.DRAW_FRAMEBUFFER, null)

    return { cols, rows, originX: minRX, originY: minY, zoom, hasAnyTile }
  }

  private _ensureAtlas(gl: WebGL2RenderingContext, cols: number, rows: number): void {
    const width = cols * TILE_SIZE
    const height = rows * TILE_SIZE
    if (width === this._atlasWidth && height === this._atlasHeight) return

    this._deleteAtlasTextures(gl)
    this._atlasWidth = width
    this._atlasHeight = height

    this._atlasWaveH = this._createAtlasTexture(gl, width, height)
    this._atlasPeriod = this._createAtlasTexture(gl, width, height)
    this._atlasDirU = this._createAtlasTexture(gl, width, height)
    this._atlasDirV = this._createAtlasTexture(gl, width, height)
    this._atlasHeightT1 = this._createAtlasTexture(gl, width, height)
    this._atlasPeriodT1 = this._createAtlasTexture(gl, width, height)
    this._atlasDirUT1 = this._createAtlasTexture(gl, width, height)
    this._atlasDirVT1 = this._createAtlasTexture(gl, width, height)
  }

  private _createAtlasTexture(gl: WebGL2RenderingContext, width: number, height: number): WebGLTexture {
    const tex = gl.createTexture()
    if (!tex) throw new Error('Failed to create atlas texture')
    gl.bindTexture(gl.TEXTURE_2D, tex)
    if (this._tileFormat === 'f16') {
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.R16F, width, height, 0, gl.RED, gl.HALF_FLOAT, null)
    } else {
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, width, height, 0, gl.RGBA, gl.UNSIGNED_BYTE, null)
    }
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
    gl.bindTexture(gl.TEXTURE_2D, null)
    return tex
  }

  private _clearAtlas(gl: WebGL2RenderingContext): void {
    const atlases = [
      this._atlasWaveH,
      this._atlasPeriod,
      this._atlasDirU,
      this._atlasDirV,
      this._atlasHeightT1,
      this._atlasPeriodT1,
      this._atlasDirUT1,
      this._atlasDirVT1,
    ]
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

  private _copyTileToAtlas(
    gl: WebGL2RenderingContext,
    src: WebGLTexture,
    dst: WebGLTexture,
    col: number,
    row: number,
  ): void {
    const dx = col * TILE_SIZE
    const dy = row * TILE_SIZE
    gl.bindFramebuffer(gl.READ_FRAMEBUFFER, this._copyFbo)
    gl.framebufferTexture2D(gl.READ_FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, src, 0)
    gl.bindFramebuffer(gl.DRAW_FRAMEBUFFER, this._atlasFbo)
    gl.framebufferTexture2D(gl.DRAW_FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, dst, 0)
    gl.blitFramebuffer(
      0, 0, TILE_SIZE, TILE_SIZE,
      dx, dy, dx + TILE_SIZE, dy + TILE_SIZE,
      gl.COLOR_BUFFER_BIT, gl.NEAREST,
    )
  }

  private _deleteAtlasTextures(gl: WebGL2RenderingContext): void {
    if (this._atlasWaveH) { gl.deleteTexture(this._atlasWaveH); this._atlasWaveH = null }
    if (this._atlasPeriod) { gl.deleteTexture(this._atlasPeriod); this._atlasPeriod = null }
    if (this._atlasDirU) { gl.deleteTexture(this._atlasDirU); this._atlasDirU = null }
    if (this._atlasDirV) { gl.deleteTexture(this._atlasDirV); this._atlasDirV = null }
    if (this._atlasHeightT1) { gl.deleteTexture(this._atlasHeightT1); this._atlasHeightT1 = null }
    if (this._atlasPeriodT1) { gl.deleteTexture(this._atlasPeriodT1); this._atlasPeriodT1 = null }
    if (this._atlasDirUT1) { gl.deleteTexture(this._atlasDirUT1); this._atlasDirUT1 = null }
    if (this._atlasDirVT1) { gl.deleteTexture(this._atlasDirVT1); this._atlasDirVT1 = null }
    this._atlasWidth = 0
    this._atlasHeight = 0
  }

  private _cleanup(): void {
    this._waveHeightManager?.destroy()
    this._waveHeightManager = null
    this._wavePeriodManager?.destroy()
    this._wavePeriodManager = null
    this._waveDirUManager?.destroy()
    this._waveDirUManager = null
    this._waveDirVManager?.destroy()
    this._waveDirVManager = null
    this._waveHeightT1Manager?.destroy()
    this._waveHeightT1Manager = null
    this._wavePeriodT1Manager?.destroy()
    this._wavePeriodT1Manager = null
    this._waveDirUT1Manager?.destroy()
    this._waveDirUT1Manager = null
    this._waveDirVT1Manager?.destroy()
    this._waveDirVT1Manager = null

    const gl = this._gl
    if (gl) {
      if (this._trailFbos) {
        for (const fbo of this._trailFbos) if (fbo) gl.deleteFramebuffer(fbo)
        this._trailFbos = null
      }
      if (this._trailTextures) {
        for (const tex of this._trailTextures) if (tex) gl.deleteTexture(tex)
        this._trailTextures = null
      }

      if (this._stateFbos) {
        for (const fbo of this._stateFbos) if (fbo) gl.deleteFramebuffer(fbo)
        this._stateFbos = null
      }
      if (this._stateTextures) {
        for (const tex of this._stateTextures) if (tex) gl.deleteTexture(tex)
        this._stateTextures = null
      }

      this._deleteAtlasTextures(gl)
      if (this._copyFbo) { gl.deleteFramebuffer(this._copyFbo); this._copyFbo = null }
      if (this._atlasFbo) { gl.deleteFramebuffer(this._atlasFbo); this._atlasFbo = null }

      if (this._drawVao) {
        gl.deleteVertexArray(this._drawVao)
        this._drawVao = null
      }
      if (this._quad) {
        deleteQuadGeometry(gl, this._quad)
        this._quad = null
      }
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
    this._uUpdateWaveHeight = null
    this._uUpdateWaveHeightT1 = null
    this._uUpdateWavePeriod = null
    this._uUpdateWavePeriodT1 = null
    this._uUpdateWaveDirU = null
    this._uUpdateWaveDirUT1 = null
    this._uUpdateWaveDirV = null
    this._uUpdateWaveDirVT1 = null
    this._uUpdateTemporalMix = null
    this._uUpdateHasWaveData = null
    this._uUpdateIsFloat16 = null
    this._uUpdateValueMinHeight = null
    this._uUpdateValueMaxHeight = null
    this._uUpdateValueMinPeriod = null
    this._uUpdateValueMaxPeriod = null
    this._uUpdateValueMinDir = null
    this._uUpdateValueMaxDir = null
    this._uUpdateTime = null
    this._uUpdateViewportBounds = null
    this._uUpdateGridOriginX = null
    this._uUpdateGridOriginY = null
    this._uUpdateGridSpacing = null
    this._uUpdateGridCols = null
    this._uUpdateGridRows = null
    this._uUpdatePhaseAmplitude = null
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

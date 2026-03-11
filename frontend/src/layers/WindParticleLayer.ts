/**
 * GPU wind-particle layer with trail rendering.
 *
 * Implements a three-pass pipeline per frame inside MapLibre's render loop:
 *
 *   Pass 1 — State Update (ping-pong)
 *     Read particle positions from state texture A, write updated positions
 *     to state texture B via a fullscreen-quad fragment shader, then swap.
 *     Currently a random walk; wind advection is added by wx-0pg.2.
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

/** Particles per axis in the state texture. */
const STATE_SIZE = 256
/** Total particle count: STATE_SIZE² = 65536. */
const PARTICLE_COUNT = STATE_SIZE * STATE_SIZE
/** Trail fade factor per frame. 0.95^60 ≈ 0.046 → ~2 second trails at 60fps. */
const TRAIL_FADE = 0.95

export interface WindParticleLayerOptions {
  /** Unique layer ID for MapLibre. */
  id?: string
  /** Overlay opacity 0-1. Default: 1.0. */
  opacity?: number
}

export class WindParticleLayer implements CustomLayerInterface {
  readonly id: string
  readonly type = 'custom' as const
  readonly renderingMode = '2d' as const

  private _map: MaplibreMap | null = null
  private _gl: WebGL2RenderingContext | null = null
  private _opacity: number

  // ── State update pass ───────────────────────────────────────────────
  private _updateProgram: GLProgram | null = null
  private _quad: QuadGeometry | null = null
  private _stateTextures: [WebGLTexture | null, WebGLTexture | null] | null = null
  private _stateFbos: [WebGLFramebuffer | null, WebGLFramebuffer | null] | null = null
  private _stateReadIndex = 0

  // Update uniforms
  private _uUpdateStateTex: WebGLUniformLocation | null = null
  private _uUpdateDt: WebGLUniformLocation | null = null
  private _uUpdateSeed: WebGLUniformLocation | null = null
  private _uUpdateViewportBounds: WebGLUniformLocation | null = null

  // ── Particle draw pass ──────────────────────────────────────────────
  private _drawProgram: GLProgram | null = null
  private _drawVao: WebGLVertexArrayObject | null = null

  // Draw uniforms
  private _uDrawStateTex: WebGLUniformLocation | null = null
  private _uDrawMatrix: WebGLUniformLocation | null = null
  private _uDrawPointSize: WebGLUniformLocation | null = null

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

  // ── Timing ──────────────────────────────────────────────────────────
  private _lastFrameTime = 0
  private _frameCount = 0

  constructor(options: WindParticleLayerOptions = {}) {
    this.id = options.id ?? 'wind-particles'
    this._opacity = options.opacity ?? 1.0
  }

  // ── CustomLayerInterface ────────────────────────────────────────────

  onAdd(map: MaplibreMap, gl: WebGLRenderingContext | WebGL2RenderingContext): void {
    if (!(gl instanceof WebGL2RenderingContext)) {
      console.error('[WindParticleLayer] WebGL2 is required')
      return
    }

    this._map = map
    this._gl = gl

    const ext = gl.getExtension('EXT_color_buffer_float')
    if (!ext) {
      console.error('[WindParticleLayer] EXT_color_buffer_float not supported')
      return
    }

    try {
      this._initResources(gl)
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

    // ── Timing ──
    const now = performance.now() / 1000
    const dt = this._lastFrameTime > 0 ? Math.min(now - this._lastFrameTime, 0.1) : 0.016
    this._lastFrameTime = now
    this._frameCount++

    // ── Save MapLibre GL state BEFORE any GL calls ──
    // Must capture state before _resizeTrailTextures which modifies FBO bindings.
    const prevProgram = gl.getParameter(gl.CURRENT_PROGRAM) as WebGLProgram | null
    const prevFbo = gl.getParameter(gl.FRAMEBUFFER_BINDING) as WebGLFramebuffer | null
    const prevActiveTexture = gl.getParameter(gl.ACTIVE_TEXTURE) as number
    const prevViewport = gl.getParameter(gl.VIEWPORT) as Int32Array
    const prevBlend = gl.getParameter(gl.BLEND) as boolean
    const prevBlendSrc = gl.getParameter(gl.BLEND_SRC_RGB) as number
    const prevBlendDst = gl.getParameter(gl.BLEND_DST_RGB) as number
    const prevBlendSrcA = gl.getParameter(gl.BLEND_SRC_ALPHA) as number
    const prevBlendDstA = gl.getParameter(gl.BLEND_DST_ALPHA) as number

    // Save texture bindings for units 0 and 1
    const prevTexBindings: (WebGLTexture | null)[] = []
    for (let i = 0; i < 2; i++) {
      gl.activeTexture(gl.TEXTURE0 + i)
      prevTexBindings.push(gl.getParameter(gl.TEXTURE_BINDING_2D) as WebGLTexture | null)
    }

    // ── Resize trail textures if canvas size changed ──
    const canvasW = gl.drawingBufferWidth
    const canvasH = gl.drawingBufferHeight
    if (canvasW !== this._trailWidth || canvasH !== this._trailHeight) {
      this._resizeTrailTextures(gl, canvasW, canvasH)
    }

    if (!this._trailTextures || !this._trailFbos) return

    // ────────────────────────────────────────────────────────────────
    // Pass 1: State Update (ping-pong)
    // ────────────────────────────────────────────────────────────────
    const stateRead = this._stateReadIndex
    const stateWrite = 1 - stateRead

    gl.bindFramebuffer(gl.FRAMEBUFFER, this._stateFbos[stateWrite])
    gl.viewport(0, 0, STATE_SIZE, STATE_SIZE)
    gl.disable(gl.BLEND)

    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this._stateTextures[stateRead])

    gl.useProgram(this._updateProgram.program)
    gl.uniform1i(this._uUpdateStateTex, 0)
    gl.uniform1f(this._uUpdateDt, dt)
    gl.uniform1f(this._uUpdateSeed, (now * 137.0) % 1000.0)

    const bounds = this._map.getBounds()
    const minLon = (bounds.getWest() + 180) / 360
    const maxLon = (bounds.getEast() + 180) / 360
    const minLat = this._latToMercatorY(bounds.getNorth())
    const maxLat = this._latToMercatorY(bounds.getSouth())
    gl.uniform4f(this._uUpdateViewportBounds, minLon, minLat, maxLon, maxLat)

    gl.bindVertexArray(this._quad.vao)
    gl.drawArrays(gl.TRIANGLES, 0, this._quad.vertexCount)
    gl.bindVertexArray(null)

    this._stateReadIndex = stateWrite

    // Ensure state texture write is complete before Pass 2b reads it.
    // Required on some mobile WebGL2 implementations (Safari/Metal).
    gl.flush()

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

    // 2b: Draw particles from state texture
    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this._stateTextures[this._stateReadIndex])

    gl.useProgram(this._drawProgram.program)
    gl.uniform1i(this._uDrawStateTex, 0)
    gl.uniformMatrix4fv(this._uDrawMatrix, false, options.modelViewProjectionMatrix)

    // Point size: scale with zoom so particles stay visually proportional
    const zoom = this._map.getZoom()
    const pointSize = Math.max(1.0, Math.min(4.0, zoom * 0.4))
    gl.uniform1f(this._uDrawPointSize, pointSize)

    gl.bindVertexArray(this._drawVao)
    gl.drawArrays(gl.POINTS, 0, PARTICLE_COUNT)
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
    for (let i = 0; i < 2; i++) {
      gl.activeTexture(gl.TEXTURE0 + i)
      gl.bindTexture(gl.TEXTURE_2D, prevTexBindings[i])
    }
    gl.activeTexture(prevActiveTexture)
    if (prevBlend) {
      gl.enable(gl.BLEND)
      gl.blendFuncSeparate(prevBlendSrc, prevBlendDst, prevBlendSrcA, prevBlendDstA)
    } else {
      gl.disable(gl.BLEND)
    }
    gl.useProgram(prevProgram)

    // Request next frame for continuous animation
    this._map.triggerRepaint()
  }

  onRemove(
    _map: MaplibreMap,
    _gl: WebGLRenderingContext | WebGL2RenderingContext,
  ): void {
    this._cleanup()
  }

  // ── Public API ──────────────────────────────────────────────────────

  /** Get the current "read" state texture. */
  get stateTexture(): WebGLTexture | null {
    return this._stateTextures?.[this._stateReadIndex] ?? null
  }

  /** State texture dimensions. */
  get stateSize(): number {
    return STATE_SIZE
  }

  /** Total particle count. */
  get particleCount(): number {
    return PARTICLE_COUNT
  }

  /** Update overlay opacity at runtime. */
  setOpacity(opacity: number): void {
    this._opacity = Math.max(0, Math.min(1, opacity))
    this._map?.triggerRepaint()
  }

  // ── Private ─────────────────────────────────────────────────────────

  /** Convert latitude to web mercator Y in [0,1]. */
  private _latToMercatorY(lat: number): number {
    const sinLat = Math.sin((lat * Math.PI) / 180)
    const y = 0.5 - (Math.log((1 + sinLat) / (1 - sinLat)) / (4 * Math.PI))
    return Math.max(0, Math.min(1, y))
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
    this._uUpdateDt = gl.getUniformLocation(up, 'u_dt')
    this._uUpdateSeed = gl.getUniformLocation(up, 'u_seed')
    this._uUpdateViewportBounds = gl.getUniformLocation(up, 'u_viewportBounds')

    const dp = this._drawProgram.program
    this._uDrawStateTex = gl.getUniformLocation(dp, 'u_stateTex')
    this._uDrawMatrix = gl.getUniformLocation(dp, 'u_matrix')
    this._uDrawPointSize = gl.getUniformLocation(dp, 'u_pointSize')

    const cp = this._compositeProgram.program
    this._uCompositeTexture = gl.getUniformLocation(cp, 'u_texture')
    this._uCompositeOpacity = gl.getUniformLocation(cp, 'u_opacity')

    // ── State textures (RGBA32F, 256×256) ──
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
  }

  /** Create an RGBA32F texture at STATE_SIZE × STATE_SIZE. */
  private _createStateTexture(gl: WebGL2RenderingContext): WebGLTexture {
    const tex = gl.createTexture()
    if (!tex) throw new Error('Failed to create state texture')

    gl.bindTexture(gl.TEXTURE_2D, tex)
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA32F, STATE_SIZE, STATE_SIZE, 0, gl.RGBA, gl.FLOAT, null)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
    gl.bindTexture(gl.TEXTURE_2D, null)
    return tex
  }

  /** Fill state texture with random particle positions. */
  private _initializeParticles(gl: WebGL2RenderingContext, tex: WebGLTexture): void {
    const data = new Float32Array(PARTICLE_COUNT * 4)
    for (let i = 0; i < PARTICLE_COUNT; i++) {
      const offset = i * 4
      data[offset + 0] = Math.random()  // lon [0,1]
      data[offset + 1] = Math.random()  // lat [0,1]
      data[offset + 2] = Math.random()  // age [0,1] — stagger spawns
      data[offset + 3] = 1.0            // reserved
    }
    gl.bindTexture(gl.TEXTURE_2D, tex)
    gl.texSubImage2D(gl.TEXTURE_2D, 0, 0, 0, STATE_SIZE, STATE_SIZE, gl.RGBA, gl.FLOAT, data)
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

  /** Free all GL resources. */
  private _cleanup(): void {
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
    this._uUpdateDt = null
    this._uUpdateSeed = null
    this._uUpdateViewportBounds = null
    this._uDrawStateTex = null
    this._uDrawMatrix = null
    this._uDrawPointSize = null
    this._uCompositeTexture = null
    this._uCompositeOpacity = null
    this._gl = null
    this._map = null
  }
}

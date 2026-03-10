/**
 * WebGL2 shader compilation and buffer helpers.
 *
 * Provides error-checked shader compilation with line-numbered diagnostics,
 * program linking, and fullscreen quad geometry for fragment-shader rendering.
 */

/** Compiled and linked WebGL program with its shader pair. */
export interface GLProgram {
  program: WebGLProgram
  vertexShader: WebGLShader
  fragmentShader: WebGLShader
}

/**
 * Compile a single shader with line-numbered error reporting.
 *
 * @throws Error with annotated GLSL source on compilation failure.
 */
function compileShader(
  gl: WebGL2RenderingContext,
  type: GLenum,
  source: string,
): WebGLShader {
  const shader = gl.createShader(type)
  if (!shader) {
    throw new Error('Failed to create shader object')
  }

  gl.shaderSource(shader, source)
  gl.compileShader(shader)

  if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
    const log = gl.getShaderInfoLog(shader) ?? 'Unknown error'
    gl.deleteShader(shader)

    // Annotate source with line numbers for debugging
    const annotated = source
      .split('\n')
      .map((line, i) => `${String(i + 1).padStart(3)}: ${line}`)
      .join('\n')

    const typeName = type === gl.VERTEX_SHADER ? 'vertex' : 'fragment'
    throw new Error(
      `${typeName} shader compilation failed:\n${log}\n\nSource:\n${annotated}`,
    )
  }

  return shader
}

/**
 * Compile vertex + fragment shaders and link into a program.
 *
 * @throws Error on compilation or link failure.
 */
export function createProgram(
  gl: WebGL2RenderingContext,
  vertexSource: string,
  fragmentSource: string,
): GLProgram {
  const vertexShader = compileShader(gl, gl.VERTEX_SHADER, vertexSource)

  let fragmentShader: WebGLShader
  try {
    fragmentShader = compileShader(gl, gl.FRAGMENT_SHADER, fragmentSource)
  } catch (e) {
    gl.deleteShader(vertexShader)
    throw e
  }

  const program = gl.createProgram()
  if (!program) {
    gl.deleteShader(vertexShader)
    gl.deleteShader(fragmentShader)
    throw new Error('Failed to create program object')
  }

  gl.attachShader(program, vertexShader)
  gl.attachShader(program, fragmentShader)
  gl.linkProgram(program)

  if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
    const log = gl.getProgramInfoLog(program) ?? 'Unknown error'
    gl.deleteProgram(program)
    gl.deleteShader(vertexShader)
    gl.deleteShader(fragmentShader)
    throw new Error(`Shader program link failed:\n${log}`)
  }

  return { program, vertexShader, fragmentShader }
}

/**
 * Delete a compiled program and its shaders.
 */
export function deleteProgram(
  gl: WebGL2RenderingContext,
  glProgram: GLProgram,
): void {
  gl.deleteProgram(glProgram.program)
  gl.deleteShader(glProgram.vertexShader)
  gl.deleteShader(glProgram.fragmentShader)
}

/** Vertex data for a fullscreen quad: two triangles covering clip space [-1,1]. */
const FULLSCREEN_QUAD_VERTICES = new Float32Array([
  // Triangle 1
  -1, -1,
   1, -1,
  -1,  1,
  // Triangle 2
  -1,  1,
   1, -1,
   1,  1,
])

/** Matching UV coordinates for the fullscreen quad: [0,1] × [0,1]. */
const FULLSCREEN_QUAD_UVS = new Float32Array([
  0, 0,
  1, 0,
  0, 1,
  0, 1,
  1, 0,
  1, 1,
])

/** Buffers and VAO for a fullscreen quad. */
export interface QuadGeometry {
  vao: WebGLVertexArrayObject
  positionBuffer: WebGLBuffer
  uvBuffer: WebGLBuffer
  vertexCount: number
}

/**
 * Create a fullscreen quad (2 triangles) with position and UV attributes.
 *
 * Binds to attribute locations:
 *   0 = a_position (vec2)
 *   1 = a_uv (vec2)
 */
export function createFullscreenQuad(gl: WebGL2RenderingContext): QuadGeometry {
  const vao = gl.createVertexArray()
  if (!vao) throw new Error('Failed to create VAO')

  const positionBuffer = gl.createBuffer()
  const uvBuffer = gl.createBuffer()
  if (!positionBuffer || !uvBuffer) {
    throw new Error('Failed to create vertex buffers')
  }

  gl.bindVertexArray(vao)

  // Position attribute (location 0)
  gl.bindBuffer(gl.ARRAY_BUFFER, positionBuffer)
  gl.bufferData(gl.ARRAY_BUFFER, FULLSCREEN_QUAD_VERTICES, gl.STATIC_DRAW)
  gl.enableVertexAttribArray(0)
  gl.vertexAttribPointer(0, 2, gl.FLOAT, false, 0, 0)

  // UV attribute (location 1)
  gl.bindBuffer(gl.ARRAY_BUFFER, uvBuffer)
  gl.bufferData(gl.ARRAY_BUFFER, FULLSCREEN_QUAD_UVS, gl.STATIC_DRAW)
  gl.enableVertexAttribArray(1)
  gl.vertexAttribPointer(1, 2, gl.FLOAT, false, 0, 0)

  gl.bindVertexArray(null)

  return {
    vao,
    positionBuffer,
    uvBuffer,
    vertexCount: 6,
  }
}

/**
 * Delete quad geometry and free GL resources.
 */
export function deleteQuadGeometry(
  gl: WebGL2RenderingContext,
  quad: QuadGeometry,
): void {
  gl.deleteVertexArray(quad.vao)
  gl.deleteBuffer(quad.positionBuffer)
  gl.deleteBuffer(quad.uvBuffer)
}

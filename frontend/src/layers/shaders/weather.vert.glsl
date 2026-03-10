#version 300 es
precision highp float;

// Quad vertex attributes (from createFullscreenQuad)
layout(location = 0) in vec2 a_position; // clip space [-1, 1] — unused in tiled mode
layout(location = 1) in vec2 a_uv;       // [0, 1] for texture sampling + tile positioning

// Map projection: transforms mercator [0,1] coordinates to clip space.
// Provided by MapLibre's CustomRenderMethodInput.modelViewProjectionMatrix.
uniform mat4 u_matrix;

// Tile positioning in mercator space.
// offset = (tileX / 2^z, tileY / 2^z)
// scale  = (1 / 2^z, 1 / 2^z)
uniform vec2 u_tileOffset;
uniform vec2 u_tileScale;

out vec2 v_uv;

void main() {
    v_uv = a_uv;
    // Map UV [0,1] to this tile's mercator footprint, then project to clip space
    vec2 mercator = u_tileOffset + a_uv * u_tileScale;
    gl_Position = u_matrix * vec4(mercator, 0.0, 1.0);
}

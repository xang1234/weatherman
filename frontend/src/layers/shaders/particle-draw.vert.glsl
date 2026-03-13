#version 300 es
precision highp float;

// Particle state texture (RGBA32F): R=lon, G=lat, B=age, A=reserved
uniform sampler2D u_stateTex;
uniform mat4 u_matrix;     // MapLibre model-view-projection
uniform float u_pointSize;

out float v_age;

void main() {
    // Convert gl_VertexID to state texture coordinate.
    // Use textureSize() to get the actual state texture width (may be
    // 128, 256, or 512 depending on GPU tier detection).
    int stateSize = textureSize(u_stateTex, 0).x;
    int x = gl_VertexID % stateSize;
    int y = gl_VertexID / stateSize;

    vec4 state = texelFetch(u_stateTex, ivec2(x, y), 0);
    v_age = state.b;

    // Project mercator [0,1] to clip space via MapLibre matrix
    gl_Position = u_matrix * vec4(state.r, state.g, 0.0, 1.0);
    gl_PointSize = u_pointSize;
}

#version 300 es
precision highp float;

// Compile-time constant — avoids uniform int driver bugs on Safari/Metal
#define STATE_SIZE 256

// Particle state texture (RGBA32F): R=lon, G=lat, B=age, A=reserved
uniform sampler2D u_stateTex;
uniform mat4 u_matrix;     // MapLibre model-view-projection
uniform float u_pointSize;

out float v_age;

void main() {
    // Convert gl_VertexID to state texture coordinate
    int x = gl_VertexID % STATE_SIZE;
    int y = gl_VertexID / STATE_SIZE;

    vec4 state = texelFetch(u_stateTex, ivec2(x, y), 0);
    v_age = state.b;

    // Project mercator [0,1] to clip space via MapLibre matrix
    gl_Position = u_matrix * vec4(state.r, state.g, 0.0, 1.0);
    gl_PointSize = u_pointSize;
}

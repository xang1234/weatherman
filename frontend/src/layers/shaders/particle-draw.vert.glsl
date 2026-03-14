#version 300 es
precision highp float;

// Particle state texture (RGBA32F): R=lon, G=lat, B=age, A=speed
uniform sampler2D u_stateTex;
uniform mat4 u_matrix;     // MapLibre model-view-projection
uniform float u_pointSize;
uniform float u_speedMax;  // Maximum wind speed for normalization

out float v_age;
out float v_speed;

void main() {
    // Convert gl_VertexID to state texture coordinate.
    int stateSize = textureSize(u_stateTex, 0).x;
    int x = gl_VertexID % stateSize;
    int y = gl_VertexID / stateSize;

    vec4 state = texelFetch(u_stateTex, ivec2(x, y), 0);
    v_age = state.b;
    v_speed = state.a / max(u_speedMax, 1.0); // normalize to [0,1]

    // Project mercator [0,1] to clip space via worldSize-scaled matrix
    gl_Position = u_matrix * vec4(state.r, state.g, 0.0, 1.0);
    gl_PointSize = u_pointSize;
}

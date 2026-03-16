#version 300 es
precision highp float;

// Particle state texture (RGBA32F): R=lon, G=lat, B=age, A=packed(height,direction)
uniform sampler2D u_stateTex;
uniform mat4 u_matrix;     // MapLibre model-view-projection
uniform float u_pointSize;
uniform float u_speedMax;  // Maximum wave height for normalization

out float v_age;
out float v_speed;
out float v_direction; // propagation direction in radians

void main() {
    // Convert gl_VertexID to state texture coordinate.
    int stateSize = textureSize(u_stateTex, 0).x;
    int x = gl_VertexID % stateSize;
    int y = gl_VertexID / stateSize;

    vec4 state = texelFetch(u_stateTex, ivec2(x, y), 0);
    v_age = state.b;

    // Unpack A channel: integer part = height*100, fractional part = direction/360
    float packed = state.a;
    float waveHeight = floor(packed) / 100.0;
    float dirDeg = fract(packed) * 360.0;

    v_speed = waveHeight / max(u_speedMax, 1.0); // normalize to [0,1]
    v_direction = radians(dirDeg);

    // Project mercator [0,1] to clip space via worldSize-scaled matrix
    gl_Position = u_matrix * vec4(state.r, state.g, 0.0, 1.0);
    gl_PointSize = u_pointSize;
}

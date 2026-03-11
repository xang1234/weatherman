#version 300 es
precision highp float;

// Fullscreen quad attributes (from createFullscreenQuad)
layout(location = 0) in vec2 a_position; // clip space [-1, 1]
layout(location = 1) in vec2 a_uv;       // [0, 1] for state texture sampling

out vec2 v_uv;

void main() {
    v_uv = a_uv;
    gl_Position = vec4(a_position, 0.0, 1.0);
}

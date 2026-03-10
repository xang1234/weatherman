#version 300 es
precision highp float;

// Fullscreen quad vertex positions in clip space [-1, 1]
layout(location = 0) in vec2 a_position;
// UV coordinates [0, 1] for texture sampling
layout(location = 1) in vec2 a_uv;

out vec2 v_uv;

void main() {
    v_uv = a_uv;
    gl_Position = vec4(a_position, 0.0, 1.0);
}

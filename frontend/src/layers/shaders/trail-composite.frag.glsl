#version 300 es
precision highp float;

uniform sampler2D u_texture;
uniform float u_opacity;

in vec2 v_uv;
out vec4 fragColor;

void main() {
    vec4 color = texture(u_texture, v_uv);
    // Multiply all channels (premultiplied alpha space)
    fragColor = color * u_opacity;
}

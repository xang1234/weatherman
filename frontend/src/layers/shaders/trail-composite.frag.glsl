#version 300 es
precision highp float;

uniform sampler2D u_texture;
uniform float u_opacity;
// Subtracted after the fade multiply. Set to 1/255 in the trail-fade pass so
// RGBA8 quantization can't stall the decay (v*fade rounds back to v for small
// v, leaving permanent haze). Defaults to 0.0 for the map-composite pass.
uniform float u_fadeEpsilon;

in vec2 v_uv;
out vec4 fragColor;

void main() {
    vec4 color = texture(u_texture, v_uv);
    // Multiply all channels (premultiplied alpha space)
    fragColor = max(color * u_opacity - u_fadeEpsilon, 0.0);
}

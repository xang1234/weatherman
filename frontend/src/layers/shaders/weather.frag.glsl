#version 300 es
precision highp float;

in vec2 v_uv;

// Placeholder color for pipeline validation.
// Future: data texture sampling + colormap decode will replace this.
uniform vec4 u_color;

out vec4 fragColor;

void main() {
    // Output premultiplied alpha (MapLibre expects gl.ONE, gl.ONE_MINUS_SRC_ALPHA)
    fragColor = vec4(u_color.rgb * u_color.a, u_color.a);
}

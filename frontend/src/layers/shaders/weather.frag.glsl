#version 300 es
precision highp float;

in vec2 v_uv;

// Data tile: float values encoded as 16-bit uint in R (low) + G (high),
// B channel = nodata flag (1.0 = nodata), A = 1.0 always.
uniform sampler2D u_dataTile;

// Color ramp: 256x1 RGBA texture for value-to-color mapping.
uniform sampler2D u_colorRamp;

// Layer opacity (0.0 - 1.0).
uniform float u_opacity;

out vec4 fragColor;

void main() {
    vec4 texel = texture(u_dataTile, v_uv);

    // Nodata check: B channel > 0.5 means nodata (0xFF in the PNG)
    if (texel.b > 0.5) {
        discard;
    }

    // Decode 16-bit unsigned value from R (low byte) + G (high byte).
    // In the PNG: R = encoded & 0xFF, G = (encoded >> 8) & 0xFF.
    // After GL normalization to [0,1]: encoded = (R + G * 256) / 65535.
    float normalized = (texel.r * 255.0 + texel.g * 255.0 * 256.0) / 65535.0;

    // Color ramp lookup — sample the 1D texture at the normalized position.
    vec4 color = texture(u_colorRamp, vec2(normalized, 0.5));

    // Output premultiplied alpha (MapLibre expects gl.ONE, gl.ONE_MINUS_SRC_ALPHA)
    float a = color.a * u_opacity;
    fragColor = vec4(color.rgb * a, a);
}

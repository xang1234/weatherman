#version 300 es
precision highp float;

in vec2 v_uv;

// Data tile T0: float values encoded as 16-bit uint in R (low) + G (high),
// B channel = nodata flag (1.0 = nodata), A = 1.0 always.
uniform sampler2D u_dataTile;

// Data tile T1: next timestep for temporal interpolation.
// When u_temporalMix == 0.0, this texture is unused.
uniform sampler2D u_dataTileT1;

// Color ramp: 256x1 RGBA texture for value-to-color mapping.
uniform sampler2D u_colorRamp;

// Layer opacity (0.0 - 1.0).
uniform float u_opacity;

// Temporal blend factor: 0.0 = T0 only, 1.0 = T1 only.
uniform float u_temporalMix;

out vec4 fragColor;

// Decode 16-bit value and return normalized [0,1], or -1.0 for nodata.
float decodeValue(vec4 texel) {
    if (texel.b > 0.5) return -1.0; // nodata
    return (texel.r * 255.0 + texel.g * 255.0 * 256.0) / 65535.0;
}

// Manual bilinear interpolation: sample 4 texels with GL_NEAREST,
// decode each to float, then interpolate the decoded values.
// GPU hardware bilinear (GL_LINEAR) would blend raw encoded bytes,
// producing incorrect values for our 16-bit encoding scheme.
float sampleBilinear(sampler2D tex, vec2 uv) {
    vec2 size = vec2(textureSize(tex, 0));
    vec2 texelCoord = uv * size - 0.5;
    // Clamp so the "right/bottom" neighbor never exceeds the last texel center.
    // Without this, base + step overshoots at uv=1.0 and CLAMP_TO_EDGE
    // collapses all 4 samples to the corner texel.
    texelCoord = clamp(texelCoord, vec2(0.0), size - 1.001);
    vec2 f = fract(texelCoord);
    vec2 base = (floor(texelCoord) + 0.5) / size;
    vec2 step = 1.0 / size;

    // Sample 4 neighboring texels
    float tl = decodeValue(texture(tex, base));
    float tr = decodeValue(texture(tex, base + vec2(step.x, 0.0)));
    float bl = decodeValue(texture(tex, base + vec2(0.0, step.y)));
    float br = decodeValue(texture(tex, base + step));

    // Count valid (non-nodata) samples and accumulate weighted values.
    // If any neighbor is nodata, exclude it and redistribute weight
    // to avoid artifacts at data boundaries.
    float weights[4] = float[4](
        (1.0 - f.x) * (1.0 - f.y),  // tl
        f.x * (1.0 - f.y),            // tr
        (1.0 - f.x) * f.y,            // bl
        f.x * f.y                      // br
    );
    float vals[4] = float[4](tl, tr, bl, br);

    float totalWeight = 0.0;
    float result = 0.0;
    for (int i = 0; i < 4; i++) {
        if (vals[i] >= 0.0) {
            result += vals[i] * weights[i];
            totalWeight += weights[i];
        }
    }

    // All 4 neighbors are nodata — signal nodata upstream
    if (totalWeight == 0.0) return -1.0;

    return result / totalWeight;
}

void main() {
    float v0 = sampleBilinear(u_dataTile, v_uv);

    float normalized;
    if (u_temporalMix > 0.0) {
        float v1 = sampleBilinear(u_dataTileT1, v_uv);
        if (v0 < 0.0 && v1 < 0.0) discard;      // both nodata
        if (v0 < 0.0) { normalized = v1; }        // T0 nodata, use T1
        else if (v1 < 0.0) { normalized = v0; }   // T1 nodata, use T0
        else { normalized = mix(v0, v1, u_temporalMix); }
    } else {
        if (v0 < 0.0) discard;
        normalized = v0;
    }

    // Color ramp lookup — sample the 1D texture at the normalized position.
    vec4 color = texture(u_colorRamp, vec2(normalized, 0.5));

    // Output premultiplied alpha (MapLibre expects gl.ONE, gl.ONE_MINUS_SRC_ALPHA)
    float a = color.a * u_opacity;
    fragColor = vec4(color.rgb * a, a);
}

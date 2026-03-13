#version 300 es
precision highp float;

in vec2 v_uv;

// Data tile T0: float values encoded as 16-bit uint in R (low) + G (high),
// B channel = nodata flag (1.0 = nodata), A = 1.0 always.
uniform sampler2D u_dataTile;

// Data tile T1: next timestep for temporal interpolation.
// When u_temporalMix == 0.0, this texture is unused.
uniform sampler2D u_dataTileT1;

// V-component tiles for vector (wind) mode.
// When u_isVector == 0, these are unused.
uniform sampler2D u_dataTileV;
uniform sampler2D u_dataTileVT1;

// Color ramp: 256x1 RGBA texture for value-to-color mapping.
uniform sampler2D u_colorRamp;

// Layer opacity (0.0 - 1.0).
uniform float u_opacity;

// Temporal blend factor: 0.0 = T0 only, 1.0 = T1 only.
uniform float u_temporalMix;

// Vector mode flag: 0 = scalar, 1 = vector (U/V components).
// In vector mode, u_dataTile holds U and u_dataTileV holds V.
// The shader reconstructs speed = sqrt(U² + V²) for color ramp lookup.
uniform int u_isVector;

// Ocean-only flag: 0 = normal, 1 = ocean-only layer (wave_height, etc.).
// When 1, sampleWithFallback tries surrounding texels if bilinear returns nodata.
uniform int u_oceanOnly;

// Value range for denormalization in vector mode.
// Decoded [0,1] values are mapped back to [u_valueMin, u_valueMax].
// Also used to normalize reconstructed speed to [0,1] for the color ramp.
uniform float u_valueMin;
uniform float u_valueMax;

// Float16 mode flag: 0 = 8-bit PNG tiles, 1 = R16F Float16 binary tiles.
// When 1, textures contain physical values directly (no decode needed).
// Nodata sentinel is -9999.0 instead of the B-channel flag.
uniform int u_isFloat16;

out vec4 fragColor;

// Nodata sentinel for Float16 tiles (-9999.0, check with margin for f16 rounding).
const float F16_NODATA_THRESH = -9000.0;

// Decode a texel based on tile format.
// PNG mode: returns normalized [0,1], or -1.0 for nodata.
// Float16 mode: returns physical value directly, or -1.0 for nodata.
float decodeValue(vec4 texel) {
    if (u_isFloat16 == 1) {
        // R16F texture: physical value in .r channel
        float val = texel.r;
        if (val < F16_NODATA_THRESH) return -1.0;
        return val;
    }
    // PNG: 16-bit uint packed in R (low) + G (high), B = nodata flag
    if (texel.b > 0.5) return -1.0;
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

// Sample with coastal-edge fallback for ocean-only layers.
// Tries bilinear first; if nodata and u_oceanOnly==1, averages up to 8
// surrounding texels at 1-texel offset to recover coastal edge values.
// For non-ocean layers, this is a zero-cost pass-through.
float sampleWithFallback(sampler2D tex, vec2 uv) {
    float val = sampleBilinear(tex, uv);
    if (val >= 0.0 || u_oceanOnly == 0) return val;

    // Try 8 neighbors at 1-texel offset
    vec2 size = vec2(textureSize(tex, 0));
    vec2 step = 1.0 / size;
    float total = 0.0;
    float count = 0.0;
    for (int dy = -1; dy <= 1; dy++) {
        for (int dx = -1; dx <= 1; dx++) {
            if (dx == 0 && dy == 0) continue;
            float nv = decodeValue(texture(tex, uv + vec2(float(dx), float(dy)) * step));
            if (nv >= 0.0) {
                total += nv;
                count += 1.0;
            }
        }
    }
    if (count == 0.0) return -1.0;
    return total / count;
}

// Interpolate a scalar value between T0 and T1 with nodata handling.
// Returns -1.0 if both are nodata.
float temporalBlend(float v0, float v1) {
    if (v0 < 0.0 && v1 < 0.0) return -1.0;
    if (v0 < 0.0) return v1;
    if (v1 < 0.0) return v0;
    return mix(v0, v1, u_temporalMix);
}

// Convert a decoded value to physical units (m/s for wind, etc.).
// In Float16 mode, values are already physical. In PNG mode, denormalize.
float toPhysical(float val) {
    if (u_isFloat16 == 1) return val;
    return val * (u_valueMax - u_valueMin) + u_valueMin;
}

// Normalize a physical value to [0,1] for color ramp lookup.
float toNormalized(float physical) {
    float range = u_valueMax - u_valueMin;
    if (range == 0.0) return 0.0;
    return clamp((physical - u_valueMin) / range, 0.0, 1.0);
}

void main() {
    float normalized;

    if (u_isVector == 1) {
        // Vector mode: sample U and V components separately,
        // interpolate in Cartesian space, reconstruct speed.
        float u0 = sampleWithFallback(u_dataTile, v_uv);
        float v0 = sampleWithFallback(u_dataTileV, v_uv);

        // Both components must be valid for a valid wind vector
        if (u0 < 0.0 || v0 < 0.0) {
            if (u_temporalMix <= 0.0) discard;
            // Try T1 before discarding
            float u1 = sampleWithFallback(u_dataTileT1, v_uv);
            float v1 = sampleWithFallback(u_dataTileVT1, v_uv);
            if (u1 < 0.0 || v1 < 0.0) discard;
            float uWind = toPhysical(u1);
            float vWind = toPhysical(v1);
            float speed = sqrt(uWind * uWind + vWind * vWind);
            normalized = clamp(speed / u_valueMax, 0.0, 1.0);
        } else if (u_temporalMix > 0.0) {
            float u1 = sampleWithFallback(u_dataTileT1, v_uv);
            float v1 = sampleWithFallback(u_dataTileVT1, v_uv);
            float uBlend = temporalBlend(u0, u1);
            float vBlend = temporalBlend(v0, v1);
            float uWind = toPhysical(uBlend);
            float vWind = toPhysical(vBlend);
            float speed = sqrt(uWind * uWind + vWind * vWind);
            normalized = clamp(speed / u_valueMax, 0.0, 1.0);
        } else {
            float uWind = toPhysical(u0);
            float vWind = toPhysical(v0);
            float speed = sqrt(uWind * uWind + vWind * vWind);
            normalized = clamp(speed / u_valueMax, 0.0, 1.0);
        }
    } else {
        // Scalar mode: single data tile per timestep
        float v0 = sampleWithFallback(u_dataTile, v_uv);

        if (u_temporalMix > 0.0) {
            float v1 = sampleWithFallback(u_dataTileT1, v_uv);
            if (v0 < 0.0 && v1 < 0.0) discard;
            if (v0 < 0.0) v0 = v1;
            else if (v1 >= 0.0) v0 = mix(v0, v1, u_temporalMix);
            // else keep v0
        } else {
            if (v0 < 0.0) discard;
        }

        // In Float16 mode, v0 is physical — normalize for color ramp.
        // In PNG mode, v0 is already normalized [0,1].
        normalized = (u_isFloat16 == 1) ? toNormalized(v0) : v0;
    }

    // Color ramp lookup — sample the 1D texture at the normalized position.
    vec4 color = texture(u_colorRamp, vec2(normalized, 0.5));

    // Output premultiplied alpha (MapLibre expects gl.ONE, gl.ONE_MINUS_SRC_ALPHA)
    float a = color.a * u_opacity;
    fragColor = vec4(color.rgb * a, a);
}

#version 300 es
precision highp float;

// Previous frame's particle state texture (RGBA32F).
// R = longitude [0,1], G = latitude [0,1], B = age [0,1], A = reserved
uniform sampler2D u_stateTex;

// Wind U/V data tiles (T0) — 16-bit encoded PNGs from TiTiler.
// R = low byte, G = high byte of uint16 value, B = nodata flag.
uniform sampler2D u_windU;
uniform sampler2D u_windV;

// Wind U/V data tiles (T1) — next timestep for temporal interpolation.
uniform sampler2D u_windUT1;
uniform sampler2D u_windVT1;

// Temporal blend factor: 0.0 = T0 only, 1.0 = T1 only.
uniform float u_temporalMix;

// Whether wind textures are bound and valid (0 = no wind data, 1 = have data).
uniform int u_hasWindData;

// Float16 mode flag: 0 = 8-bit PNG tiles, 1 = R16F Float16 binary tiles.
uniform int u_isFloat16;

// Value range for denormalization: wind encoded as [valueMin, valueMax] → [0,1].
// Typically symmetric: valueMin = -50, valueMax = 50 for m/s.
uniform float u_valueMin;
uniform float u_valueMax;

// Speed scaling factor: converts m/s displacement to mercator units per second.
// Tuned so particles move at visually appropriate speed.
uniform float u_speedScale;

// Time delta in seconds since last frame
uniform float u_dt;

// Frame counter for random seed variation
uniform float u_seed;

// Viewport bounds in [0,1] mercator space for respawning
uniform vec4 u_viewportBounds; // (minLon, minLat, maxLon, maxLat)

in vec2 v_uv;
out vec4 fragColor;

// ── Pseudo-random hash ──────────────────────────────────────────────
// Based on Wang hash — gives decorrelated values per texel per frame.

float hash(vec2 p) {
    vec3 p3 = fract(vec3(p.xyx) * 0.1031);
    p3 += dot(p3, p3.yzx + 33.33);
    return fract((p3.x + p3.y) * p3.z);
}

vec2 hash2(vec2 p) {
    return vec2(
        hash(p),
        hash(p + vec2(127.1, 311.7))
    );
}

// ── Wind field sampling ─────────────────────────────────────────────

// Nodata sentinel threshold for Float16 tiles.
const float F16_NODATA_THRESH = -9000.0;

// Decode wind tile texel. Returns normalized [0,1] (PNG) or physical m/s (Float16),
// or -1.0 for nodata.
float decodeWind(vec4 texel) {
    if (u_isFloat16 == 1) {
        float val = texel.r;
        if (val < F16_NODATA_THRESH) return -1.0;
        return val;
    }
    if (texel.b > 0.5) return -1.0;
    return (texel.r * 255.0 + texel.g * 255.0 * 256.0) / 65535.0;
}

// Sample wind vector at a mercator position. Returns wind in m/s or vec2(0) if nodata.
vec2 sampleWind(vec2 pos) {
    // Particle positions are in mercator [0,1] which maps directly to tile UV
    float u0 = decodeWind(texture(u_windU, pos));
    float v0 = decodeWind(texture(u_windV, pos));

    if (u0 < 0.0 || v0 < 0.0) return vec2(0.0);

    if (u_temporalMix > 0.0) {
        float u1 = decodeWind(texture(u_windUT1, pos));
        float v1 = decodeWind(texture(u_windVT1, pos));
        // If T1 is nodata, use T0 only
        if (u1 >= 0.0 && v1 >= 0.0) {
            u0 = mix(u0, u1, u_temporalMix);
            v0 = mix(v0, v1, u_temporalMix);
        }
    }

    // In Float16 mode, values are already physical (m/s).
    // In PNG mode, denormalize from [0,1] to [valueMin, valueMax].
    if (u_isFloat16 == 0) {
        float valueRange = u_valueMax - u_valueMin;
        u0 = u0 * valueRange + u_valueMin;
        v0 = v0 * valueRange + u_valueMin;
    }

    return vec2(u0, v0);
}

// ── Main update ─────────────────────────────────────────────────────

void main() {
    vec4 state = texture(u_stateTex, v_uv);
    float lon = state.r;
    float lat = state.g;
    float age = state.b;

    // Advance age
    // Particles live ~4 seconds: age increments by dt/4.0 per frame
    float maxLife = 4.0;
    age += u_dt / maxLife;

    vec2 seedOffset = v_uv * 256.0 + vec2(u_seed);

    if (u_hasWindData == 1) {
        // Sample wind field at current particle position
        vec2 wind = sampleWind(vec2(lon, lat));
        float speed = length(wind);

        if (speed > 0.001) {
            // Advect by wind: convert m/s to mercator displacement
            // u_speedScale converts physical velocity to mercator units/second
            lon += wind.x * u_speedScale * u_dt;
            lat -= wind.y * u_speedScale * u_dt; // Y is inverted in mercator

            // Add slight random jitter for visual richness (1% of displacement)
            vec2 rnd = hash2(seedOffset) * 2.0 - 1.0;
            lon += rnd.x * speed * u_speedScale * u_dt * 0.01;
            lat -= rnd.y * speed * u_speedScale * u_dt * 0.01;
        } else {
            // No wind at this location — gentle random drift
            vec2 rnd = hash2(seedOffset) * 2.0 - 1.0;
            lon += rnd.x * 0.0002;
            lat += rnd.y * 0.0002;
        }
    } else {
        // No wind data bound — random walk fallback
        vec2 rnd = hash2(seedOffset) * 2.0 - 1.0;
        float walkSpeed = 0.001;
        lon += rnd.x * walkSpeed;
        lat += rnd.y * walkSpeed;
    }

    // Respawn if aged out or drifted outside viewport
    bool outOfBounds = lon < u_viewportBounds.x || lon > u_viewportBounds.z ||
                       lat < u_viewportBounds.y || lat > u_viewportBounds.w;

    if (age >= 1.0 || outOfBounds) {
        // Respawn at random position within viewport
        vec2 spawnRnd = hash2(seedOffset + vec2(42.0, 17.0));
        lon = mix(u_viewportBounds.x, u_viewportBounds.z, spawnRnd.x);
        lat = mix(u_viewportBounds.y, u_viewportBounds.w, spawnRnd.y);
        age = 0.0;
    }

    fragColor = vec4(lon, lat, age, 1.0);
}

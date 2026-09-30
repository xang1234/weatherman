#version 300 es
precision highp float;

// Previous frame's particle state texture (RGBA32F).
// R = longitude [0,1], G = latitude [0,1], B = age [0,1], A = speed (m/s),
// negative when the particle must not be drawn this frame
uniform sampler2D u_stateTex;

// Wind U/V atlas textures (T0) — all visible tiles packed into one texture.
uniform sampler2D u_windU;
uniform sampler2D u_windV;

// Wind U/V atlas textures (T1) — next timestep for temporal interpolation.
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

// Atlas uniforms: define how mercator coordinates map into the packed tile atlas.
// originX/Y = integer tile coordinates of the atlas top-left corner.
// zoom = 2^z (tiles per axis at this zoom level).
// cols/rows = number of tiles packed horizontally/vertically.
uniform float u_atlasOriginX;
uniform float u_atlasOriginY;
uniform float u_atlasZoom;
uniform float u_atlasCols;
uniform float u_atlasRows;

in vec2 v_uv;

out vec4 fragColor;

// ── Nodata sentinel ────────────────────────────────────────────────
// Must be far below any valid physical wind value (-50 m/s).
const float NODATA = -99999.0;
bool isNodata(float v) { return v < -99000.0; }

// Nodata threshold for Float16 tiles (backend writes -9999.0).
const float F16_NODATA_THRESH = -9000.0;

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

// Decode wind tile texel. Returns normalized [0,1] (PNG) or physical m/s (Float16),
// or NODATA sentinel for missing data.
float decodeWind(vec4 texel) {
    if (u_isFloat16 == 1) {
        float val = texel.r;
        if (val < F16_NODATA_THRESH) return NODATA;
        return val;
    }
    if (texel.b > 0.5) return NODATA;
    return (texel.r * 255.0 + texel.g * 255.0 * 256.0) / 65535.0;
}

// Bilinear sample of an atlas texture. Atlases are GL_NEAREST (PNG tiles pack
// 16 bits across two channels, so hardware filtering would blend encoded
// bytes): fetch the 4 texels, decode, then interpolate. Data tiles stop at z5,
// so above that one texel spans many screen pixels and nearest would be blocky.
// Nodata where the nearest texel is nodata; nodata neighbours are skipped.
float sampleAtlas(sampler2D tex, vec2 uv) {
    ivec2 size = textureSize(tex, 0);
    vec2 tc = uv * vec2(size) - 0.5;
    ivec2 i0 = clamp(ivec2(floor(tc)), ivec2(0), size - 1);
    ivec2 i1 = min(i0 + 1, size - 1);
    vec2 f = clamp(tc - vec2(i0), 0.0, 1.0);

    float vals[4] = float[4](
        decodeWind(texelFetch(tex, i0, 0)),
        decodeWind(texelFetch(tex, ivec2(i1.x, i0.y), 0)),
        decodeWind(texelFetch(tex, ivec2(i0.x, i1.y), 0)),
        decodeWind(texelFetch(tex, i1, 0))
    );
    if (isNodata(vals[int(step(0.5, f.x)) + 2 * int(step(0.5, f.y))])) return NODATA;

    float weights[4] = float[4](
        (1.0 - f.x) * (1.0 - f.y),
        f.x * (1.0 - f.y),
        (1.0 - f.x) * f.y,
        f.x * f.y
    );
    float total = 0.0;
    float totalWeight = 0.0;
    for (int i = 0; i < 4; i++) {
        if (!isNodata(vals[i])) {
            total += vals[i] * weights[i];
            totalWeight += weights[i];
        }
    }
    return totalWeight > 0.0 ? total / totalWeight : NODATA;
}

// Sample wind vector at a mercator position using the tile atlas. Returns
// false where there is no data: outside the atlas, tile not loaded yet, or
// a nodata texel.
// Converts mercator [0,1] → atlas UV by computing which tile the position
// falls in, then mapping to the corresponding region within the packed atlas.
// No Y-flip: XYZ tiles (y=0 at north) + GL upload (row 0 at bottom) means
// sampling at mercator Y=0 (north) → GL V=0 → bottom row = north = correct.
bool sampleWind(vec2 pos, out vec2 wind) {
    wind = vec2(0.0);

    // Convert mercator [0,1] position to atlas-local tile position.
    // tilePos.x/y are in tile units relative to the atlas origin.
    vec2 tilePos = pos * u_atlasZoom - vec2(u_atlasOriginX, u_atlasOriginY);

    // Out-of-atlas guard: particles outside visible tile range get no wind
    if (tilePos.x < 0.0 || tilePos.x >= u_atlasCols ||
        tilePos.y < 0.0 || tilePos.y >= u_atlasRows)
        return false;

    // Atlas UV: fractional position within the packed atlas texture
    vec2 atlasUV = tilePos / vec2(u_atlasCols, u_atlasRows);

    float u0 = sampleAtlas(u_windU, atlasUV);
    float v0 = sampleAtlas(u_windV, atlasUV);

    if (isNodata(u0) || isNodata(v0)) return false;

    if (u_temporalMix > 0.0) {
        float u1 = sampleAtlas(u_windUT1, atlasUV);
        float v1 = sampleAtlas(u_windVT1, atlasUV);
        // If T1 is nodata, use T0 only
        if (!isNodata(u1) && !isNodata(v1)) {
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

    wind = vec2(u0, v0);
    return true;
}

// ── Main update ─────────────────────────────────────────────────────

void main() {
    vec4 state = texture(u_stateTex, v_uv);
    float lon = state.r;
    float lat = state.g;
    float age = state.b;

    // Each particle has its own lifetime (4–8 s): long enough to trace a
    // streamline, and varied so the field never ages in step.
    float maxLife = mix(4.0, 8.0, hash(v_uv * 311.7));
    age += u_dt / maxLife;

    vec2 seedOffset = v_uv * 256.0 + vec2(u_seed);

    vec2 wind = vec2(0.0);
    bool hasWind = u_hasWindData == 1 && sampleWind(vec2(lon, lat), wind);
    float speed = length(wind);

    if (hasWind) {
        // Advect by wind: u_speedScale converts m/s to mercator units/second
        lon += wind.x * u_speedScale * u_dt;
        lat -= wind.y * u_speedScale * u_dt; // Y is inverted in mercator

        // Add slight random jitter for visual richness (1% of displacement)
        vec2 rnd = hash2(seedOffset) * 2.0 - 1.0;
        lon += rnd.x * speed * u_speedScale * u_dt * 0.01;
        lat -= rnd.y * speed * u_speedScale * u_dt * 0.01;
    }

    bool outOfBounds = lon < u_viewportBounds.x || lon > u_viewportBounds.z ||
                       lat < u_viewportBounds.y || lat > u_viewportBounds.w;

    // Respawn at a random position in the viewport when aged out, carried off
    // screen, or sitting where there is no wind data (tile still loading).
    if (!hasWind || age >= 1.0 || outOfBounds) {
        vec2 spawnRnd = hash2(seedOffset + vec2(42.0, 17.0));
        lon = mix(u_viewportBounds.x, u_viewportBounds.z, spawnRnd.x);
        lat = mix(u_viewportBounds.y, u_viewportBounds.w, spawnRnd.y);
        // A particle that lived out its life restarts at 0 and fades in. One
        // displaced by a pan, a zoom or missing data gets a random age: they
        // are respawned in bulk, and a shared age would make the whole field
        // fade out and restart together every lifetime.
        age = age >= 1.0 ? 0.0 : hash(seedOffset + vec2(7.3, 91.1));
        // Negative speed = not drawn this frame (see particle-draw.frag.glsl);
        // the next update samples the wind at the new position.
        speed = -1.0;
    }

    fragColor = vec4(lon, lat, age, speed);
}

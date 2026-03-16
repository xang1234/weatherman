#version 300 es
precision highp float;

// Previous frame's particle state texture (RGBA32F).
// R = longitude [0,1], G = latitude [0,1], B = age [0,1],
// A = packed(height, propagation direction): floor(h*100) + dir/360
uniform sampler2D u_stateTex;

// Wave height atlas textures (T0/T1) — all visible tiles packed into one texture.
uniform sampler2D u_waveHeight;
uniform sampler2D u_waveHeightT1;

// Wave direction atlas textures (T0/T1) — wave propagation direction in degrees.
uniform sampler2D u_waveDirection;
uniform sampler2D u_waveDirectionT1;

// Temporal blend factor: 0.0 = T0 only, 1.0 = T1 only.
uniform float u_temporalMix;

// Whether wave textures are bound and valid (0 = no data, 1 = have data).
uniform int u_hasWaveData;

// Float16 mode flag: 0 = 8-bit PNG tiles, 1 = R16F Float16 binary tiles.
uniform int u_isFloat16;

// Value ranges for denormalization (PNG mode only).
// Height: typically [0, 15] meters.
uniform float u_valueMinHeight;
uniform float u_valueMaxHeight;
// Direction: typically [0, 360] degrees.
uniform float u_valueMinDir;
uniform float u_valueMaxDir;

// Speed scaling factor: converts m/s displacement to mercator units per second.
uniform float u_speedScale;

// Time delta in seconds since last frame
uniform float u_dt;

// Frame counter for random seed variation
uniform float u_seed;

// Viewport bounds in [0,1] mercator space for respawning
uniform vec4 u_viewportBounds; // (minLon, minLat, maxLon, maxLat)

// Atlas uniforms: define how mercator coordinates map into the packed tile atlas.
uniform float u_atlasOriginX;
uniform float u_atlasOriginY;
uniform float u_atlasZoom;
uniform float u_atlasCols;
uniform float u_atlasRows;

in vec2 v_uv;

out vec4 fragColor;

// ── Nodata sentinel ────────────────────────────────────────────────
const float NODATA = -99999.0;
bool isNodata(float v) { return v < -99000.0; }

// Nodata threshold for Float16 tiles (backend writes -9999.0).
const float F16_NODATA_THRESH = -9000.0;

// ── Pseudo-random hash ──────────────────────────────────────────────

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

// ── Tile texel decode ───────────────────────────────────────────────

// Decode a scalar tile texel. Returns normalized [0,1] (PNG) or physical value (Float16),
// or NODATA sentinel for missing data.
float decodeTile(vec4 texel) {
    if (u_isFloat16 == 1) {
        float val = texel.r;
        if (val < F16_NODATA_THRESH) return NODATA;
        return val;
    }
    if (texel.b > 0.5) return NODATA;
    return (texel.r * 255.0 + texel.g * 255.0 * 256.0) / 65535.0;
}

// ── Circular interpolation for direction ────────────────────────────
// Naive mix(350, 10, 0.5) gives 180 (wrong). This handles the wraparound
// correctly by taking the shortest arc between two angles.

float circularMix(float a, float b, float t) {
    float diff = mod(b - a + 540.0, 360.0) - 180.0;
    return mod(a + diff * t + 360.0, 360.0);
}

// ── Wave field sampling ─────────────────────────────────────────────

// Sample wave vector at a mercator position using the tile atlas.
// Returns vec4(u, v, height, propagationDirDeg) where u/v are Cartesian
// velocity components and propagationDirDeg is the direction waves travel TO.
vec4 sampleWave(vec2 pos) {
    // Convert mercator [0,1] position to atlas-local tile position.
    vec2 tilePos = pos * u_atlasZoom - vec2(u_atlasOriginX, u_atlasOriginY);

    // Out-of-atlas guard: particles outside visible tile range get no data
    if (tilePos.x < 0.0 || tilePos.x >= u_atlasCols ||
        tilePos.y < 0.0 || tilePos.y >= u_atlasRows)
        return vec4(0.0);

    // Atlas UV: fractional position within the packed atlas texture
    vec2 atlasUV = tilePos / vec2(u_atlasCols, u_atlasRows);

    float h0 = decodeTile(texture(u_waveHeight, atlasUV));
    float d0 = decodeTile(texture(u_waveDirection, atlasUV));

    if (isNodata(h0) || isNodata(d0)) return vec4(0.0);

    // Denormalize PNG values to physical units
    if (u_isFloat16 == 0) {
        h0 = h0 * (u_valueMaxHeight - u_valueMinHeight) + u_valueMinHeight;
        d0 = d0 * (u_valueMaxDir - u_valueMinDir) + u_valueMinDir;
    }

    if (u_temporalMix > 0.0) {
        float h1 = decodeTile(texture(u_waveHeightT1, atlasUV));
        float d1 = decodeTile(texture(u_waveDirectionT1, atlasUV));
        // If T1 is nodata, use T0 only
        if (!isNodata(h1) && !isNodata(d1)) {
            if (u_isFloat16 == 0) {
                h1 = h1 * (u_valueMaxHeight - u_valueMinHeight) + u_valueMinHeight;
                d1 = d1 * (u_valueMaxDir - u_valueMinDir) + u_valueMinDir;
            }
            h0 = mix(h0, h1, u_temporalMix);
            d0 = circularMix(d0, d1, u_temporalMix);
        }
    }

    // Convert polar (height, direction) to Cartesian (u, v).
    // GFS-Wave dirpw_sfc: direction FROM which waves propagate.
    // 0° = from north, 90° = from east (meteorological convention).
    // Negate to get propagation direction (where waves travel TO):
    // "from north" (0°) → particles move south.
    float dir_rad = radians(d0);
    float u = -h0 * sin(dir_rad);
    float v = -h0 * cos(dir_rad);

    // Propagation direction = "from" direction + 180°
    float propDirDeg = mod(d0 + 180.0, 360.0);

    return vec4(u, v, h0, propDirDeg);
}

// ── Pack height + direction into a single float ─────────────────────
// Integer part: height * 100 (0–1500 for 0–15m)
// Fractional part: direction / 360 (0.0–0.999...)
float packHeightDir(float height, float dirDeg) {
    float h = floor(clamp(height, 0.0, 15.0) * 100.0);
    float d = mod(dirDeg, 360.0) / 360.0;
    return h + d;
}

// ── Main update ─────────────────────────────────────────────────────

void main() {
    vec4 state = texture(u_stateTex, v_uv);
    float lon = state.r;
    float lat = state.g;
    float age = state.b;

    // Advance age
    // Particles live ~8 seconds: longer than wind (4s) for the slow, sweeping
    // swell aesthetic — particles travel further before respawning
    float maxLife = 8.0;
    age += u_dt / maxLife;

    vec2 seedOffset = v_uv * 256.0 + vec2(u_seed);
    float waveHeight = 0.0;
    float waveDirDeg = 0.0;

    if (u_hasWaveData == 1) {
        // Sample wave field at current particle position
        vec4 wave = sampleWave(vec2(lon, lat));
        waveHeight = wave.z;
        waveDirDeg = wave.w;

        // Rotate wave velocity by small random angle to break convergence streaks
        float jitterAngle = (hash(seedOffset + vec2(73.7, 13.1)) - 0.5) * 0.35; // ±10°
        float ca = cos(jitterAngle);
        float sa = sin(jitterAngle);
        wave = vec4(ca * wave.x - sa * wave.y, sa * wave.x + ca * wave.y, wave.z, wave.w);

        if (waveHeight > 0.001) {
            // Advect by wave velocity: convert to mercator displacement
            lon += wave.x * u_speedScale * u_dt;
            lat -= wave.y * u_speedScale * u_dt; // Y is inverted in mercator

            // Add slight random jitter for visual richness (1% of displacement)
            vec2 rnd = hash2(seedOffset) * 2.0 - 1.0;
            lon += rnd.x * waveHeight * u_speedScale * u_dt * 0.08;
            lat -= rnd.y * waveHeight * u_speedScale * u_dt * 0.08;
        } else {
            // No wave data at this position (land or calm) — force respawn next frame.
            // Use 0.99 instead of 1.0 to avoid triggering the respawn block in the
            // same frame (which would create a per-frame respawn loop for land particles).
            age = 0.99;
        }
    } else {
        // No wave data bound — random walk fallback
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
        waveHeight = 0.0;
        waveDirDeg = 0.0;
    }

    // Pack height + propagation direction into A channel
    fragColor = vec4(lon, lat, age, packHeightDir(waveHeight, waveDirDeg));
}

#version 300 es
precision highp float;

// State texture is kept so the draw pass can reuse the wind particle plumbing.
uniform sampler2D u_stateTex;

uniform sampler2D u_waveHeight;
uniform sampler2D u_waveHeightT1;
uniform sampler2D u_wavePeriod;
uniform sampler2D u_wavePeriodT1;
uniform sampler2D u_waveDirU;
uniform sampler2D u_waveDirUT1;
uniform sampler2D u_waveDirV;
uniform sampler2D u_waveDirVT1;

uniform float u_temporalMix;
uniform int u_hasWaveData;
uniform int u_isFloat16;

uniform float u_valueMinHeight;
uniform float u_valueMaxHeight;
uniform float u_valueMinPeriod;
uniform float u_valueMaxPeriod;
uniform float u_valueMinDir;
uniform float u_valueMaxDir;

uniform float u_time;
uniform vec4 u_viewportBounds; // (minLon, minLat, maxLon, maxLat)
uniform float u_gridOriginX;
uniform float u_gridOriginY;
uniform float u_gridSpacing;
uniform float u_gridCols;
uniform float u_gridRows;
uniform float u_phaseAmplitude;

uniform float u_atlasOriginX;
uniform float u_atlasOriginY;
uniform float u_atlasZoom;
uniform float u_atlasCols;
uniform float u_atlasRows;

in vec2 v_uv;

out vec4 fragColor;

const float NODATA = -99999.0;
const float F16_NODATA_THRESH = -9000.0;

bool isNodata(float v) { return v < -99000.0; }

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

float decodeTile(vec4 texel) {
    if (u_isFloat16 == 1) {
        float val = texel.r;
        if (val < F16_NODATA_THRESH) return NODATA;
        return val;
    }
    if (texel.b > 0.5) return NODATA;
    return (texel.r * 255.0 + texel.g * 255.0 * 256.0) / 65535.0;
}

bool atlasLookup(vec2 pos, out vec2 atlasUV) {
    vec2 tilePos = pos * u_atlasZoom - vec2(u_atlasOriginX, u_atlasOriginY);
    if (
        tilePos.x < 0.0 || tilePos.x >= u_atlasCols ||
        tilePos.y < 0.0 || tilePos.y >= u_atlasRows
    ) {
        atlasUV = vec2(0.0);
        return false;
    }

    atlasUV = tilePos / vec2(u_atlasCols, u_atlasRows);
    return true;
}

float decodePhysical(
    sampler2D tex,
    vec2 atlasUV,
    float valueMin,
    float valueMax
) {
    float value = decodeTile(texture(tex, atlasUV));
    if (isNodata(value)) return NODATA;
    if (u_isFloat16 == 0) {
        value = value * (valueMax - valueMin) + valueMin;
    }
    return value;
}

float sampleScalar(
    sampler2D tex0,
    sampler2D tex1,
    vec2 pos,
    float valueMin,
    float valueMax
) {
    vec2 atlasUV;
    if (!atlasLookup(pos, atlasUV)) return NODATA;

    float value0 = decodePhysical(tex0, atlasUV, valueMin, valueMax);
    if (isNodata(value0)) return NODATA;

    if (u_temporalMix > 0.0) {
        float value1 = decodePhysical(tex1, atlasUV, valueMin, valueMax);
        if (!isNodata(value1)) {
            value0 = mix(value0, value1, u_temporalMix);
        }
    }

    return value0;
}

vec2 sampleDirection(vec2 pos) {
    vec2 atlasUV;
    if (!atlasLookup(pos, atlasUV)) return vec2(0.0);

    float u0 = decodePhysical(u_waveDirU, atlasUV, u_valueMinDir, u_valueMaxDir);
    float v0 = decodePhysical(u_waveDirV, atlasUV, u_valueMinDir, u_valueMaxDir);
    if (isNodata(u0) || isNodata(v0)) return vec2(0.0);

    if (u_temporalMix > 0.0) {
        float u1 = decodePhysical(u_waveDirUT1, atlasUV, u_valueMinDir, u_valueMaxDir);
        float v1 = decodePhysical(u_waveDirVT1, atlasUV, u_valueMinDir, u_valueMaxDir);
        if (!isNodata(u1) && !isNodata(v1)) {
            u0 = mix(u0, u1, u_temporalMix);
            v0 = mix(v0, v1, u_temporalMix);
        }
    }

    vec2 dir = vec2(u0, v0);
    float magnitude = length(dir);
    if (magnitude < 1e-6) return vec2(0.0);
    return dir / magnitude;
}

float packHeightDir(float height, float dirDeg) {
    float h = floor(clamp(height, 0.0, 15.0) * 100.0);
    float d = mod(dirDeg, 360.0) / 360.0;
    return h + d;
}

void main() {
    int stateSize = textureSize(u_stateTex, 0).x;
    ivec2 texel = ivec2(gl_FragCoord.xy);
    int particleId = texel.y * stateSize + texel.x;
    int activeCount = int(u_gridCols * u_gridRows + 0.5);

    if (
        u_hasWaveData != 1 ||
        particleId < 0 ||
        particleId >= activeCount ||
        u_gridSpacing <= 0.0
    ) {
        fragColor = vec4(-1.0, -1.0, 0.5, 0.0);
        return;
    }

    float particle = float(particleId);
    float col = mod(particle, u_gridCols);
    float row = floor(particle / u_gridCols);
    vec2 cellId = vec2(
        u_gridOriginX / u_gridSpacing + col,
        u_gridOriginY / u_gridSpacing + row
    );
    vec2 jitter = hash2(cellId) - 0.5;

    float anchorLon = u_gridOriginX + (col + 0.5 + jitter.x * 0.6) * u_gridSpacing;
    float anchorLat = u_gridOriginY + (row + 0.5 + jitter.y * 0.6) * u_gridSpacing;
    vec2 anchorPos = vec2(anchorLon, anchorLat);

    float waveHeight = sampleScalar(
        u_waveHeight,
        u_waveHeightT1,
        anchorPos,
        u_valueMinHeight,
        u_valueMaxHeight
    );
    float wavePeriod = sampleScalar(
        u_wavePeriod,
        u_wavePeriodT1,
        anchorPos,
        u_valueMinPeriod,
        u_valueMaxPeriod
    );
    vec2 direction = sampleDirection(anchorPos);

    if (
        isNodata(waveHeight) ||
        isNodata(wavePeriod) ||
        waveHeight <= 0.05 ||
        length(direction) < 1e-6
    ) {
        fragColor = vec4(anchorLon, anchorLat, 0.5, 0.0);
        return;
    }

    float periodNorm = clamp(
        wavePeriod / max(u_valueMaxPeriod, 1.0),
        0.0,
        1.0
    );

    // Faster phase rates with per-particle variation
    float phaseRate = mix(0.12, 0.35, periodNorm);
    float rateJitter = mix(0.85, 1.15, hash(cellId + vec2(53.7, 11.9)));
    phaseRate *= rateJitter;

    float phaseSeed = hash(cellId + vec2(19.7, 53.1));
    float phase = fract(u_time * phaseRate + phaseSeed);

    // Forward travel with Hermite ease-in-out (no symmetric back-and-forth)
    float eased = phase * phase * (3.0 - 2.0 * phase);
    float ampJitter = mix(0.7, 1.3, hash(cellId + vec2(41.3, 97.2)));
    float travel = u_phaseAmplitude * mix(0.45, 1.0, periodNorm) * ampJitter;

    // Perpendicular wobble for organic feel
    float wobbleFreq = mix(1.5, 3.0, hash(cellId + vec2(73.1, 29.3)));
    float wobble = sin(phase * wobbleFreq * 6.2832) * 0.15 * travel;

    // Combine forward travel + wobble before single clamp (avoids edge bunching)
    float lon = clamp(
        anchorLon + direction.x * travel * eased + (-direction.y) * wobble,
        u_viewportBounds.x,
        u_viewportBounds.z
    );
    float lat = clamp(
        anchorLat - direction.y * travel * eased - direction.x * wobble,
        u_viewportBounds.y,
        u_viewportBounds.w
    );

    float waveDirDeg = mod(degrees(atan(direction.x, direction.y)) + 360.0, 360.0);
    fragColor = vec4(lon, lat, phase, packHeightDir(waveHeight, waveDirDeg));
}

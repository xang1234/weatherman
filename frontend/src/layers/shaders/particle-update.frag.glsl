#version 300 es
precision highp float;

// Previous frame's particle state texture (RGBA32F).
// R = longitude [0,1], G = latitude [0,1], B = age [0,1], A = reserved
uniform sampler2D u_stateTex;

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

    // Random walk offset (placeholder — wind advection replaces this in wx-0pg.2)
    vec2 seedOffset = v_uv * 256.0 + vec2(u_seed);
    vec2 rnd = hash2(seedOffset) * 2.0 - 1.0; // [-1, 1]

    // ~0.001 in mercator units per frame ≈ visible gentle drift
    float walkSpeed = 0.001;
    lon += rnd.x * walkSpeed;
    lat += rnd.y * walkSpeed;

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

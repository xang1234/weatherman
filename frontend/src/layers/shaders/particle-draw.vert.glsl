#version 300 es
precision highp float;

// Particle state textures (RGBA32F): R=lon, G=lat, B=age, A=speed
uniform sampler2D u_stateTex;      // after this frame's update
uniform sampler2D u_prevStateTex;  // before it
uniform mat4 u_matrix;     // MapLibre model-view-projection
uniform vec2 u_viewport;   // drawing buffer size in pixels
uniform float u_lineWidth; // trail width in pixels
uniform float u_speedMax;  // Maximum wind speed for normalization

out float v_age;
out float v_speed;
// Pixel position in the segment's own frame: x runs from the tail (0) to the
// head (v_length), y is the distance across.
out vec2 v_local;
out float v_length;

// Each particle is one segment, from where it was before this frame's update
// to where it is now, drawn as a quad (two triangles) around that segment.
// (end, side): end 0 = tail, 1 = head; side -1/+1 across the segment.
const vec2 CORNERS[6] = vec2[6](
    vec2(0.0, -1.0), vec2(1.0, -1.0), vec2(0.0, 1.0),
    vec2(0.0, 1.0), vec2(1.0, -1.0), vec2(1.0, 1.0)
);

vec2 toPixels(vec2 mercator) {
    vec4 clip = u_matrix * vec4(mercator, 0.0, 1.0);
    return (clip.xy / clip.w * 0.5 + 0.5) * u_viewport;
}

void main() {
    // Six vertices per particle; no vertex buffers, everything comes from gl_VertexID.
    int particle = gl_VertexID / 6;
    int stateSize = textureSize(u_stateTex, 0).x;
    ivec2 texel = ivec2(particle % stateSize, particle / stateSize);

    vec4 state = texelFetch(u_stateTex, texel, 0);
    vec4 prev = texelFetch(u_prevStateTex, texel, 0);
    v_age = state.b;
    v_speed = state.a / max(u_speedMax, 1.0); // normalize to [0,1]

    // Just respawned, or no wind data at its position: not drawn. This also
    // keeps a segment from joining the old position to the new one.
    if (state.a < 0.0) {
        gl_Position = vec4(2.0, 2.0, 2.0, 1.0);
        return;
    }

    vec2 tail = toPixels(prev.rg);
    vec2 head = toPixels(state.rg);
    float len = distance(tail, head);
    vec2 dir = len > 1e-4 ? (head - tail) / len : vec2(1.0, 0.0);
    vec2 normal = vec2(-dir.y, dir.x);

    // Pad by half the width for the round caps, plus a pixel for the soft edge.
    float pad = 0.5 * u_lineWidth + 1.0;
    vec2 corner = CORNERS[gl_VertexID % 6];
    v_local = vec2(corner.x * len + (corner.x * 2.0 - 1.0) * pad, corner.y * pad);
    v_length = len;

    vec2 pixel = tail + dir * v_local.x + normal * v_local.y;
    gl_Position = vec4(pixel / u_viewport * 2.0 - 1.0, 0.0, 1.0);
}

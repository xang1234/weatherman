#version 300 es
precision highp float;

uniform float u_lineWidth; // trail width in pixels

in float v_age;
in float v_speed;
in vec2 v_local;
in float v_length;
out vec4 fragColor;

void main() {
    // Distance from this pixel to the segment: a capsule, round at both ends.
    // A particle that barely moved this frame degenerates to a round dot.
    float dist = length(vec2(v_local.x - clamp(v_local.x, 0.0, v_length), v_local.y));
    float radius = 0.5 * u_lineWidth;
    float shape = 1.0 - smoothstep(radius - 0.5, radius + 0.5, dist);

    // Speed-dependent brightness: calm zones recede, strong wind glows.
    // pow < 1 lifts mid-range speeds so typical winds stay clearly visible.
    float speedAlpha = mix(0.3, 1.0, pow(clamp(v_speed, 0.0, 1.0), 0.7));

    // Hold full brightness through life; only ease in at spawn (hides the
    // respawn pop) and ease out near death. The trail buffer carries motion —
    // a linear age fade just makes the whole field look dim and grainy.
    float fadeIn = smoothstep(0.0, 0.05, v_age);
    float fadeOut = 1.0 - smoothstep(0.85, 1.0, v_age);

    float alpha = shape * speedAlpha * fadeIn * fadeOut;

    // White particle with premultiplied alpha
    fragColor = vec4(alpha, alpha, alpha, alpha);
}

#version 300 es
precision highp float;

in float v_age;
in float v_speed;
out vec4 fragColor;

void main() {
    // Just respawned, or no wind data at its position
    if (v_speed < 0.0) discard;

    // Circular point with soft edge using gl_PointCoord [0,1]
    vec2 ctr = gl_PointCoord - 0.5;
    float dist = length(ctr) * 2.0; // 0 at center, 1 at edge
    float circle = 1.0 - smoothstep(0.6, 1.0, dist);

    // Speed-dependent brightness: calm zones recede, strong wind glows.
    // pow < 1 lifts mid-range speeds so typical winds stay clearly visible.
    float speedAlpha = mix(0.3, 1.0, pow(clamp(v_speed, 0.0, 1.0), 0.7));

    // Hold full brightness through life; only ease in at spawn (hides the
    // respawn pop) and ease out near death. The trail buffer carries motion —
    // a linear age fade just makes the whole field look dim and grainy.
    float fadeIn = smoothstep(0.0, 0.05, v_age);
    float fadeOut = 1.0 - smoothstep(0.85, 1.0, v_age);

    float alpha = circle * speedAlpha * fadeIn * fadeOut;

    // White particle with premultiplied alpha
    fragColor = vec4(alpha, alpha, alpha, alpha);
}

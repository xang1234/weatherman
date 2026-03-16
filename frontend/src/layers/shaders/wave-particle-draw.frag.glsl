#version 300 es
precision highp float;

in float v_age;
in float v_speed;
in float v_direction; // propagation direction in radians
out vec4 fragColor;

// SDF rounded rectangle centered at origin.
// halfSize = (halfWidth, halfHeight), r = corner radius.
float sdRoundedRect(vec2 p, vec2 halfSize, float r) {
    vec2 d = abs(p) - halfSize + r;
    return length(max(d, 0.0)) + min(max(d.x, d.y), 0.0) - r;
}

void main() {
    // Map gl_PointCoord [0,1] to centered [-0.5, 0.5]
    vec2 p = gl_PointCoord - 0.5;

    // Rotate so local X aligns with wave crest (perpendicular to propagation).
    // Propagation is along v_direction, so crest is at v_direction + 90°.
    // We rotate by -v_direction to align propagation with local Y axis,
    // making local X the crest axis.
    float c = cos(-v_direction);
    float s = sin(-v_direction);
    vec2 rotated = vec2(c * p.x - s * p.y, s * p.x + c * p.y);

    // SDF dash: wide along crest (X), thin along propagation (Y)
    float halfWidth = 0.42;   // crest axis — ~84% of point size
    float halfHeight = 0.07;  // propagation axis — thin dash
    float radius = 0.04;      // rounded corners

    float d = sdRoundedRect(rotated, vec2(halfWidth, halfHeight), radius);

    // Anti-aliased edge (1px feather in normalized point coords)
    float aa = fwidth(d);
    float shape = 1.0 - smoothstep(-aa, aa, d);

    // Zero-speed slots are invalid/calm and should disappear entirely.
    float speedNorm = clamp(v_speed, 0.0, 1.0);
    float speedAlpha = smoothstep(0.0, 0.01, speedNorm) * mix(0.15, 1.0, speedNorm);

    // Lifecycle fade: smooth fade-in at birth, fade-out approaching death
    float fadeIn = smoothstep(0.0, 0.05, v_age);
    float fadeOut = 1.0 - smoothstep(0.70, 1.0, v_age);
    float lifecycle = fadeIn * fadeOut;

    float alpha = shape * speedAlpha * lifecycle;

    // White color with premultiplied alpha
    fragColor = vec4(alpha, alpha, alpha, alpha);
}

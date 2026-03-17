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

    // Two dashes along the crest axis, each half the original width,
    // separated by a gap for a broken wavefront look.
    float dashHalfW = 0.10;   // each segment ~half the original 0.42
    float dashHalfH = 0.07;   // propagation axis — thin dash
    float radius = 0.03;      // rounded corners
    float offset = 0.15;      // center-to-center half-separation

    float d1 = sdRoundedRect(rotated - vec2( offset, 0.0), vec2(dashHalfW, dashHalfH), radius);
    float d2 = sdRoundedRect(rotated - vec2(-offset, 0.0), vec2(dashHalfW, dashHalfH), radius);
    float d = min(d1, d2); // SDF union

    // Anti-aliased edge (1px feather in normalized point coords)
    float aa = fwidth(d);
    float shape = 1.0 - smoothstep(-aa, aa, d);

    // Soft glow halo around the dashes for a shiny/luminous look
    float glow = exp(-max(d, 0.0) * 14.0) * 0.4;

    // Zero-speed slots are invalid/calm and should disappear entirely.
    float speedNorm = clamp(v_speed, 0.0, 1.0);
    float speedAlpha = smoothstep(0.0, 0.01, speedNorm) * mix(0.6, 1.0, speedNorm);

    // Lifecycle fade: brief flash, not a long-lived tracer
    float fadeIn = smoothstep(0.0, 0.08, v_age);
    float fadeOut = 1.0 - smoothstep(0.30, 0.50, v_age);
    float lifecycle = fadeIn * fadeOut;

    // Combine crisp shape + soft glow for a bright, shiny appearance
    float core = shape * speedAlpha * lifecycle;
    float halo = glow * speedAlpha * lifecycle;
    float alpha = clamp(core + halo, 0.0, 1.0);

    // Bright white with premultiplied alpha
    fragColor = vec4(alpha, alpha, alpha, alpha);
}

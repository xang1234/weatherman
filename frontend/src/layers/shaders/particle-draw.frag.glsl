#version 300 es
precision highp float;

in float v_age;
in float v_speed;
out vec4 fragColor;

void main() {
    // Circular point with soft edge using gl_PointCoord [0,1]
    vec2 ctr = gl_PointCoord - 0.5;
    float dist = length(ctr) * 2.0; // 0 at center, 1 at edge
    float circle = 1.0 - smoothstep(0.6, 1.0, dist);

    // Speed-dependent brightness: faster wind = brighter
    float speedAlpha = mix(0.5, 1.0, clamp(v_speed, 0.0, 1.0));

    // Fade out as particle ages (age 0→1 over lifespan)
    float ageFade = 1.0 - v_age;

    float alpha = circle * speedAlpha * ageFade;

    // White particle with premultiplied alpha
    fragColor = vec4(alpha, alpha, alpha, alpha);
}

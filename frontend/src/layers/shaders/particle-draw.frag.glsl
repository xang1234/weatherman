#version 300 es
precision highp float;

in float v_age;
out vec4 fragColor;

void main() {
    // Circular point with soft edge using gl_PointCoord [0,1]
    vec2 ctr = gl_PointCoord - 0.5;
    float dist = length(ctr) * 2.0; // 0 at center, 1 at edge
    float alpha = 1.0 - smoothstep(0.5, 1.0, dist);

    // Fade out as particle ages (age 0→1 over lifespan)
    alpha *= 1.0 - v_age;

    // White particle with premultiplied alpha
    fragColor = vec4(alpha, alpha, alpha, alpha);
}

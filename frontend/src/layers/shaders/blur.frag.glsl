#version 300 es
precision highp float;

uniform sampler2D u_texture;
uniform vec2 u_texelSize;
uniform float u_blurRadius;
uniform float u_opacity;

in vec2 v_uv;
out vec4 fragColor;

void main() {
    // Passthrough when blur is disabled (zero-cost at high zoom)
    if (u_blurRadius < 0.001) {
        fragColor = texture(u_texture, v_uv) * u_opacity;
        return;
    }

    // 9-tap Gaussian blur (3x3 weighted kernel)
    // Weights: center=0.25, cardinals=0.125, corners=0.0625 (sum=1.0)
    vec2 off = u_blurRadius * u_texelSize;

    vec4 color = texture(u_texture, v_uv) * 0.25;

    // Cardinal neighbors (4 × 0.125)
    color += texture(u_texture, v_uv + vec2( off.x, 0.0))  * 0.125;
    color += texture(u_texture, v_uv + vec2(-off.x, 0.0))  * 0.125;
    color += texture(u_texture, v_uv + vec2(0.0,  off.y))  * 0.125;
    color += texture(u_texture, v_uv + vec2(0.0, -off.y))  * 0.125;

    // Corner neighbors (4 × 0.0625)
    color += texture(u_texture, v_uv + vec2( off.x,  off.y)) * 0.0625;
    color += texture(u_texture, v_uv + vec2(-off.x,  off.y)) * 0.0625;
    color += texture(u_texture, v_uv + vec2( off.x, -off.y)) * 0.0625;
    color += texture(u_texture, v_uv + vec2(-off.x, -off.y)) * 0.0625;

    fragColor = color * u_opacity;
}

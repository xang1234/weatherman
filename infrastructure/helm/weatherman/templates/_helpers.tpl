{{/*
Common labels for all resources.
*/}}
{{- define "weatherman.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for a specific component.
*/}}
{{- define "weatherman.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Render probe configuration from values.
Accepts a dict with .probe (the probe config from values).
*/}}
{{- define "weatherman.probe" -}}
httpGet:
  path: {{ .probe.httpGet.path }}
  port: {{ .probe.httpGet.port }}
{{- if .probe.initialDelaySeconds }}
initialDelaySeconds: {{ .probe.initialDelaySeconds }}
{{- end }}
periodSeconds: {{ .probe.periodSeconds }}
timeoutSeconds: {{ .probe.timeoutSeconds }}
failureThreshold: {{ .probe.failureThreshold }}
{{- if .probe.successThreshold }}
successThreshold: {{ .probe.successThreshold }}
{{- end }}
{{- end }}

{{/*
Render merged plain env vars and secretKeyRef-backed env vars.
Accepts:
- .globalEnv / .componentEnv as string maps
- .globalSecretEnv / .componentSecretEnv as maps of
  { name: <secretName>, key: <secretKey>, optional?: <bool> }
*/}}
{{- define "weatherman.containerEnv" -}}
{{- $env := mergeOverwrite (dict) (.globalEnv | default dict) (.componentEnv | default dict) -}}
{{- range $name, $value := $env }}
- name: {{ $name }}
  value: {{ $value | quote }}
{{- end }}
{{- $secretEnv := mergeOverwrite (dict) (.globalSecretEnv | default dict) (.componentSecretEnv | default dict) -}}
{{- range $name, $spec := $secretEnv }}
- name: {{ $name }}
  valueFrom:
    secretKeyRef:
      name: {{ $spec.name | quote }}
      key: {{ $spec.key | quote }}
      {{- if hasKey $spec "optional" }}
      optional: {{ $spec.optional }}
      {{- end }}
{{- end }}
{{- end }}

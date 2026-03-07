import type { DataAgeState, FreshnessStatus } from '@/types/data-age'

const STATUS_COLORS: Record<FreshnessStatus, string> = {
  fresh: '#2ea043',   // green
  aging: '#d29922',   // amber
  stale: '#f85149',   // red
}

function formatRelativeTime(minutes: number): string {
  if (minutes < 1) return 'just now'
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  const mins = minutes % 60
  if (hours < 24) {
    return mins > 0 ? `${hours}h ${mins}m ago` : `${hours}h ago`
  }
  const days = Math.floor(hours / 24)
  return `${days}d ${hours % 24}h ago`
}

function formatAbsoluteTime(date: Date): string {
  return date.toISOString().replace('T', ' ').slice(0, 16) + 'Z'
}

export interface DataAgeIndicatorProps {
  state: DataAgeState
}

export function DataAgeIndicator({ state }: DataAgeIndicatorProps) {
  const dotColor = STATUS_COLORS[state.status]

  return (
    <div
      style={{
        position: 'absolute',
        top: 10,
        right: 50,
        zIndex: 10,
        background: 'rgba(13, 17, 23, 0.85)',
        backdropFilter: 'blur(8px)',
        border: '1px solid rgba(48, 54, 61, 0.6)',
        borderRadius: 8,
        padding: '8px 12px',
        color: '#e6edf3',
        fontSize: 12,
        fontFamily: 'system-ui, -apple-system, sans-serif',
        lineHeight: 1.4,
        minWidth: 160,
        userSelect: 'none',
      }}
      title={`Published: ${formatAbsoluteTime(state.publishedAt)}`}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        {/* Status dot */}
        <span
          style={{
            width: 8,
            height: 8,
            borderRadius: '50%',
            background: dotColor,
            boxShadow: `0 0 6px ${dotColor}`,
            flexShrink: 0,
          }}
        />
        {/* Model + run */}
        <span style={{ fontWeight: 600, textTransform: 'uppercase' }}>
          {state.model}
        </span>
        <span style={{ color: '#8b949e' }}>{state.runId}</span>
        {/* Offline warning */}
        {state.isOffline && (
          <span title={state.error ?? 'Backend unreachable'} style={{ marginLeft: 'auto' }}>
            &#x26A0;
          </span>
        )}
      </div>
      <div style={{ color: '#8b949e', marginTop: 2 }}>
        {formatRelativeTime(state.ageMinutes)}
        <span style={{ margin: '0 4px' }}>&middot;</span>
        {formatAbsoluteTime(state.publishedAt)}
      </div>
    </div>
  )
}

import type { CSSProperties } from 'react'
import type { VoyageRouteState } from '@/hooks/useVoyageRoute'

export interface VoyageDrawButtonProps {
  route: VoyageRouteState
}

export function VoyageDrawButton({ route }: VoyageDrawButtonProps) {
  if (route.isDrawing) {
    return (
      <div style={containerStyle}>
        <button
          type="button"
          onClick={route.finishDrawing}
          style={{ ...buttonStyle, background: 'rgba(56, 139, 253, 0.18)', color: '#58a6ff' }}
        >
          Done ({route.waypoints.length} pts)
        </button>
        {route.waypoints.length > 0 && (
          <button type="button" onClick={route.undoLastWaypoint} style={buttonStyle}>
            Undo
          </button>
        )}
      </div>
    )
  }

  return (
    <div style={containerStyle}>
      <button type="button" onClick={route.startDrawing} style={buttonStyle}>
        Draw Route
      </button>
      {route.lineString && (
        <button type="button" onClick={route.clearRoute} style={buttonStyle}>
          Clear
        </button>
      )}
    </div>
  )
}

const containerStyle: CSSProperties = {
  position: 'absolute',
  bottom: 80,
  right: 12,
  zIndex: 10,
  display: 'flex',
  gap: 6,
}

const buttonStyle: CSSProperties = {
  border: '1px solid rgba(48, 54, 61, 0.6)',
  background: 'rgba(13, 17, 23, 0.85)',
  backdropFilter: 'blur(8px)',
  color: '#c9d1d9',
  borderRadius: 8,
  padding: '8px 14px',
  cursor: 'pointer',
  fontSize: 13,
  fontWeight: 500,
}

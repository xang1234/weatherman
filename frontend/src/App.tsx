import { lazy, Suspense } from 'react'

const MapView = lazy(() =>
  import('@/components/MapView').then((m) => ({ default: m.MapView })),
)

function App() {
  return (
    <div style={{ width: '100vw', height: '100vh' }}>
      <Suspense
        fallback={
          <div
            style={{
              width: '100%',
              height: '100%',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              background: '#f0f0f0',
              color: '#374151',
            }}
          >
            Loading map...
          </div>
        }
      >
        <MapView />
      </Suspense>
    </div>
  )
}

export default App

const FORECAST_DATE_FORMATTER = new Intl.DateTimeFormat(undefined, {
  weekday: 'short',
  month: 'short',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
  timeZone: 'UTC',
})

/** Valid time of a forecast hour, e.g. "Wed, Sep 30, 12:00" (UTC); F-notation without a cycle time. */
export function formatForecastDateTime(
  cycleTime: string | null,
  forecastHour: number,
): string {
  const cycleDate = cycleTime ? new Date(cycleTime) : null
  if (!cycleDate || Number.isNaN(cycleDate.getTime())) {
    return `F${forecastHour.toString().padStart(3, '0')}`
  }
  const validDate = new Date(cycleDate.getTime() + forecastHour * 60 * 60 * 1000)
  return FORECAST_DATE_FORMATTER.format(validDate)
}

/** "12.35°N, 165.88°W": longitude wrapped into ±180°, hemispheres instead of signs. */
export function formatLatLon(lat: number, lon: number, digits = 2): string {
  const wrapped = ((((lon + 180) % 360) + 360) % 360) - 180
  const ns = lat < 0 ? 'S' : 'N'
  const ew = wrapped < 0 ? 'W' : 'E'
  return `${Math.abs(lat).toFixed(digits)}°${ns}, ${Math.abs(wrapped).toFixed(digits)}°${ew}`
}

export interface CoverageParameter {
  observedProperty?: {
    label?: {
      en?: string
    }
  }
  unit?: {
    symbol?: string
  }
}

export interface CoverageRange {
  values: Array<number | null>
}

export interface CoverageJSON {
  domain: {
    axes: {
      x: { values: number[] }
      y: { values: number[] }
      t: { values: number[] }
    }
  }
  parameters: Record<string, CoverageParameter>
  ranges: Record<string, CoverageRange>
}

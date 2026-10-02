/**
 * An AIS snapshot as the map shows it. The snapshot of a date is rebuilt
 * through the day by live ingest; each build has a new revision, and tile
 * URLs carry it so a rebuild reaches an open map (#72).
 */
export interface AISSnapshot {
  /** YYYY-MM-DD */
  date: string
  /** 0 when the server reports none (a database from before revisions). */
  revision: number
}

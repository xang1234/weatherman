"""Quality control checks for processed weather data.

These checks run after processing and before publish. In Phase 2 they
log warnings and return structured results; in Phase 4 (wx-glk.2) they
will gate the publish step.
"""

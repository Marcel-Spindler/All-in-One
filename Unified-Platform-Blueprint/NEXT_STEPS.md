# Next Steps - Start jetzt

## Sprint 0 (direkt)
1. PS Copilot Backend um /api/v1/runs Endpunkte erweitern.
2. In-Memory Run Store als erster Schritt (spaeter SQLite).
3. Standardisierte Run-Client-Helfer in Python erstellen (ein Modul pro Tool oder shared snippet).

## Sprint 1 (erste End-to-End Version)
1. PDL Fast meldet Run Start/Ende inkl. Metriken und Artefakte.
2. Incident Tool meldet Incident-Exportlauf.
3. Waagen Performance meldet Reportlauf.
4. PS Copilot zeigt letzte 20 Runs + Fehler an.

## Technische Reihenfolge
1. API im Node Backend implementieren.
2. Python Helper (requests + retry + timeout) erstellen.
3. Je Tool ein minimaler Hook an bestehender Exportstelle.
4. Smoke-Test: 1 Lauf je Tool, danach zentrale Run-Liste pruefen.

## Abnahmekriterien
- Jeder Lauf hat runId und status.
- Fehler sind zentral sichtbar.
- Output-Dateien sind je Lauf verknuepft.
- Keine bestehende Businesslogik kaputt.

## Hinweise
- Phase 1 bewusst ohne Firebase.
- Fokus auf Stabilitaet und Transparenz.
- Danach optional: SQLite + Copilot Ops Dashboard.

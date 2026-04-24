# Verbesserungen - Priorisiert

## Bereits erreicht
- Zentrale Ergebnisablage unter results/
- Gemeinsame Blaupause, Contracts und API-V1
- Erste Run-API im Hub
- Unified Launcher + Desktop-Shortcut

## Jetzt sinnvollster Ausbau

### P1 - Sofort hoher Mehrwert
1. Python-Tools posten jeden Lauf an den Hub (`/api/v1/runs/*`).
2. PS Copilot zeigt letzte Runs, Fehler und Artefakte als echtes Ops-Board.
3. Gemeinsame Konfigurationsdatei fuer Ports, Pfade und Ergebnisziele.

### P2 - Stabilitaet und Transparenz
4. In-Memory Run Store durch SQLite ersetzen.
5. Zentraler Fehler-Log mit Zeitstempel, Tool, Datei und Schweregrad.
6. Health-Checks pro Tool im Dashboard farblich anzeigen.

### P3 - Best-of-Best Ausbaustufe
7. Ein zentrales Web-Frontend statt nur Launcher + Einzeltools.
8. Cross-Tool KPI-Layer: Incident + PDL + QC zusammen visualisieren.
9. Daily Ops Brief automatisch aus den letzten Runs generieren.
10. Optional externe Freigaben/Cloud nur fuer Reports und Benachrichtigungen.

## Empfehlung fuer den naechsten echten Bauschritt
Der technisch richtige naechste Schritt ist:
- `Run-Historie + SQLite + Ops-Dashboard`

Damit wird aus der Sammlung einzelner Tools ein wirkliches Plattform-System.

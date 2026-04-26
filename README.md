# All in One

Zentrale Arbeitsumgebung fuer vier operative Tools:

- PDL fast
- incident-tool anhand BoxIDs
- PS Copilot
- Waagen Performance

Ziel: Ein gemeinsames Projekt mit klaren Startpunkten, nachvollziehbaren Datenfluessen und moeglichst wenig Ratearbeit fuer neue Nutzer.

## Was ist wo?

### PDL fast

Pfad: PDL fast

Zweck:

- Tracking und RESET fuer Factor
- Verarbeitung von PDL-Dateien
- Gewichts- und Picklist-Bezug
- neue GSheet-Transparenz fuer Forecast und Inbound

Wichtig:

- Hauptstart ueber PDL fast/launch_pdl_fast.cmd
- In der App gibt es Bereiche fuer Factor, Weekly Fulfillment, Maitre Sync und Admin-Kontrolle

### Incident Tool

Pfad: incident-tool anhand BoxIDs

Zweck:

- Vorfaelle auf Produktionsdaten anwenden
- Boxen, Customer-IDs und Picklist zusammenfuehren
- Excel-Export fuer Factor-Incident-Prozess erzeugen

Wichtig:

- Moderner Einstieg liegt unter incident-tool anhand BoxIDs/modern_incident_tool/app.py
- Imports-Dateien muessen im Imports-Ordner liegen oder im Admin-Bereich hochgeladen werden

### PS Copilot

Pfad: PS Copilot/ps-basecamp-backend

Zweck:

- Wissenshub, Chat, Ops-Uebersicht
- Einstieg fuer Fragen, Wissen und spaeter zentrale Plattform-Funktionen

Wichtig:

- Start ueber PS Copilot/ps-basecamp-backend oder vorhandene BAT-Dateien
- Standardadresse lokal: <http://localhost:3020>

### Unified-Platform-Blueprint

Pfad: Unified-Platform-Blueprint

Zweck:

- Zielarchitektur, Contracts, API-Blaupause, gemeinsame Ergebnisse

Wichtig:

- Nicht das operative Haupttool, sondern die gemeinsame Integrationsbasis

### Waagen Performance

Pfad: Waagen Performance

Zweck:

- Report-Erzeugung und Qualitaetsauswertung fuer Waagen-/Bug-Performance

## Empfohlene Startreihenfolge

1. PDL fast starten, wenn Tracking, RESET, Picklist- und Produktionsvergleich benoetigt wird.
2. Incident Tool starten, wenn konkrete Produktionsvorfaelle auf Boxen angewendet werden sollen.
3. PS Copilot starten, wenn Wissen, Daily Ops oder zentrales Nachschlagen benoetigt wird.
4. Waagen Performance ausfuehren, wenn QC-/Performance-Reports gebraucht werden.

## Fuer neue Nutzer

Wenn Du nicht weisst, wo Du anfangen sollst:

1. Lies zuerst die Hilfetexte in der jeweiligen App.
2. Arbeite immer mit der richtigen KW.
3. Pruefe vor dem Rechnen, ob die benoetigten Dateien oder GSheets wirklich geladen wurden.
4. Exportiere Ergebnisse erst, wenn Plausibilitaet und Status passen.

## Projektprinzipien

- Bestehende Fachlogik bleibt erhalten.
- Neue Funktionen werden so eingebaut, dass vorhandene Prozesse nicht brechen.
- Admin- und Statussicht sollen zeigen, was geladen, verarbeitet und erzeugt wurde.
- Hilfetexte muessen erklaeren, was in ein Feld gehoert, was ein Button macht und wann welcher Bereich sinnvoll ist.

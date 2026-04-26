# Unified Ops Platform - Blaupause

## Zielbild
Eine gemeinsame Plattform, die die vier bestehenden Tools fachlich verbindet, ohne ihre Staerken zu verlieren:
- incident-tool anhand BoxIDs
- PDL fast
- Waagen Performance
- PS Copilot (ps-basecamp-backend)

Leitlinie: lose Kopplung, klare Datenvertraege, gemeinsame Orchestrierung, schrittweise Migration ohne Big Bang.

## Ausgangslage (Ist)
- Alle Tools arbeiten lokal und dateibasiert (CSV/XLSX/HTML/JSON).
- PDL Fast und Incident Tool haben ueberlappende Quellen (Box, Recipe, Picklist, Customer).
- Waagen Performance erzeugt Reports mit hoher Relevanz fuer Qualitaetssteuerung.
- PS Copilot besitzt bereits ein lauffaehiges Backend mit API-Struktur und eignet sich als Integrations-Hub.

## Zielarchitektur (Soll)

### 1) Integrations-Hub
Bestehendes Node-Backend in PS Copilot wird erweitert und wird zentrale API-Drehscheibe.

Verantwortung:
- Job-Orchestrierung
- Run-Tracking (Status, Zeit, Inputs, Outputs)
- Data Contracts validieren
- Zugriff auf Artefakte (Dateien/Reports)

### 2) Processing Worker (Python)
Bestehende Python-Logiken bleiben fachlich dort, werden aber als aufrufbare Worker standardisiert:
- incident-worker
- pdl-worker
- qc-worker

Jeder Worker kann:
- Input-Dateien lesen
- Output-Artefakte schreiben
- Run-Metadaten als JSON an Hub zurueckgeben

### 3) Gemeinsame Datenebene
- Phase 1: Dateiablage bleibt (minimaler Eingriff)
- Phase 2: Metadaten in SQLite (spaeter optional Postgres)
- Phase 3: Event-/Run-Historie und aggregierte Dashboards

### 4) Copilot-Schicht
PS Copilot liest nicht nur Wissensdateien, sondern auch Run-Metadaten und Reports.
Ergebnis: Chat kann operative Fragen datenbasiert beantworten.

## Systemkontext

```text
[PDL Fast] -----------\
                        >-- [Integration Hub API] -- [Run Store/Metadata DB]
[Incident Tool] ------/              |                |
                                     |                +-- [Artifacts: CSV/XLSX/HTML]
[Waagen Performance] ---------------/
                                     \
                                      +-- [PS Copilot UI + Chat + Ops Insights]
```

## Standardisierte Datenvertraege
Die initialen Contracts liegen in:
- contracts/incident_run.schema.json
- contracts/pdl_run.schema.json
- contracts/qc_run.schema.json

Gemeinsame Felder:
- runId, tool, status, startedAt, finishedAt
- inputFiles[], outputFiles[]
- metrics{}
- warnings[], errors[]

## API-V1 (Start)
Die API-Definition liegt in:
- api/openapi.yaml

Initiale Kernendpunkte:
- POST /api/v1/runs/start
- PATCH /api/v1/runs/{runId}
- POST /api/v1/runs/{runId}/artifact
- GET /api/v1/runs/{runId}
- GET /api/v1/runs
- GET /api/v1/health

## Integrationsstrategie je Tool

### incident-tool anhand BoxIDs
- Bestehende Auswertelogik unveraendert halten.
- Nach Export: Run-Resultat als JSON (Contract) an Hub melden.
- Artefakte mit Typ incident_log, incident_details, incident_export registrieren.

### PDL fast
- Nach jeder Tracking/RESET-Generierung Run-Metadaten an Hub melden.
- Factor als company im metrics-Block ausweisen.
- Artefakte mit Typ tracking_csv, reset_csv, run_report registrieren.

### Waagen Performance
- Report-Erzeugung um Run-Meldung erweitern.
- Fehlerkennzahlen standardisiert in metrics schreiben.
- HTML/XLSX/CSV als Artefakte registrieren.

### PS Copilot
- Bestehende Chat-/Knowledge-Funktionalitaet beibehalten.
- Neue Ops-Endpoints konsumieren fuer:
  - letzte Runs
  - fehlgeschlagene Jobs
  - KPI-Trends

## Firebase-Entscheidung
Empfehlung fuer Start: **Kein Firebase als Kernplattform in Phase 1**.

Begruendung:
- Aktuelle Prozesse sind batch-/dateibasiert, nicht primär realtime/mobile.
- Schnellster Mehrwert entsteht durch zentralen Integrations-Hub mit Contracts.
- Firebase kann spaeter optional ergaenzt werden fuer:
  - User Auth
  - Push/Notifications
  - externe Dashboards

## Umsetzungsplan (3 Phasen)

### Phase 1 - Hub + Contracts (1-2 Wochen)
- OpenAPI-Endpunkte im PS Copilot Backend anlegen
- Run-Contracts validieren
- Alle drei Python-Tools senden Run-Metadaten
- Erstes zentrales Run-Monitoring

### Phase 2 - Gemeinsame Metadatenbank (1 Woche)
- SQLite fuer Runs, Artefakte, Fehler, KPI-Snapshots
- Filterbare Abfrage-Endpoints
- Basis-Dashboard fuer Verlauf und Ausfaelle

### Phase 3 - Intelligente Steuerung (1-2 Wochen)
- Copilot-Ops-Insights auf Run-Daten
- automatisierte Zusammenfassungen (Daily Ops Brief)
- optionale externe Freigaben und Benachrichtigungen

## Definition of Done fuer Phase 1
- Jeder Tool-Run erzeugt einen eindeutigen runId-Eintrag im Hub
- Fehlerstatus und Warnungen werden zentral sichtbar
- Artefakte sind je Run auffindbar
- Mindestens 1 End-to-End-Demo mit allen 4 Tools

## Risiken und Gegenmassnahmen
- Risiko: Unterschiedliche Dateiformate pro Tool
  - Massnahme: Strikte Contracts und Adapter je Tool
- Risiko: Lokale Pfad-/Berechtigungsprobleme
  - Massnahme: zentrale Path-Resolver-Konfiguration
- Risiko: Seiteneffekte in bestehenden Prozessen
  - Massnahme: read-only Integration zuerst, dann write-back

## Startpaket in diesem Ordner
- BLUEPRINT.md
- api/openapi.yaml
- contracts/*.schema.json
- NEXT_STEPS.md

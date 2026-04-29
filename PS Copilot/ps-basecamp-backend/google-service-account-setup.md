# Google Service Account Setup fuer PS Copilot

Dieses Dokument beschreibt, was du brauchst, damit der Helfer Daten aus Google Drive/Google Sheets lesen und einfache Prognosen berechnen kann.

## 1) Was du bereitstellen musst

1. Google Cloud Projekt mit aktivierter API:
- Google Drive API
- Google Sheets API

2. Service Account:
- Service Account E-Mail
- Private Key (JSON Key)

3. Freigaben im Google Drive:
- Der Service Account muss auf den Hauptordner berechtigt sein (mindestens Viewer)
- Falls noetig: auf einzelne Unterordner/Sheets ebenfalls Viewer

4. Optional fuer Firmenumgebung:
- Domain-Wide Delegation + Impersonation User
- Dann kann der Service Account im Namen eines Firmenusers lesen

## 2) .env Eintraege

In der Datei .env folgende Werte setzen:

GOOGLE_SERVICE_ACCOUNT_EMAIL=dein-service-account@dein-projekt.iam.gserviceaccount.com
GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
GOOGLE_DRIVE_ROOT_FOLDER_ID=deine_hauptordner_id
GOOGLE_IMPERSONATED_USER=

Wichtig:
- Private Key muss mit \n gespeichert werden (nicht echte Zeilenumbrueche)
- GOOGLE_IMPERSONATED_USER nur setzen, wenn Domain-Wide Delegation genutzt wird

## 3) Neue API-Endpunkte im Projekt

1. GET /api/google/status
- prueft, ob Konfiguration vorhanden ist und ob der Root-Ordner erreichbar ist

2. GET /api/google/folder/files?folderId=<id>&q=<text>&pageSize=100
- listet Dateien aus dem Ordner
- liefert Zusammenfassung (Anzahl Sheets, Ordner, sonstige Dateien)

3. GET /api/google/sheets/:spreadsheetId/meta
- liefert Sheet-Metadaten und Tab-Namen

4. GET /api/google/sheets/:spreadsheetId/values?range=A1:Z300
- liest Werte aus einem Bereich

5. POST /api/google/sheets/forecast
- erstellt Basis-Prognose (lineare Regression)
- Body Beispiel:
{
  "spreadsheetId": "<sheet-id>",
  "range": "A2:B200",
  "valueColumnIndex": 1,
  "dateColumnIndex": 0,
  "horizon": 7
}

## 4) Was der Helfer mit den Daten direkt machen kann

1. Inventarisierung:
- Welche Sheets/Dateien liegen im Hauptordner
- Welche Dateien wurden kuerzlich veraendert

2. KPI-Analyse:
- Zahlenreihen aus Sheets lesen
- Min/Max/Mittelwert/Trend bestimmen

3. Forecast (Basis):
- 7 bis 30 Schritte voraus
- Trend Up/Down/Flat

4. Vorbereitung fuer spaetere KI-Analytik:
- Datenquellen standardisieren
- Wichtige Ranges definieren
- Qualitaetschecks auf fehlende Werte

## 5) Empfohlene Datenstruktur fuer gute Prognosen

Pro KPI ein Sheet mit:
- Spalte A: Datum/Zeit
- Spalte B: Metrikwert (numerisch)
- Keine gemischten Formate in der Metrikspalte
- Optional weitere Spalten: Shift, Standort, Kategorie

## 6) Nächste Ausbaustufen (wenn du willst)

1. Forecast v2:
- Wochentagseffekte
- Shift-Effekte
- Ausreisser-Erkennung

2. Forecast v3:
- Mehrere Metriken gemeinsam
- Konfidenzintervalle
- Alarmregeln bei Abweichungen

3. Automatisierung:
- Täglicher Snapshot in Knowledge-Datei
- Morning-Briefing mit Top-Risiken und Handlungsempfehlungen

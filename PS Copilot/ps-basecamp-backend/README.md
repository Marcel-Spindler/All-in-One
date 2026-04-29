# PS Copilot Basecamp Backend

Lokales Operations- und Produktionsplanungs-Tool fuer HelloFresh und Factor.

## Kernfunktionen

- Produktionsplaner fuer HF und Factor
- Analyse von Picklisten und PDL-Dateien
- Tagesfokus mit KW- und Cutoff-Aufloesung
- Lokaler Dateizugriff mit Google-Drive-Fallback
- Wissensbereich und Link-Sammlung fuer den operativen Alltag

## Start

```powershell
npm install
npm start
```

Danach ist das Cockpit unter `http://localhost:3020` verfuegbar.

## Schnellzugriff

- Fester Copilot-Link: http://localhost:3020/
- Ein-Klick-Start aus dem Workspace-Root: `PS-Copilot-starten.bat`
- Der Launcher startet den lokalen Server bei Bedarf und oeffnet danach direkt das Cockpit im Browser.

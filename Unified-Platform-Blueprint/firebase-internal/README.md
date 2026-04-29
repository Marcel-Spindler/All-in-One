# Firebase Internal Startpaket

Dieses Verzeichnis ist der sichere Start fuer die interne Online-Version von All in One.

Ziel:

- Firebase und Google Cloud parallel zum bestehenden lokalen Betrieb vorbereiten.
- Keine bestehende lokale App ersetzen.
- Erst Hosting, Login, Regeln und Portal aufbauen.

## Inhalt

- hosting/: internes Web-Portal fuer All in One
- firebase.json: Hosting-, Firestore- und Storage-Konfiguration
- .firebaserc.example: Beispiel fuer Projektzuordnung
- firestore.rules: erste interne Zugriffregeln
- storage.rules: erste interne Storage-Regeln

## Woche 1

1. Firebase-Projekt mit Billing verbinden.
2. Auth aktivieren und auf interne Nutzer begrenzen.
3. Firestore und Storage anlegen.
4. Dieses Hosting-Portal deployen.
5. Erst danach Backend-Dienste schrittweise anbinden.

## Wichtige Leitlinie

Dieses Paket schaltet nichts an den lokalen Launchern oder lokalen Tools ab.
Es schafft nur die Online-Basis fuer die spaetere parallele Migration.

## Geplante naechste Schritte

1. Firebase-Konfiguration in hosting/firebase-config.js eintragen.
2. Login mit internen Konten pruefen.
3. Erste Run-Daten aus Firestore anzeigen.
4. PS Copilot als ersten Cloud-Run-Dienst anbinden.

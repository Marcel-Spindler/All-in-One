import { initializeApp } from "https://www.gstatic.com/firebasejs/11.7.1/firebase-app.js";
import {
  getAuth,
  GoogleAuthProvider,
  onAuthStateChanged,
  signInWithPopup,
  signOut,
} from "https://www.gstatic.com/firebasejs/11.7.1/firebase-auth.js";

const authStatus = document.getElementById("authStatus");
const authDot = document.getElementById("authDot");
const signInBtn = document.getElementById("signInBtn");
const signOutBtn = document.getElementById("signOutBtn");
const userLabel = document.getElementById("userLabel");
const portalMessage = document.getElementById("portalMessage");

let auth;

async function boot() {
  try {
    const configModule = await import("./firebase-config.js");
    const app = initializeApp(configModule.firebaseConfig);
    auth = getAuth(app);
    attachAuthHandlers();
    setReadyState("Firebase bereit. Anmeldung intern testen.");
  } catch (error) {
    setPendingState("Firebase-Konfiguration fehlt noch");
    portalMessage.textContent = "Kopiere firebase-config.example.js nach firebase-config.js und trage die Projektwerte ein. Erst danach kann die interne Anmeldung getestet werden.";
  }
}

function attachAuthHandlers() {
  const provider = new GoogleAuthProvider();

  signInBtn.addEventListener("click", async () => {
    if (!auth) {
      return;
    }

    try {
      await signInWithPopup(auth, provider);
    } catch (error) {
      portalMessage.textContent = `Anmeldung fehlgeschlagen: ${error.message}`;
    }
  });

  signOutBtn.addEventListener("click", async () => {
    if (!auth) {
      return;
    }

    await signOut(auth);
  });

  onAuthStateChanged(auth, (user) => {
    if (user) {
      userLabel.textContent = user.email || "Angemeldet";
      authStatus.textContent = "Interner Nutzer angemeldet";
      signOutBtn.disabled = false;
      portalMessage.textContent = "Login funktioniert. Als Nächstes koennt ihr Firestore-Runs, Rollen und Tool-Status anbinden, ohne die lokalen Tools abzuschalten.";
    } else {
      userLabel.textContent = "Nicht angemeldet";
      authStatus.textContent = "Firebase bereit. Anmeldung intern testen.";
      signOutBtn.disabled = true;
      portalMessage.textContent = "Firebase ist verbunden. Teste jetzt die interne Google-Anmeldung. Danach binden wir Firestore-Runs, Rollen und die ersten Tool-Statusdaten an.";
    }
  });
}

function setReadyState(message) {
  authDot.classList.add("ready");
  authStatus.textContent = message;
  portalMessage.textContent = "Firebase ist verbunden. Als Nächstes kann die interne Google-Anmeldung getestet und danach Firestore fuer Runs sowie Rollen angebunden werden.";
}

function setPendingState(message) {
  authDot.classList.remove("ready");
  authStatus.textContent = message;
  signInBtn.disabled = true;
  signOutBtn.disabled = true;
}

boot();
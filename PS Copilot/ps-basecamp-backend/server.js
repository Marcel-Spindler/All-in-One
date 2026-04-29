// server.js — PS Copilot v2 (Chat + Knowledge)

import express from "express";
import cors from "cors";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import { GoogleGenerativeAI } from "@google/generative-ai";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.join(__dirname, ".env"), override: true });

// === Config ===
const PORT = Number(process.env.PORT) || 3020;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-2.5-flash";
const KNOWLEDGE_DIR = path.join(__dirname, "knowledge");
const DATA_DIR = path.join(__dirname, "data");
const RUNS_STORE_PATH = path.join(DATA_DIR, "runs-store.json");
const PLATFORM_CONFIG_PATH = path.join(__dirname, "..", "..", "Unified-Platform-Blueprint", "platform.config.json");
const DEFAULT_WORKSPACE_ROOT = path.join(__dirname, "..", "..");

if (!GEMINI_API_KEY) {
  console.error("[FEHLER] Keine GEMINI_API_KEY in .env gesetzt.");
}

if (!fs.existsSync(KNOWLEDGE_DIR)) fs.mkdirSync(KNOWLEDGE_DIR, { recursive: true });
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const genAI = new GoogleGenerativeAI(GEMINI_API_KEY);

// === Knowledge ===
function loadKnowledge() {
  return fs
    .readdirSync(KNOWLEDGE_DIR)
    .filter((f) => f.endsWith(".md"))
    .map((file) => {
      try {
        return { file, content: fs.readFileSync(path.join(KNOWLEDGE_DIR, file), "utf-8") };
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function findRelevantKnowledge(query, maxSnippets = 5) {
  const MAX_CHARS = 800_000; // ~200k Tokens, sicher unter dem 1M-Limit
  const docs = loadKnowledge();
  if (!docs.length) return "(Keine Wissensdateien vorhanden.)";

  const keywords = query
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .split(/\s+/)
    .filter((w) => w.length >= 2);

  let selected;
  if (!keywords.length) {
    selected = docs.slice(0, maxSnippets);
  } else {
    selected = docs
      .map((doc) => {
        const hay = (doc.file + " " + doc.content).toLowerCase();
        const score = keywords.reduce((s, kw) => s + (hay.includes(kw) ? 1 : 0), 0);
        return { ...doc, score };
      })
      .filter((d) => d.score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, maxSnippets);
  }

  if (!selected.length) return "(Keine passenden Wissensdateien gefunden.)";

  // Kontext auf MAX_CHARS begrenzen
  const parts = [];
  let total = 0;
  for (const d of selected) {
    const block = `--- ${d.file} ---\n${d.content}`;
    if (total + block.length > MAX_CHARS) {
      const remaining = MAX_CHARS - total;
      if (remaining > 500) parts.push(block.slice(0, remaining) + "\n[... gekürzt]");
      break;
    }
    parts.push(block);
    total += block.length;
  }
  return parts.join("\n\n");
}

// === System-Prompt ===
const SYSTEM_PROMPT = `Du bist PS Copilot — der interne KI-Assistent und die zentrale Wissensbibel für Problem Solve.
Dein Wissen umfasst alles rund um HelloFresh (HF) und Factor.

ANTWORTREGELN:
- Antworte IMMER auf Deutsch. Wichtige englische Fachbegriffe (Technical Terms) setzt du in Klammern dahinter, z.B. "Tageswechsel (Day Change)".
- Strukturiere JEDE Antwort mit Markdown:
  - Nutze ## Überschriften (Headings) um längere Antworten in klare Abschnitte zu gliedern.
  - Nutze ### Unterüberschriften (Subheadings) für Details innerhalb eines Abschnitts.
  - Verwende Listen (- oder 1. 2. 3.) für Schritte und Aufzählungen.
  - Verwende **Fett** für wichtige Begriffe und Schlüsselwörter.
  - Nutze Tabellen wenn Daten oder Vergleiche dargestellt werden.
- Bei kurzen Antworten (1-2 Sätze) sind keine Überschriften nötig.
- Bei längeren Antworten (ab 3+ Absätzen) MUSST du mit Überschriften strukturieren.
- Nutze das bereitgestellte Wissen (Knowledge) um Fragen zu beantworten.
- Wenn du etwas nicht weißt, sag das ehrlich.`;

// === Express App ===
const app = express();
app.use(cors({ origin: true }));
app.use(express.json({ limit: "2mb" }));
app.use(express.static(path.join(__dirname, "public")));

// --- Unified Ops: Run Store (Phase 1: in-memory) ---
const ALLOWED_TOOLS = new Set(["incident-tool", "pdl-fast", "waagen-performance", "ps-copilot"]);
const ALLOWED_STATUS = new Set(["started", "success", "warning", "failed"]);
const runStore = new Map();

function persistRun(run) {
  runStore.set(run.runId, run);
  persistRunStore();
}

function persistRunStore() {
  const payload = {
    savedAt: new Date().toISOString(),
    runs: Array.from(runStore.values()),
  };
  fs.writeFileSync(RUNS_STORE_PATH, JSON.stringify(payload, null, 2), "utf-8");
}

function hydrateRunStore() {
  if (!fs.existsSync(RUNS_STORE_PATH)) {
    return;
  }

  try {
    const raw = fs.readFileSync(RUNS_STORE_PATH, "utf-8");
    const parsed = JSON.parse(raw);
    const runs = Array.isArray(parsed?.runs) ? parsed.runs : [];
    for (const run of runs) {
      if (!run || typeof run !== "object" || !run.runId) continue;
      runStore.set(run.runId, {
        runId: run.runId,
        tool: run.tool,
        status: run.status,
        startedAt: run.startedAt,
        finishedAt: run.finishedAt || null,
        inputFiles: Array.isArray(run.inputFiles) ? run.inputFiles : [],
        outputFiles: Array.isArray(run.outputFiles) ? run.outputFiles : [],
        artifacts: Array.isArray(run.artifacts) ? run.artifacts : [],
        metrics: run.metrics && typeof run.metrics === "object" ? run.metrics : {},
        warnings: Array.isArray(run.warnings) ? run.warnings : [],
        errors: Array.isArray(run.errors) ? run.errors : [],
        updatedAt: run.updatedAt || run.startedAt,
      });
    }
  } catch (error) {
    console.error("[RunStore] Konnte persistente Runs nicht laden:", error.message);
  }
}

function getRunsSnapshot() {
  return Array.from(runStore.values()).sort((a, b) => Date.parse(b.startedAt) - Date.parse(a.startedAt));
}

function filterRuns({ tool = "", status = "", search = "", since = "", limit = 50 } = {}) {
  let runs = getRunsSnapshot();

  if (tool) {
    runs = runs.filter((run) => run.tool === tool);
  }

  if (status) {
    runs = runs.filter((run) => run.status === status);
  }

  if (since) {
    const sinceMs = Date.parse(since);
    if (Number.isFinite(sinceMs)) {
      runs = runs.filter((run) => {
        const startedAtMs = Date.parse(run.startedAt || "");
        return Number.isFinite(startedAtMs) && startedAtMs >= sinceMs;
      });
    }
  }

  if (search) {
    const needle = search.toLowerCase();
    runs = runs.filter((run) => {
      const metricText = Object.entries(run.metrics || {}).map(([key, value]) => `${key}:${value}`).join(" ");
      const warningText = Array.isArray(run.warnings) ? run.warnings.join(" ") : "";
      const errorText = Array.isArray(run.errors) ? run.errors.join(" ") : "";
      const artifactText = Array.isArray(run.artifacts) ? run.artifacts.map((artifact) => `${artifact.type} ${artifact.path}`).join(" ") : "";
      return [run.runId, run.tool, run.status, metricText, warningText, errorText, artifactText]
        .join(" ")
        .toLowerCase()
        .includes(needle);
    });
  }

  return runs.slice(0, limit);
}

function buildOpsSummary() {
  let totalRuns = 0;
  let failedRuns = 0;
  let warningRuns = 0;
  let successRuns = 0;
  const recentRuns = getRunsSnapshot().slice(0, 10);
  const toolStats = {};
  for (const run of runStore.values()) {
    totalRuns += 1;
    if (!toolStats[run.tool]) {
      toolStats[run.tool] = { total: 0, failed: 0, warning: 0, success: 0 };
    }
    toolStats[run.tool].total += 1;
    if (run.status === "failed") {
      failedRuns += 1;
      toolStats[run.tool].failed += 1;
    }
    if (run.status === "warning") {
      warningRuns += 1;
      toolStats[run.tool].warning += 1;
    }
    if (run.status === "success") {
      successRuns += 1;
      toolStats[run.tool].success += 1;
    }
  }

  return {
    totalRuns,
    failedRuns,
    warningRuns,
    successRuns,
    toolStats,
    recentRuns: recentRuns.map(toRunResponse),
  };
}

function buildDailyOpsBrief() {
  const summary = buildOpsSummary();
  const lines = [];
  lines.push("# Daily Ops Brief");
  lines.push("");
  lines.push(`Erstellt: ${new Date().toISOString()}`);
  lines.push("");
  lines.push("## Plattformstatus");
  lines.push(`- Gesamtruns: ${summary.totalRuns}`);
  lines.push(`- Erfolgreich: ${summary.successRuns}`);
  lines.push(`- Warnungen: ${summary.warningRuns}`);
  lines.push(`- Fehlgeschlagen: ${summary.failedRuns}`);
  lines.push("");

  lines.push("## Tool-Lage");
  const toolEntries = Object.entries(summary.toolStats).sort((a, b) => a[0].localeCompare(b[0], "de-DE"));
  if (!toolEntries.length) {
    lines.push("- Noch keine Tool-Runs vorhanden.");
  } else {
    for (const [tool, stats] of toolEntries) {
      lines.push(`- ${tool}: ${stats.total} Runs | ${stats.success} erfolgreich | ${stats.warning} Warnungen | ${stats.failed} fehlgeschlagen`);
    }
  }
  lines.push("");

  lines.push("## Letzte wichtige Läufe");
  if (!summary.recentRuns.length) {
    lines.push("- Keine aktuellen Runs vorhanden.");
  } else {
    for (const run of summary.recentRuns.slice(0, 5)) {
      const metricPreview = Object.entries(run.metrics || {}).slice(0, 3).map(([key, value]) => `${key}=${value}`).join(", ");
      lines.push(`- ${run.tool} | ${run.status} | ${run.startedAt}${metricPreview ? ` | ${metricPreview}` : ""}`);
    }
  }
  lines.push("");

  lines.push("## Fokus heute");
  if (summary.failedRuns > 0) {
    lines.push("1. Fehlgeschlagene Runs zuerst pruefen und blockierende Ursachen beseitigen.");
  } else if (summary.warningRuns > 0) {
    lines.push("1. Warnungs-Runs durchgehen und auf Qualitaets- oder Datenprobleme pruefen.");
  } else {
    lines.push("1. Plattform laeuft stabil. Fokus auf Durchsatz, Transparenz und Automatisierung.");
  }
  lines.push("2. Neue Artefakte im zentralen results-Ordner gegen die letzten Runs abgleichen.");
  lines.push("3. Auffaellige Trends im Ops Board fuers Team festhalten.");

  return {
    generatedAt: new Date().toISOString(),
    summary,
    markdown: lines.join("\n"),
  };
}

function loadPlatformConfig() {
  try {
    if (!fs.existsSync(PLATFORM_CONFIG_PATH)) {
      return {};
    }
    return JSON.parse(fs.readFileSync(PLATFORM_CONFIG_PATH, "utf-8"));
  } catch (error) {
    console.error("[PlatformConfig] Konnte Konfiguration nicht laden:", error.message);
    return {};
  }
}

function buildKpiBoard() {
  const runs = getRunsSnapshot();
  const latestByTool = {};
  for (const run of runs) {
    if (!latestByTool[run.tool]) {
      latestByTool[run.tool] = run;
    }
  }

  const latestFactorRun = runs.find((run) => run.tool === "pdl-fast" && run.metrics?.company === "Factor") || null;
  const latestHelloFreshRun = runs.find((run) => run.tool === "pdl-fast" && run.metrics?.company === "HelloFresh") || null;
  const latestIncidentRun = latestByTool["incident-tool"] || null;
  const latestQcRun = latestByTool["waagen-performance"] || null;

  return {
    incident: latestIncidentRun ? {
      status: latestIncidentRun.status,
      productionDate: latestIncidentRun.metrics?.productionDate || null,
      incidentCount: Number(latestIncidentRun.metrics?.incidentCount || 0),
      affectedBoxes: Number(latestIncidentRun.metrics?.affectedBoxes || 0),
      mappedCustomers: Number(latestIncidentRun.metrics?.mappedCustomers || 0),
    } : null,
    factor: latestFactorRun ? {
      status: latestFactorRun.status,
      week: latestFactorRun.metrics?.week || null,
      boxCount: Number(latestFactorRun.metrics?.boxCount || 0),
      trackingRows: Number(latestFactorRun.metrics?.trackingRows || 0),
      resetRows: Number(latestFactorRun.metrics?.resetRows || 0),
    } : null,
    hellofresh: latestHelloFreshRun ? {
      status: latestHelloFreshRun.status,
      week: latestHelloFreshRun.metrics?.week || null,
      trackingRows: Number(latestHelloFreshRun.metrics?.trackingRows || 0),
      resetRows: Number(latestHelloFreshRun.metrics?.resetRows || 0),
      cutoffCount: Number(latestHelloFreshRun.metrics?.cutoffCount || 0),
    } : null,
    qc: latestQcRun ? {
      status: latestQcRun.status,
      totalChecks: Number(latestQcRun.metrics?.totalChecks || 0),
      totalErrors: Number(latestQcRun.metrics?.totalErrors || 0),
      errorRate: Number(latestQcRun.metrics?.errorRate || 0),
      boxesWithErrors: Number(latestQcRun.metrics?.boxesWithErrors || 0),
      boxErrorRate: Number(latestQcRun.metrics?.boxErrorRate || 0),
    } : null,
    totals: {
      totalRuns: runs.length,
      totalArtifacts: runs.reduce((sum, run) => sum + (Array.isArray(run.artifacts) ? run.artifacts.length : 0), 0),
    },
  };
}

function buildOpsTrends(days = 14) {
  const buckets = new Map();
  const toolTotals = {};
  const threshold = Date.now() - (Math.max(1, days) * 24 * 60 * 60 * 1000);

  for (const run of runStore.values()) {
    const startedAtMs = Date.parse(run.startedAt || "");
    if (!Number.isFinite(startedAtMs) || startedAtMs < threshold) continue;

    const dayKey = new Date(startedAtMs).toISOString().slice(0, 10);
    if (!buckets.has(dayKey)) {
      buckets.set(dayKey, { date: dayKey, total: 0, success: 0, warning: 0, failed: 0 });
    }
    const bucket = buckets.get(dayKey);
    bucket.total += 1;
    if (run.status === "success") bucket.success += 1;
    if (run.status === "warning") bucket.warning += 1;
    if (run.status === "failed") bucket.failed += 1;

    if (!toolTotals[run.tool]) {
      toolTotals[run.tool] = { total: 0, success: 0, warning: 0, failed: 0 };
    }
    toolTotals[run.tool].total += 1;
    if (run.status === "success") toolTotals[run.tool].success += 1;
    if (run.status === "warning") toolTotals[run.tool].warning += 1;
    if (run.status === "failed") toolTotals[run.tool].failed += 1;
  }

  return {
    days: Math.max(1, days),
    daily: Array.from(buckets.values()).sort((a, b) => a.date.localeCompare(b.date, "de-DE")),
    toolTotals,
  };
}

function getWorkspaceRootFromConfig() {
  const config = loadPlatformConfig();
  return config?.paths?.workspaceRoot || DEFAULT_WORKSPACE_ROOT;
}

function isPreviewableExtension(filePath) {
  const ext = path.extname(String(filePath || "")).toLowerCase();
  return [".txt", ".md", ".json", ".csv", ".html", ".log", ".ps1", ".py", ".js", ".yaml", ".yml"].includes(ext);
}

function buildArtifactPreview(filePath) {
  const workspaceRoot = path.resolve(getWorkspaceRootFromConfig());
  const resolvedPath = path.resolve(String(filePath || ""));
  if (!resolvedPath.startsWith(workspaceRoot)) {
    return { ok: false, reason: "Pfad liegt außerhalb des Workspace." };
  }
  if (!fs.existsSync(resolvedPath)) {
    return { ok: false, reason: "Datei nicht gefunden." };
  }
  if (!isPreviewableExtension(resolvedPath)) {
    return { ok: false, reason: "Dateityp ist nicht direkt vorschaufähig." };
  }

  const raw = fs.readFileSync(resolvedPath, "utf-8");
  const ext = path.extname(resolvedPath).toLowerCase();
  const previewText = raw.length > 12000 ? `${raw.slice(0, 12000)}\n\n[... gekürzt]` : raw;
  let kind = "text";
  let structured = null;

  if (ext === ".json") {
    try {
      const parsed = JSON.parse(raw);
      kind = "json";
      structured = {
        rootType: Array.isArray(parsed) ? "array" : typeof parsed,
        itemCount: Array.isArray(parsed) ? parsed.length : undefined,
        keys: parsed && typeof parsed === "object" && !Array.isArray(parsed) ? Object.keys(parsed).slice(0, 20) : undefined,
      };
    } catch {
      kind = "text";
    }
  }

  if ([".csv", ".tsv"].includes(ext)) {
    const lines = raw.split(/\r?\n/).filter((line) => line.trim().length > 0);
    const sampleLine = lines[0] || "";
    const delimiterCandidates = [";", ",", "\t"];
    const delimiter = delimiterCandidates
      .map((candidate) => ({ candidate, score: sampleLine.split(candidate).length }))
      .sort((a, b) => b.score - a.score)[0]?.candidate || ";";
    const rows = lines.slice(0, 21).map((line) => line.split(delimiter).map((value) => value.trim()));
    const headers = rows[0] || [];
    const dataRows = rows.slice(1, 11);
    kind = "table";
    structured = {
      delimiter,
      headers,
      rows: dataRows,
      totalSampleRows: Math.max(0, lines.length - 1),
    };
  }

  if (ext === ".html") {
    kind = "html";
  }

  return {
    ok: true,
    path: resolvedPath,
    ext,
    kind,
    structured,
    previewText,
  };
}

hydrateRunStore();

function createRunId() {
  const rnd = Math.random().toString(36).slice(2, 8);
  return `run_${Date.now()}_${rnd}`;
}

function isIsoDateTime(value) {
  if (!value || typeof value !== "string") return false;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed);
}

function toRunResponse(run) {
  return {
    runId: run.runId,
    tool: run.tool,
    status: run.status,
    startedAt: run.startedAt,
    finishedAt: run.finishedAt || null,
    inputFiles: run.inputFiles,
    outputFiles: run.outputFiles,
    artifacts: run.artifacts,
    metrics: run.metrics,
    warnings: run.warnings,
    errors: run.errors,
    updatedAt: run.updatedAt,
  };
}

// Favicon — kein 404 im Browser
app.get("/favicon.ico", (req, res) => res.status(204).end());

// Chat-Sessions (in-memory)
const sessions = new Map();
const MAX_HISTORY = 40;
const SESSION_TTL = 60 * 60 * 1000; // 1 Stunde

function getSession(id) {
  const session = sessions.get(id);
  if (session) { session.lastAccess = Date.now(); return session; }
  const s = { history: [], lastAccess: Date.now() };
  sessions.set(id, s);
  return s;
}

setInterval(() => {
  const now = Date.now();
  for (const [id, s] of sessions) {
    if (now - s.lastAccess > SESSION_TTL) sessions.delete(id);
  }
}, 5 * 60 * 1000);

// --- API: Chat (Streaming) ---
app.post("/api/chat", async (req, res) => {
  const { message, sessionId } = req.body;
  if (!message || typeof message !== "string" || !message.trim()) {
    return res.status(400).json({ error: "Nachricht darf nicht leer sein." });
  }
  if (!GEMINI_API_KEY) {
    return res.status(500).json({ error: "Kein Gemini API-Key konfiguriert." });
  }

  const sid = sessionId || "default";
  const session = getSession(sid);
  const knowledge = findRelevantKnowledge(message.trim());

  const contextPrompt = `${SYSTEM_PROMPT}\n\n=== RELEVANTES WISSEN ===\n${knowledge}\n=== ENDE WISSEN ===`;

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  try {
    const model = genAI.getGenerativeModel({
      model: GEMINI_MODEL,
      systemInstruction: { role: "user", parts: [{ text: contextPrompt }] },
    });
    const chat = model.startChat({
      history: session.history.map((m) => ({ role: m.role, parts: [{ text: m.text }] })),
    });

    const result = await chat.sendMessageStream(message.trim());
    let fullAnswer = "";

    for await (const chunk of result.stream) {
      const text = chunk.text();
      if (text) {
        fullAnswer += text;
        res.write(`data: ${JSON.stringify({ text })}\n\n`);
      }
    }

    // History aktualisieren
    session.history.push({ role: "user", text: message.trim() });
    session.history.push({ role: "model", text: fullAnswer });
    if (session.history.length > MAX_HISTORY) {
      session.history = session.history.slice(-MAX_HISTORY);
    }

    res.write(`data: ${JSON.stringify({ done: true })}\n\n`);
    res.end();
  } catch (err) {
    console.error("[Chat-Fehler]", err.message);
    res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
    res.end();
  }
});

// --- API: Knowledge CRUD ---
app.get("/api/knowledge", (_req, res) => {
  const docs = loadKnowledge();
  res.json(docs.map((d) => {
    const title = d.content.split("\n").find((l) => l.startsWith("#"))?.replace(/^#+\s*/, "") || d.file;
    return { file: d.file, title, size: Buffer.byteLength(d.content, "utf-8") };
  }));
});

// --- API: Knowledge Headings (TOC) ---
app.get("/api/knowledge/headings", (_req, res) => {
  const docs = loadKnowledge();
  const result = docs.map((d) => {
    const lines = d.content.split("\n");
    const docTitle = lines.find((l) => l.startsWith("# "))?.replace(/^#\s*/, "") || d.file.replace(".md", "");
    const headings = [];
    for (const line of lines) {
      const m = line.match(/^(#{1,3})\s+(.+)/);
      if (m) {
        headings.push({ level: m[1].length, text: m[2].trim() });
      }
    }
    return { file: d.file, title: docTitle, headings };
  });
  res.json(result);
});

app.get("/api/knowledge/:file", (req, res) => {
  const file = path.basename(req.params.file);
  if (!file.endsWith(".md")) return res.status(400).json({ error: "Nur .md Dateien." });
  const filePath = path.join(KNOWLEDGE_DIR, file);
  if (!fs.existsSync(filePath)) return res.status(404).json({ error: "Datei nicht gefunden." });
  const content = fs.readFileSync(filePath, "utf-8");
  res.json({ file, content });
});

app.post("/api/knowledge", (req, res) => {
  const { filename, content } = req.body;
  if (!filename || !content) return res.status(400).json({ error: "filename und content erforderlich." });
  const safe = path.basename(filename).replace(/[^a-zA-Z0-9_\-\.]/g, "_");
  const name = safe.endsWith(".md") ? safe : safe + ".md";
  fs.writeFileSync(path.join(KNOWLEDGE_DIR, name), content, "utf-8");
  res.json({ ok: true, file: name });
});

app.put("/api/knowledge/:file", (req, res) => {
  const file = path.basename(req.params.file);
  if (!file.endsWith(".md")) return res.status(400).json({ error: "Nur .md Dateien." });
  const filePath = path.join(KNOWLEDGE_DIR, file);
  if (!fs.existsSync(filePath)) return res.status(404).json({ error: "Datei nicht gefunden." });
  const { content } = req.body;
  if (!content) return res.status(400).json({ error: "content erforderlich." });
  fs.writeFileSync(filePath, content, "utf-8");
  res.json({ ok: true, file });
});

app.delete("/api/knowledge/:file", (req, res) => {
  const file = path.basename(req.params.file);
  if (!file.endsWith(".md")) return res.status(400).json({ error: "Nur .md Dateien." });
  const filePath = path.join(KNOWLEDGE_DIR, file);
  if (!fs.existsSync(filePath)) return res.status(404).json({ error: "Datei nicht gefunden." });
  fs.unlinkSync(filePath);
  res.json({ ok: true, deleted: file });
});

// --- API: Health ---
app.get("/api/health", (_req, res) => {
  res.json({ status: "ok", model: GEMINI_MODEL, knowledgeFiles: loadKnowledge().length });
});

// --- API v1: Unified Ops Runs ---
app.get("/api/v1/health", (_req, res) => {
  res.json({ status: "ok", service: "ps-copilot-backend", runs: runStore.size, store: RUNS_STORE_PATH });
});

app.get("/api/v1/ops/summary", (_req, res) => {
  res.json(buildOpsSummary());
});

app.get("/api/v1/ops/brief", (_req, res) => {
  res.json(buildDailyOpsBrief());
});

app.get("/api/v1/config", (_req, res) => {
  res.json(loadPlatformConfig());
});

app.get("/api/v1/ops/kpis", (_req, res) => {
  res.json(buildKpiBoard());
});

app.get("/api/v1/ops/trends", (req, res) => {
  const daysRaw = Number.parseInt(String(req.query.days || "14"), 10);
  const days = Number.isFinite(daysRaw) ? Math.max(1, Math.min(90, daysRaw)) : 14;
  res.json(buildOpsTrends(days));
});

app.get("/api/v1/artifacts/preview", (req, res) => {
  const filePath = String(req.query.path || "").trim();
  if (!filePath) {
    return res.status(400).json({ ok: false, reason: "path ist erforderlich." });
  }
  const result = buildArtifactPreview(filePath);
  if (!result.ok) {
    return res.status(400).json(result);
  }
  return res.json(result);
});

app.post("/api/v1/runs/start", (req, res) => {
  const { tool, startedAt, inputFiles = [] } = req.body || {};

  if (!ALLOWED_TOOLS.has(tool)) {
    return res.status(400).json({ error: "Ungueltiges tool. Erlaubt: incident-tool, pdl-fast, waagen-performance, ps-copilot" });
  }
  if (!isIsoDateTime(startedAt)) {
    return res.status(400).json({ error: "startedAt muss ein gueltiger ISO Date-Time String sein." });
  }
  if (!Array.isArray(inputFiles) || inputFiles.some((item) => typeof item !== "string")) {
    return res.status(400).json({ error: "inputFiles muss ein String-Array sein." });
  }

  const runId = createRunId();
  const now = new Date().toISOString();
  const run = {
    runId,
    tool,
    status: "started",
    startedAt,
    finishedAt: null,
    inputFiles,
    outputFiles: [],
    artifacts: [],
    metrics: {},
    warnings: [],
    errors: [],
    updatedAt: now,
  };

  runStore.set(runId, run);
  persistRun(run);
  return res.status(201).json(toRunResponse(run));
});

app.patch("/api/v1/runs/:runId", (req, res) => {
  const run = runStore.get(req.params.runId);
  if (!run) return res.status(404).json({ error: "Run nicht gefunden." });

  const { status, finishedAt, metrics, warnings, errors } = req.body || {};

  if (typeof status !== "undefined") {
    if (!ALLOWED_STATUS.has(status)) {
      return res.status(400).json({ error: "Ungueltiger status. Erlaubt: started, success, warning, failed" });
    }
    run.status = status;
  }

  if (typeof finishedAt !== "undefined") {
    if (finishedAt !== null && !isIsoDateTime(finishedAt)) {
      return res.status(400).json({ error: "finishedAt muss null oder ein gueltiger ISO Date-Time String sein." });
    }
    run.finishedAt = finishedAt;
  }

  if (typeof metrics !== "undefined") {
    if (!metrics || typeof metrics !== "object" || Array.isArray(metrics)) {
      return res.status(400).json({ error: "metrics muss ein Objekt sein." });
    }
    run.metrics = { ...run.metrics, ...metrics };
  }

  if (typeof warnings !== "undefined") {
    if (!Array.isArray(warnings) || warnings.some((item) => typeof item !== "string")) {
      return res.status(400).json({ error: "warnings muss ein String-Array sein." });
    }
    run.warnings = warnings;
  }

  if (typeof errors !== "undefined") {
    if (!Array.isArray(errors) || errors.some((item) => typeof item !== "string")) {
      return res.status(400).json({ error: "errors muss ein String-Array sein." });
    }
    run.errors = errors;
  }

  run.updatedAt = new Date().toISOString();
  persistRun(run);
  return res.json(toRunResponse(run));
});

app.post("/api/v1/runs/:runId/artifact", (req, res) => {
  const run = runStore.get(req.params.runId);
  if (!run) return res.status(404).json({ error: "Run nicht gefunden." });

  const { type, path: artifactPath, contentType = "", sizeBytes = 0 } = req.body || {};
  if (!type || typeof type !== "string") {
    return res.status(400).json({ error: "type ist erforderlich und muss String sein." });
  }
  if (!artifactPath || typeof artifactPath !== "string") {
    return res.status(400).json({ error: "path ist erforderlich und muss String sein." });
  }
  if (typeof contentType !== "string") {
    return res.status(400).json({ error: "contentType muss String sein." });
  }
  if (!Number.isFinite(Number(sizeBytes)) || Number(sizeBytes) < 0) {
    return res.status(400).json({ error: "sizeBytes muss eine nicht-negative Zahl sein." });
  }

  const artifact = {
    type,
    path: artifactPath,
    contentType,
    sizeBytes: Number(sizeBytes),
    createdAt: new Date().toISOString(),
  };

  run.artifacts.push(artifact);
  if (!run.outputFiles.includes(artifactPath)) run.outputFiles.push(artifactPath);
  run.updatedAt = new Date().toISOString();
  persistRun(run);

  return res.json({ ok: true, runId: run.runId, artifact });
});

app.get("/api/v1/runs/:runId", (req, res) => {
  const run = runStore.get(req.params.runId);
  if (!run) return res.status(404).json({ error: "Run nicht gefunden." });
  return res.json(toRunResponse(run));
});

app.get("/api/v1/runs", (req, res) => {
  const tool = String(req.query.tool || "").trim();
  const status = String(req.query.status || "").trim();
  const search = String(req.query.search || "").trim();
  const since = String(req.query.since || "").trim();
  const limitRaw = Number.parseInt(String(req.query.limit || "50"), 10);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(500, limitRaw)) : 50;

  return res.json(filterRuns({ tool, status, search, since, limit }).map(toRunResponse));
});

// --- API: Session löschen ---
app.delete("/api/session/:id", (req, res) => {
  sessions.delete(req.params.id);
  res.json({ ok: true });
});

// --- Start ---
app.listen(PORT, () => {
  const docs = loadKnowledge();
  console.log(`\n  PS Copilot v2 läuft auf http://localhost:${PORT}`);
  console.log(`  Model: ${GEMINI_MODEL}`);
  console.log(`  Run Store: ${RUNS_STORE_PATH}`);
  console.log(`  Knowledge: ${docs.length} Dateien geladen\n`);
});

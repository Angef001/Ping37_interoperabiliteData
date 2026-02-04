"""Dashboard Web (UI).

⚠️ Important : cette UI doit rester cohérente avec la CLI + l'API.

Principe :
- La CLI / l'API génèrent les rapports (last_run.json + archives runs/).
- L'UI lit ces mêmes rapports et les affiche.

Objectif : éviter une 2ème "source de vérité" côté UI.
"""

from fastapi import APIRouter, Request, UploadFile, File, HTTPException, Body, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import os, json
import polars as pl
from pathlib import Path

from app.core.converters.fhir_to_edsan import process_dir  
from app.core.converters.fhir_to_edsan import process_bundle, EDS_DIR
from app.core.converters.edsan_to_fhir import export_eds_to_fhir

import tempfile
import io, zipfile
from pydantic import BaseModel
from typing import Optional
import requests
from zipfile import ZipFile, ZIP_DEFLATED

#  On importe les valeurs par défaut du convertisseur
from app.core.converters.fhir_to_edsan import (
    process_dir,
    process_bundle,
    EDS_DIR as DEFAULT_EDS_DIR,
    REPORTS_DIR as DEFAULT_REPORTS_DIR,
    REPORTS_DIR_EXPORT as DEFAULT_REPORTS_DIR_EXPORT,
)

from app.core.converters.edsan_to_fhir import export_eds_to_fhir


router = APIRouter()
templates = Jinja2Templates(directory="app/web/templates")

# ---------------------------------------------------------------------------
# Configuration globale (partagée avec l'API)
#
# L'UI doit lire les *mêmes* dossiers que la CLI / API.
# On garde les valeurs par défaut du convertisseur, mais on autorise
# la surcharge via variables d'environnement (utile avec Podman).
# ---------------------------------------------------------------------------

EDS_DIR = os.getenv("EDS_DIR", DEFAULT_EDS_DIR)
REPORTS_DIR = os.getenv("REPORTS_DIR", DEFAULT_REPORTS_DIR)
REPORTS_DIR_EXPORT = os.getenv("REPORTS_DIR_EXPORT", DEFAULT_REPORTS_DIR_EXPORT)


def _load_json_if_exists(path: str):
    """Petit helper pour éviter de répéter try/except partout."""
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        # L'UI ne doit jamais planter à cause d'un souci de parsing de rapport
        return None
    return None


def load_last_run() -> dict | None:
    """Charge le dernier rapport d'import généré par la conversion."""
    return _load_json_if_exists(os.path.join(REPORTS_DIR, "last_run.json"))


def load_last_export() -> dict | None:
    """Charge le dernier rapport d'export généré par l'export EDS->FHIR."""
    return _load_json_if_exists(os.path.join(REPORTS_DIR_EXPORT, "last_export_fhir.json"))


def _effective_eds_dir() -> str:
    """
    Détermine le dossier EDS "effectif" pour l'UI.

    Objectif :
    - Si un run a été lancé via CLI/API avec un override --eds-dir,
      on veut que le dashboard affiche les stats du BON dossier.
    - Donc on lit last_run.json et si `eds_dir` existe, on l'utilise.

    Fallback :
    - si last_run absent / mal formé / eds_dir manquant -> EDS_DIR (env/défaut).
    """
    last_run = load_last_run()
    if isinstance(last_run, dict):
        lr_eds = (last_run.get("eds_dir") or "").strip()
        if lr_eds:
            return lr_eds
    return EDS_DIR


def merged_cfg(payload: dict) -> dict:
    """
    Fusionne la config issue d'un payload (UI/POST) avec l'environnement.
    - Si une clé n'est pas fournie dans payload, on prend la variable d'env.
    - Sinon on prend payload.

    Objectif : permettre à la UI (et plus tard aux endpoints) de surcharger
    dynamiquement les chemins (EDS, reports...) sans casser les valeurs par défaut.
    """
    def pick(key: str, default: str | None = None):
        v = payload.get(key)
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return os.getenv(key, default)
        return v

    return {
        # EDS
        "EDS_DIR": pick("EDS_DIR"),

        # Export FHIR (selon votre logique existante)
        "FHIR_OUTPUT_DIR": pick("FHIR_OUTPUT_DIR"),
        "FHIR_EXPORT_DIR": pick("FHIR_EXPORT_DIR"),
        "FHIR_BUNDLE_STRATEGY": pick("FHIR_BUNDLE_STRATEGY", "patient"),

        #  Reports import/export (alignement CLI/API/UI)
        # NB: REPORTS_DIR / REPORTS_DIR_EXPORT doivent être définis plus haut
        # (ex: via os.getenv(..., DEFAULT_REPORTS_DIR))
        "REPORTS_DIR": pick("REPORTS_DIR", REPORTS_DIR),
        "REPORTS_DIR_EXPORT": pick("REPORTS_DIR_EXPORT", REPORTS_DIR_EXPORT),
    }


def list_parquets(eds_dir: str | None = None):
    """
    Liste les .parquet du dossier EDS.
    - Si eds_dir n'est pas fourni, on utilise le dossier EDS effectif (déduit via last_run.json si possible).
    """
    base = eds_dir or _effective_eds_dir()
    if not os.path.isdir(base):
        return []
    return sorted([f for f in os.listdir(base) if f.endswith(".parquet")])


# ================== DASHBOARD =================
@router.get("/ui", response_class=HTMLResponse)
async def ui_home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


# ================= IMPORT FHIR =================
@router.get("/ui/import", response_class=HTMLResponse)
async def import_page(request: Request):
    return templates.TemplateResponse("import.html", {"request": request})


@router.post("/ui/import/run", response_class=HTMLResponse)
async def import_run(
    query_url: str | None = Form(None),
    eds_dir: str | None = Form(None),
    reports_dir: str | None = Form(None),
    file: UploadFile | None = File(None),
):
    """
    Import UI :
    - Mode CHU (recommandé) : query_url + (optionnel) eds_dir + (optionnel) reports_dir
    - Mode legacy : upload d'un bundle JSON

    Objectif :
    - Parquets -> eds_dir (si fourni)
    - Reports (last_run.json + runs/) -> reports_dir (si fourni)
    """
    try:
        import app.core.converters.fhir_to_edsan as f2e_module

        # ----------------------------
        # 1) Mode URL de requête (CHU)
        # ----------------------------
        if query_url and query_url.strip():
            url = query_url.strip()

            old_eds = getattr(f2e_module, "EDS_DIR", None)
            old_rep = getattr(f2e_module, "REPORTS_DIR", None)

            # override si demandé
            if eds_dir and eds_dir.strip():
                f2e_module.EDS_DIR = eds_dir.strip()
            if reports_dir and reports_dir.strip():
                f2e_module.REPORTS_DIR = reports_dir.strip()

            try:
                resp = requests.get(url, headers={"Accept": "application/fhir+json"}, timeout=60)
                resp.raise_for_status()
                bundle = resp.json()

                res = process_bundle(bundle)  # génère parquets + last_run dans REPORTS_DIR
            finally:
                # restore (important)
                if old_eds is not None:
                    f2e_module.EDS_DIR = old_eds
                if old_rep is not None:
                    f2e_module.REPORTS_DIR = old_rep

            eds_html = f"<div class='muted'>EDS_DIR : <code>{eds_dir.strip()}</code></div>" if (eds_dir and eds_dir.strip()) else ""
            rep_html = f"<div class='muted'>REPORTS_DIR : <code>{reports_dir.strip()}</code></div>" if (reports_dir and reports_dir.strip()) else ""

            return HTMLResponse(
                "<div class='card ok'><h3>✅ Import (URL) réussi</h3>"
                f"<div class='muted'>URL : <code>{url}</code></div>"
                f"{eds_html}{rep_html}"
                f"<pre class='pre'>{json.dumps(res, indent=2, ensure_ascii=False)}</pre></div>"
            )

        # ----------------------------
        # 2) Mode legacy : fichier JSON
        # ----------------------------
        if file is not None:
            old_eds = getattr(f2e_module, "EDS_DIR", None)
            old_rep = getattr(f2e_module, "REPORTS_DIR", None)

            if eds_dir and eds_dir.strip():
                f2e_module.EDS_DIR = eds_dir.strip()
            if reports_dir and reports_dir.strip():
                f2e_module.REPORTS_DIR = reports_dir.strip()

            try:
                bundle = json.loads((await file.read()).decode("utf-8"))
                res = process_bundle(bundle)
            finally:
                if old_eds is not None:
                    f2e_module.EDS_DIR = old_eds
                if old_rep is not None:
                    f2e_module.REPORTS_DIR = old_rep

            return HTMLResponse(
                "<div class='card ok'><h3>✅ Import (legacy) réussi</h3>"
                f"<pre class='pre'>{json.dumps(res, indent=2, ensure_ascii=False)}</pre></div>"
            )

        return HTMLResponse(
            "<div class='card err'><h3>❌ Entrée manquante</h3>"
            "<div class='muted'>Fournis soit une <b>URL de requête</b>, soit un <b>fichier JSON</b>.</div></div>",
            status_code=400,
        )

    except Exception as e:
        return HTMLResponse(
            "<div class='card err'><h3>❌ Erreur import</h3>"
            f"<pre class='pre'>{str(e)}</pre></div>",
            status_code=400
        )

# ================= EDS =================
@router.get("/ui/eds", response_class=HTMLResponse)
async def eds_page(request: Request):
    # On affiche les tables du dossier EDS effectif (déduit via last_run.json si besoin)
    base_eds = _effective_eds_dir()
    return templates.TemplateResponse(
        "eds.html",
        {"request": request, "tables": list_parquets(base_eds), "eds_dir": base_eds}
    )


@router.get("/ui/eds/preview", response_class=HTMLResponse)
async def eds_preview(table: str, limit: int = 50):
    # Preview dans le dossier EDS effectif
    base_eds = _effective_eds_dir()
    path = os.path.join(base_eds, table)
    if not os.path.exists(path):
        return HTMLResponse("<div class='card'>❌ Table introuvable</div>", status_code=404)

    df = pl.read_parquet(path).head(limit)

    cols = df.columns
    rows = df.to_dicts()

    thead = "".join(f"<th>{c}</th>" for c in cols)
    tbody = "".join(
        "<tr>" + "".join(f"<td>{r.get(c,'')}</td>" for c in cols) + "</tr>"
        for r in rows
    )

    return HTMLResponse(
        f"""
        <div class="card">
          <div class="muted"><b>{table}</b> — preview ({min(limit, df.height)} lignes)</div>
          <div class="table-wrap" style="margin-top:10px;">
            <table class="table">
              <thead><tr>{thead}</tr></thead>
              <tbody>{tbody}</tbody>
            </table>
          </div>
        </div>
        """
    )


# ================= STATS =================
@router.get("/ui/stats", response_class=HTMLResponse)
async def stats_page(request: Request):
    return templates.TemplateResponse("stats.html", {"request": request})


@router.get("/ui/stats/data", response_class=HTMLResponse)
async def stats_data():
    # ✅ Toujours calculer les stats sur le dossier EDS effectif (déduit via last_run.json si besoin)
    base_eds = _effective_eds_dir()

    if not os.path.isdir(base_eds):
        return HTMLResponse("<div class='card err'><h3>❌ Dossier EDS introuvable</h3></div>", status_code=404)

    tables = list_parquets(base_eds)
    if not tables:
        return HTMLResponse("<div class='card err'><h3>❌ Aucune table parquet trouvée</h3></div>", status_code=404)

    # KPI par table + total
    total_rows = 0
    kpis = ""
    table_rows = []

    for t in tables:
        df = pl.read_parquet(os.path.join(base_eds, t))
        rows = df.height
        cols = len(df.columns)
        total_rows += rows
        table_rows.append((t, rows, cols))

        kpis += f"""
        <div class="kpi">
          <div class="kpi-title">{t}</div>
          <div class="kpi-value">{rows}</div>
          <div class="kpi-sub">{cols} colonnes</div>
        </div>
        """

    header = f"""
    <div class="grid">
      <div class="kpi">
        <div class="kpi-title">Total lignes (toutes tables)</div>
        <div class="kpi-value">{total_rows}</div>
        <div class="kpi-sub">{len(tables)} tables</div>
      </div>
      {kpis}
    </div>
    """

    # Table récap
    rows_html = ""
    for t, r, c in table_rows:
        rows_html += f"<tr><td>{t}</td><td>{r}</td><td>{c}</td></tr>"

    recap = f"""
    <div class="card">
      <div class="section-title">🧾 Récapitulatif</div>
      <div class="table-wrap">
        <table class="table">
          <thead><tr><th>Table</th><th>Lignes</th><th>Colonnes</th></tr></thead>
          <tbody>{rows_html}</tbody>
        </table>
      </div>
      <div class="muted" style="margin-top:10px;">EDS utilisé : <code>{base_eds}</code></div>
    </div>
    """

    # ✅ last_run.json : on le lit dans REPORTS_DIR (même endroit que CLI/API)
    last_run = load_last_run()
    if isinstance(last_run, dict):
        report_path = os.path.join(REPORTS_DIR, "last_run.json")
        report_block = f"""
        <div class="card">
          <div class="section-title">🧾 last_run.json <span class="badge">dernier traitement</span></div>
          <div class="muted">Source : <code>{report_path}</code></div>
          <pre class="pre">{json.dumps(last_run, ensure_ascii=False, indent=2)}</pre>
        </div>
        """
    else:
        report_block = f"""
        <div class="card">
          <div class="section-title">🧾 last_run.json</div>
          <div class="muted">Aucun rapport import disponible dans <code>{REPORTS_DIR}</code>.</div>
        </div>
        """

    return HTMLResponse(header + recap + report_block)


@router.get("/ui/home/data", response_class=HTMLResponse)
async def ui_home_data():
    # ✅ Toujours calculer les infos home sur le dossier EDS effectif (déduit via last_run.json si besoin)
    base_eds = _effective_eds_dir()

    if not os.path.isdir(base_eds):
        return HTMLResponse("<div class='card'>EDS introuvable.</div>", status_code=404)

    tables = list_parquets(base_eds)
    if not tables:
        return HTMLResponse("<div class='card'>Aucune table parquet trouvée.</div>", status_code=404)

    # stats par table
    detail_rows = []
    total_rows = 0
    for t in tables:
        df = pl.read_parquet(os.path.join(base_eds, t))
        r = df.height
        c = len(df.columns)
        total_rows += r
        detail_rows.append((t, r, c))

    # ✅ last_run.json : on le lit dans REPORTS_DIR (même endroit que CLI/API)
    last_run = load_last_run()

    # fichiers traités (si dispo)
    files_count = None
    merge_report = None
    if isinstance(last_run, dict):
        files_count = last_run.get("files_processed") or last_run.get("files") or last_run.get("processed_files")
        merge_report = last_run.get("merge_report") or last_run.get("merge") or last_run.get("merge_stats")

    # KPI
    kpi_html = f"""
    <div class="grid">
      <div class="kpi">
        <div class="kpi-title">Tables EDS</div>
        <div class="kpi-value">{len(tables)}</div>
        <div class="kpi-sub">parquets détectés</div>
      </div>
      <div class="kpi">
        <div class="kpi-title">Total lignes (toutes tables)</div>
        <div class="kpi-value">{total_rows}</div>
        <div class="kpi-sub">somme des lignes</div>
      </div>
      <div class="kpi">
        <div class="kpi-title">OK – rapport disponible</div>
        <div class="kpi-value">{'Oui' if isinstance(last_run, dict) else 'Non'}</div>
        <div class="kpi-sub">last_run.json</div>
      </div>
      <div class="kpi">
        <div class="kpi-title">Fichiers traités</div>
        <div class="kpi-value">{files_count if files_count is not None else '-'}</div>
        <div class="kpi-sub">si disponible</div>
      </div>
    </div>
    """

    # Table détail par table
    detail_tr = "".join(
        f"<tr><td>{t}</td><td>{r}</td><td>{c}</td></tr>"
        for t, r, c in detail_rows
    )
    detail_table = f"""
    <div class="card">
      <div class="muted" style="margin-bottom:10px;"><b>Détail par table</b></div>
      <div class="table-wrap">
        <table class="table">
          <thead><tr><th>table</th><th>rows</th><th>cols</th></tr></thead>
          <tbody>{detail_tr}</tbody>
        </table>
      </div>
      <div class="muted" style="margin-top:10px;">EDS utilisé : <code>{base_eds}</code></div>
    </div>
    """

    # Merge report (si présent)
    merge_html = ""
    if isinstance(merge_report, list) and len(merge_report) > 0:
        mtr = ""
        for row in merge_report:
            table = row.get("table", "")
            before_rows = row.get("before_rows", row.get("before", ""))
            incoming_rows = row.get("incoming_rows", row.get("incoming", ""))
            after_rows = row.get("after_rows", row.get("after", ""))
            added_rows = row.get("added_rows", row.get("added", ""))
            mtr += f"<tr><td>{table}</td><td>{before_rows}</td><td>{incoming_rows}</td><td>{after_rows}</td><td>{added_rows}</td></tr>"

        merge_html = f"""
        <div class="card">
          <div class="muted" style="margin-bottom:10px;"><b>Merge report</b></div>
          <div class="table-wrap">
            <table class="table">
              <thead>
                <tr>
                  <th>table</th><th>before_rows</th><th>incoming_rows</th><th>after_rows</th><th>added_rows</th>
                </tr>
              </thead>
              <tbody>{mtr}</tbody>
            </table>
          </div>

          <details style="margin-top:10px;">
            <summary>Voir JSON complet</summary>
            <pre class="pre">{json.dumps(last_run, ensure_ascii=False, indent=2) if isinstance(last_run, dict) else ""}</pre>
          </details>
        </div>
        """
    else:
        merge_html = f"""
        <div class="card">
          <div class="muted"><b>Merge report</b></div>
          <div class="muted">Aucun merge_report détecté dans last_run.json.</div>
          <details style="margin-top:10px;">
            <summary>Voir JSON complet</summary>
            <pre class="pre">{json.dumps(last_run, ensure_ascii=False, indent=2) if isinstance(last_run, dict) else ""}</pre>
          </details>
        </div>
        """

    # Raccourcis
    shortcuts = """
    <div class="card">
      <div class="muted" style="margin-bottom:10px;"><b>Raccourcis</b></div>
      <div class="shortcuts">
        <a class="shortcut" href="/ui/import"><div class="title">Importer un FHIR</div><div class="icon">⬆️</div></a>
        <a class="shortcut" href="/ui/convert"><div class="title">Convertir un dossier</div><div class="icon">📁</div></a>
        <a class="shortcut" href="/ui/eds"><div class="title">Explorer l’EDS</div><div class="icon">🔎</div></a>
        <a class="shortcut" href="/ui/export"><div class="title">Exporter ZIP</div><div class="icon">📦</div></a>
      </div>
    </div>
    """

    # Layout split (deux colonnes)
    split = f"""
    <div class="split">
      {detail_table}
      {merge_html}
    </div>
    """

    return HTMLResponse(kpi_html + split + shortcuts)


@router.get("/ui/convert", response_class=HTMLResponse)
async def ui_convert(request: Request):
    return templates.TemplateResponse("convert.html", {"request": request})


@router.post("/ui/convert/run", response_class=HTMLResponse)
async def ui_convert_run(request: Request):
    form = await request.form()
    fhir_dir = (form.get("fhir_dir") or "").strip()

    if not fhir_dir:
        # défaut compatible avec ton projet (tu peux ajuster)
        project_root = Path(__file__).resolve().parents[2]
        fhir_dir = str(project_root / "synthea" / "output" / "fhir")

    try:
        res = process_dir(fhir_dir)
        return HTMLResponse(
            "<div class='card'><b>✅ Conversion terminée</b>"
            f"<div class='muted'>Dossier : <code>{fhir_dir}</code></div>"
            f"<pre class='pre'>{json.dumps(res, ensure_ascii=False, indent=2)}</pre></div>"
        )
    except Exception as e:
        return HTMLResponse(
            "<div class='card'><b>❌ Erreur</b>"
            f"<pre class='pre'>{str(e)}</pre></div>",
            status_code=400
        )


@router.get("/ui/export", response_class=HTMLResponse)
async def ui_export(request: Request):
    return templates.TemplateResponse("export.html", {"request": request})


@router.get("/ui/export/download")
async def ui_export_download():
    # ✅ Export ZIP du dossier EDS effectif
    base_eds = _effective_eds_dir()

    if not os.path.isdir(base_eds):
        raise HTTPException(status_code=404, detail="EDS introuvable")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for f in os.listdir(base_eds):
            # ✅ uniquement les parquets
            if f.endswith(".parquet"):
                z.write(os.path.join(base_eds, f), arcname=f)

    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=eds_export.zip"}
    )


@router.get("/ui/eds/meta", response_class=HTMLResponse)
async def eds_meta(table: str, limit: int = 50):
    # Meta dans le dossier EDS effectif
    base_eds = _effective_eds_dir()
    path = os.path.join(base_eds, table)
    if not os.path.exists(path):
        return HTMLResponse("<div class='card'>❌ Table introuvable</div>", status_code=404)

    df = pl.read_parquet(path)
    return HTMLResponse(
        "<div class='card'>"
        f"<div class='muted'><b>{table}</b></div>"
        f"<div style='margin-top:8px;'>Lignes : <b>{df.height}</b> — Colonnes : <b>{len(df.columns)}</b></div>"
        "</div>"
    )

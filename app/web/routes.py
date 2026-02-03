from fastapi import APIRouter, Request, UploadFile, File, HTTPException, Body
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import os, json
import polars as pl
from pathlib import Path
from app.core.converters.fhir_to_edsan import process_dir  
from app.core.converters.fhir_to_edsan import process_bundle
from app.core.converters.edsan_to_fhir import export_eds_to_fhir
import tempfile
from fastapi.responses import StreamingResponse
import io, zipfile
from pydantic import BaseModel
from typing import Optional
import requests
from zipfile import ZipFile, ZIP_DEFLATED
 
 
 
router = APIRouter()
templates = Jinja2Templates(directory="app/web/templates")
 
def merged_cfg(payload: dict) -> dict:
    def pick(key: str, default: str | None = None):
        v = payload.get(key)
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return os.getenv(key, default)
        return v
 
    return {
        "EDS_DIR": pick("EDS_DIR"),
        "FHIR_OUTPUT_DIR": pick("FHIR_OUTPUT_DIR"),
        "FHIR_EXPORT_DIR": pick("FHIR_EXPORT_DIR"),
        "FHIR_BUNDLE_STRATEGY": pick("FHIR_BUNDLE_STRATEGY", "patient"),
    }
 
def list_parquets():
    if not os.path.isdir(EDS_DIR):
        return []
    return sorted([f for f in os.listdir(EDS_DIR) if f.endswith(".parquet")])
 
 
# ================== DASHBOARD =================
@router.get("/ui", response_class=HTMLResponse)
async def ui_home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})
 
 
 
# ================= IMPORT FHIR =================
@router.get("/ui/import", response_class=HTMLResponse)
async def import_page(request: Request):
    return templates.TemplateResponse("import.html", {"request": request})
 
 
@router.post("/ui/import/run", response_class=HTMLResponse)
async def import_run(file: UploadFile = File(...)):
    try:
        bundle = json.loads((await file.read()).decode("utf-8"))
        res = process_bundle(bundle)
        return HTMLResponse(
            "<div class='card ok'><h3>✅ Import réussi</h3>"
            f"<pre class='pre'>{json.dumps(res, indent=2, ensure_ascii=False)}</pre></div>"
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
    return templates.TemplateResponse(
        "eds.html",
        {"request": request, "tables": list_parquets()}
    )
 
 
 
   
   
@router.get("/ui/eds/preview", response_class=HTMLResponse)
async def eds_preview(table: str, limit: int = 50):
    path = os.path.join(EDS_DIR, table)
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
    if not os.path.isdir(EDS_DIR):
        return HTMLResponse("<div class='card err'><h3>❌ Dossier EDS introuvable</h3></div>", status_code=404)
 
    tables = list_parquets()
    if not tables:
        return HTMLResponse("<div class='card err'><h3>❌ Aucune table parquet trouvée</h3></div>", status_code=404)
 
    # KPI par table + total
    total_rows = 0
    kpis = ""
    table_rows = []
 
    for t in tables:
        df = pl.read_parquet(os.path.join(EDS_DIR, t))
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
    </div>
    """
 
    # last_run.json
    report_path = os.path.join(EDS_DIR, "last_run.json")
    report_block = ""
    if os.path.exists(report_path):
        with open(report_path, "r", encoding="utf-8") as f:
            last_run = json.load(f)
        report_block = f"""
        <div class="card">
          <div class="section-title">🧾 last_run.json <span class="badge">dernier traitement</span></div>
          <pre class="pre">{json.dumps(last_run, ensure_ascii=False, indent=2)}</pre>
        </div>
        """
    else:
        report_block = """
        <div class="card">
          <div class="section-title">🧾 last_run.json</div>
          <div class="muted">Aucun fichier last_run.json trouvé dans le dossier EDS.</div>
        </div>
        """
 
    return HTMLResponse(header + recap + report_block)
 
@router.get("/ui/home/data", response_class=HTMLResponse)
async def ui_home_data():
    if not os.path.isdir(EDS_DIR):
        return HTMLResponse("<div class='card'>EDS introuvable.</div>", status_code=404)
 
    tables = list_parquets()
    if not tables:
        return HTMLResponse("<div class='card'>Aucune table parquet trouvée.</div>", status_code=404)
 
    # stats par table
    detail_rows = []
    total_rows = 0
    for t in tables:
        df = pl.read_parquet(os.path.join(EDS_DIR, t))
        r = df.height
        c = len(df.columns)
        total_rows += r
        detail_rows.append((t, r, c))
 
    # last_run.json
    report_path = os.path.join(EDS_DIR, "last_run.json")
    last_run = None
    if os.path.exists(report_path):
        with open(report_path, "r", encoding="utf-8") as f:
            last_run = json.load(f)
 
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
        <div class="kpi-value">{'Oui' if last_run else 'Non'}</div>
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
            <pre class="pre">{json.dumps(last_run, ensure_ascii=False, indent=2) if last_run else ""}</pre>
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
            <pre class="pre">{json.dumps(last_run, ensure_ascii=False, indent=2) if last_run else ""}</pre>
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
    if not os.path.isdir(EDS_DIR):
        raise HTTPException(status_code=404, detail="EDS introuvable")
 
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for f in os.listdir(EDS_DIR):
            # ✅ uniquement les parquets
            if f.endswith(".parquet"):
                z.write(os.path.join(EDS_DIR, f), arcname=f)
 
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=eds_export.zip"}
    )
 
@router.get("/ui/eds/meta", response_class=HTMLResponse)
async def eds_meta(table: str, limit: int = 50):
    path = os.path.join(EDS_DIR, table)
    if not os.path.exists(path):
        return HTMLResponse("<div class='card'>❌ Table introuvable</div>", status_code=404)
 
    df = pl.read_parquet(path)
    return HTMLResponse(
        "<div class='card'>"
        f"<div class='muted'><b>{table}</b></div>"
        f"<div style='margin-top:8px;'>Lignes : <b>{df.height}</b> — Colonnes : <b>{len(df.columns)}</b></div>"
        "</div>"
    )
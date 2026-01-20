# -*- coding: utf-8 -*-
# ============================================================
# Painel de Produção por Vistoriador — MULTI-MESES (modelo Qualidade)
# ============================================================

import os
import io
import re
import json
import unicodedata
from datetime import datetime, date
from typing import Optional, Tuple, Dict, List

import streamlit as st
import pandas as pd
import numpy as np

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Drive API (se algum mês vier como XLSX no índice, já fica preparado)
from google.oauth2 import service_account as gcreds
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ------------------ CONFIG BÁSICA ------------------
st.set_page_config(page_title="Painel de Produção por Vistoriador", layout="wide")
st.title("Painel de Produção por Vistoriador")

st.markdown(
    """
<style>
.card-wrap{display:flex;gap:16px;flex-wrap:wrap;margin:12px 0 6px;}
.card{background:#f7f7f9;border-radius:12px;box-shadow:0 1px 4px rgba(0,0,0,.06);padding:14px 16px;min-width:200px;flex:1;text-align:center}
.card h4{margin:0 0 6px;font-size:14px;color:#0f355a;font-weight:800}
.card h2{margin:0;font-size:26px;font-weight:900;color:#222}
.card .sub{margin-top:8px;display:inline-block;padding:6px 10px;border-radius:8px;font-size:12px;font-weight:800}
.sub.ok{background:#e8f5ec;color:#197a31;border:1px solid #cce9d4}
.sub.bad{background:#fdeaea;color:#a31616;border:1px solid #f2cccc}
.sub.neu{background:#f1f1f4;color:#444;border:1px solid #e4e4e8}
.section{font-size:18px;font-weight:900;margin:22px 0 8px}
.small{color:#666;font-size:13px}
.table-note{margin-top:8px;color:#666;font-size:12px}
</style>
""",
    unsafe_allow_html=True,
)

fast_mode = st.toggle("Modo rápido (pular tabelas pesadas)", value=False)


# ------------------ CREDENCIAL ------------------
def _get_client_and_drive():
    try:
        block = st.secrets["gcp_service_account"]
    except Exception:
        st.error("Não encontrei [gcp_service_account] no secrets.toml.")
        st.stop()

    if "json_path" in block:
        path = block["json_path"]
        if not os.path.isabs(path):
            path = os.path.join(os.path.dirname(__file__), path)
        try:
            with open(path, "r", encoding="utf-8") as f:
                info = json.load(f)
        except Exception as e:
            st.error(f"Não consegui abrir o JSON da service account: {path}")
            with st.expander("Detalhes"):
                st.exception(e)
            st.stop()
    else:
        info = dict(block)

    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scopes)
    gc = gspread.authorize(creds)

    dscopes = ["https://www.googleapis.com/auth/drive.readonly"]
    gcred = gcreds.Credentials.from_service_account_info(info, scopes=dscopes)
    drive = build("drive", "v3", credentials=gcred, cache_discovery=False)

    return gc, drive


client, DRIVE = _get_client_and_drive()


# ------------------ SECRETS: IDs ------------------
PROD_INDEX_ID = st.secrets.get("prod_index_sheet_id", "").strip()
if not PROD_INDEX_ID:
    st.error("Faltou `prod_index_sheet_id` no secrets.toml")
    st.stop()


# ------------------ HELPERS ------------------
ID_RE = re.compile(r"/d/([a-zA-Z0-9-_]+)")

def _sheet_id(s: str) -> Optional[str]:
    s = (s or "").strip()
    m = ID_RE.search(s)
    if m:
        return m.group(1)
    return s if re.fullmatch(r"[A-Za-z0-9-_]{20,}", s) else None

def _ym_token(x: str) -> Optional[str]:
    """Converte 'MM/AAAA' -> 'AAAA-MM'."""
    if not x:
        return None
    s = str(x).strip()
    if re.fullmatch(r"\d{2}/\d{4}", s):
        mm, yy = s.split("/")
        return f"{yy}-{int(mm):02d}"
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s
    return None

def parse_date_any(x):
    if pd.isna(x) or x == "":
        return pd.NaT
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        try:
            return (pd.to_datetime("1899-12-30") + pd.to_timedelta(int(x), unit="D")).date()
        except Exception:
            pass
    s = str(x).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        return pd.to_datetime(s).date()
    except Exception:
        return pd.NaT

def _upper(x):
    return str(x).upper().strip() if pd.notna(x) else ""

def _yes(v) -> bool:
    return str(v).strip().upper() in {"S", "SIM", "Y", "YES", "TRUE", "1"}

def _strip_accents(s: str) -> str:
    if s is None:
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(ch))

def _find_col(cols, *names) -> Optional[str]:
    """Encontra a coluna em 'cols' ignorando acentos/maiúsculas/espaços."""
    norm = {re.sub(r"\W+", "", _strip_accents(c).upper()): c for c in cols}
    for nm in names:
        key = re.sub(r"\W+", "", _strip_accents(nm).upper())
        if key in norm:
            return norm[key]
    return None

def _fmt_int(x) -> str:
    try:
        return f"{int(x):,}".replace(",", ".")
    except Exception:
        return "0"

def _fmt_mes(ym: str) -> str:
    # ym = "AAAA-MM" -> "MM/AAAA"
    return f"{ym[5:7]}/{ym[:4]}"


# ------------------ LEITURA DO ÍNDICE ------------------
@st.cache_data(ttl=300, show_spinner=False)
def read_index(sheet_id: str, tab: str = "ARQUIVOS") -> pd.DataFrame:
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(tab)
    rows = ws.get_all_records()
    if not rows:
        return pd.DataFrame(columns=["URL", "MÊS", "ATIVO"])
    df = pd.DataFrame(rows)
    df.columns = [c.strip().upper() for c in df.columns]
    for need in ["URL", "MÊS", "ATIVO"]:
        if need not in df.columns:
            df[need] = ""
    return df


# ------------------ DRIVE: fallback para XLSX ------------------
@st.cache_data(ttl=300, show_spinner=False)
def _drive_get_file_metadata(file_id: str) -> dict:
    return DRIVE.files().get(fileId=file_id, fields="id, name, mimeType").execute()

@st.cache_data(ttl=300, show_spinner=False)
def _drive_download_bytes(file_id: str) -> bytes:
    req = DRIVE.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


# ------------------ LEITURA / PRODUÇÃO + METAS ------------------
@st.cache_data(ttl=300, show_spinner=False)
def read_prod_month(month_sheet_id: str, ym: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Produção:
    - Cada linha = 1 vistoria
    - Revistoria = mesma UNIDADE + mesmo CHASSI a partir da 2ª ocorrência (no mês)
    - IS_REV = 1 (rev) / 0 (principal)
    Metas:
    - Aba 'METAS' (se existir)
    - Espera colunas VISTORIADOR, UNIDADE, META_MENSAL e opcional TIPO
    """
    meta = _drive_get_file_metadata(month_sheet_id)
    title = meta.get("name", month_sheet_id)
    mime = meta.get("mimeType", "")

    # ----- lê produção (aba 1) -----
    if mime == "application/vnd.google-apps.spreadsheet":
        sh = client.open_by_key(month_sheet_id)
        ws = sh.sheet1
        df = pd.DataFrame(ws.get_all_records())
    else:
        # XLSX
        content = _drive_download_bytes(month_sheet_id)
        df = pd.read_excel(io.BytesIO(content), sheet_name=0, engine="openpyxl")

    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), title

    df.columns = [str(c).strip().upper() for c in df.columns]

    col_unid = "UNIDADE" if "UNIDADE" in df.columns else None
    col_data = "DATA" if "DATA" in df.columns else None
    col_chas = "CHASSI" if "CHASSI" in df.columns else None
    col_per  = "PERITO" if "PERITO" in df.columns else None
    col_dig  = "DIGITADOR" if "DIGITADOR" in df.columns else None

    req = [col_unid, col_data, col_chas, (col_per or col_dig)]
    if any(r is None for r in req):
        # estrutura do mês não bate com o padrão
        return pd.DataFrame(), pd.DataFrame(), title

    # normalização
    df[col_unid] = df[col_unid].map(_upper)
    df["__DATA__"] = df[col_data].apply(parse_date_any)
    df[col_chas] = df[col_chas].map(_upper)

    # vistoriador
    if col_per and col_dig:
        df["VISTORIADOR"] = np.where(
            df[col_per].astype(str).str.strip() != "",
            df[col_per].map(_upper),
            df[col_dig].map(_upper),
        )
    elif col_per:
        df["VISTORIADOR"] = df[col_per].map(_upper)
    else:
        df["VISTORIADOR"] = df[col_dig].map(_upper)

    # limpa
    df = df[
        df["__DATA__"].notna() &
        df[col_chas].astype(str).str.strip().ne("") &
        df[col_unid].astype(str).str.strip().ne("") &
        df["VISTORIADOR"].astype(str).str.strip().ne("")
    ].copy()

    # revistoria por UNIDADE + CHASSI
    df = df.sort_values(["__DATA__", col_unid, col_chas], kind="mergesort").reset_index(drop=True)
    df["__ORD__"] = df.groupby([col_unid, col_chas]).cumcount()
    df["IS_REV"] = (df["__ORD__"] >= 1).astype(int)

    # ----- lê metas -----
    metas = pd.DataFrame()
    try:
        if mime == "application/vnd.google-apps.spreadsheet":
            sh = client.open_by_key(month_sheet_id)
            ws_meta = sh.worksheet("METAS")
            rows = ws_meta.get_all_records()
            dm = pd.DataFrame(rows) if rows else pd.DataFrame()
        else:
            content = _drive_download_bytes(month_sheet_id)
            dm = pd.read_excel(io.BytesIO(content), sheet_name="METAS", engine="openpyxl")

        if not dm.empty:
            cols = list(dm.columns)
            c_vist = _find_col(cols, "VISTORIADOR")
            c_unid = _find_col(cols, "UNIDADE", "CIDADE")
            c_meta = _find_col(cols, "META_MENSAL", "META MENSAL", "META")
            c_tipo = _find_col(cols, "TIPO", "PERFIL")

            out = pd.DataFrame()
            out["VISTORIADOR"] = dm[c_vist].astype(str).map(_upper) if c_vist else ""
            out["UNIDADE"] = dm[c_unid].astype(str).map(_upper) if c_unid else ""
            out["META_MENSAL"] = pd.to_numeric(dm[c_meta], errors="coerce").fillna(0).astype(int) if c_meta else 0
            out["TIPO"] = dm[c_tipo].astype(str).map(_upper) if c_tipo else ""
            out["YM"] = ym or ""
            metas = out
    except Exception:
        metas = pd.DataFrame()

    return df, metas, title


# ------------------ CARREGA MESES ------------------
idx_p = read_index(PROD_INDEX_ID)
if "ATIVO" in idx_p.columns:
    idx_p = idx_p[idx_p["ATIVO"].map(_yes)].copy()

# mantém apenas linhas com mês válido
idx_p["YM"] = idx_p["MÊS"].map(_ym_token)
idx_p = idx_p[idx_p["YM"].notna()].copy()

if idx_p.empty:
    st.error("Índice de Produção (ARQUIVOS) sem meses válidos/ativos.")
    st.stop()

# ordena por YM
idx_p = idx_p.sort_values("YM").reset_index(drop=True)

# lê todos os meses ativos
dp_all, metas_all = [], []
ok_p, er_p = [], []

for _, r in idx_p.iterrows():
    sid = _sheet_id(r["URL"])
    ym = r["YM"]
    if not sid:
        continue
    try:
        dp, dm, ttl = read_prod_month(sid, ym=ym)
        if not dp.empty:
            dp["YM"] = ym
            dp_all.append(dp)
        if not dm.empty:
            metas_all.append(dm)
        ok_p.append(f"{ttl} ({ym})")
    except Exception as e:
        er_p.append((sid, e))

if not dp_all:
    st.error("Não consegui ler Produção de nenhum mês ativo.")
    st.stop()

dfP = pd.concat(dp_all, ignore_index=True)
dfMetas = pd.concat(metas_all, ignore_index=True) if metas_all else pd.DataFrame(
    columns=["VISTORIADOR", "UNIDADE", "META_MENSAL", "TIPO", "YM"]
)

# meses disponíveis
ym_all = sorted(dfP["YM"].dropna().unique().tolist())
label_map = {_fmt_mes(m): m for m in ym_all}
sel_label = st.selectbox("Mês de referência", options=list(label_map.keys()), index=len(ym_all) - 1)
ym_sel = label_map[sel_label]

# ------------------ FILTROS (unidade / vistoriador) ------------------
# base do mês selecionado
viewP_mes = dfP[dfP["YM"] == ym_sel].copy()

# lista de unidades e vists do mês
unids = sorted(viewP_mes["UNIDADE"].dropna().unique().tolist()) if "UNIDADE" in viewP_mes.columns else []
vists = sorted(viewP_mes["VISTORIADOR"].dropna().unique().tolist()) if "VISTORIADOR" in viewP_mes.columns else []

c1, c2 = st.columns(2)
with c1:
    f_unids = st.multiselect("Unidades (opcional)", options=unids, default=unids)
with c2:
    f_vists = st.multiselect("Vistoriadores (opcional)", options=vists, default=[])

# aplica filtros no mês selecionado
if f_unids and "UNIDADE" in viewP_mes.columns:
    viewP_mes = viewP_mes[viewP_mes["UNIDADE"].isin([_upper(u) for u in f_unids])].copy()
if f_vists and "VISTORIADOR" in viewP_mes.columns:
    viewP_mes = viewP_mes[viewP_mes["VISTORIADOR"].isin([_upper(v) for v in f_vists])].copy()

# ------------------ AGREGA PRODUÇÃO (mês) ------------------
def _make_prod(df_prod: pd.DataFrame) -> pd.DataFrame:
    if df_prod.empty:
        return pd.DataFrame(columns=["VISTORIADOR", "UNIDADE", "vist", "rev", "liq"])
    out = (
        df_prod.groupby(["VISTORIADOR", "UNIDADE"], dropna=False)
               .agg(vist=("IS_REV", "size"), rev=("IS_REV", "sum"))
               .reset_index()
    )
    out["liq"] = out["vist"] - out["rev"]
    return out

prod_mes = _make_prod(viewP_mes)

# metas do mês selecionado (se existir)
metas_mes = dfMetas[dfMetas["YM"].astype(str) == ym_sel].copy() if "YM" in dfMetas.columns else dfMetas.copy()
if not metas_mes.empty:
    metas_mes["VISTORIADOR"] = metas_mes["VISTORIADOR"].astype(str).map(_upper)
    metas_mes["UNIDADE"] = metas_mes["UNIDADE"].astype(str).map(_upper)

# junta produção x meta
base_mes = prod_mes.merge(
    metas_mes[["VISTORIADOR", "UNIDADE", "META_MENSAL", "TIPO"]] if not metas_mes.empty else
    pd.DataFrame(columns=["VISTORIADOR", "UNIDADE", "META_MENSAL", "TIPO"]),
    on=["VISTORIADOR", "UNIDADE"],
    how="left",
)

base_mes["META_MENSAL"] = pd.to_numeric(base_mes.get("META_MENSAL", 0), errors="coerce").fillna(0).astype(int)
base_mes["FALTANTE"] = (base_mes["META_MENSAL"] - base_mes["liq"]).clip(lower=0).astype(int)
base_mes["BATEU"] = base_mes["liq"] >= base_mes["META_MENSAL"]
base_mes["TIPO"] = base_mes.get("TIPO", "").fillna("").astype(str).map(_upper)

# ------------------ CARDS (mês) ------------------
total_vist = int(prod_mes["vist"].sum()) if not prod_mes.empty else 0
total_rev = int(prod_mes["rev"].sum()) if not prod_mes.empty else 0
total_liq = int(prod_mes["liq"].sum()) if not prod_mes.empty else 0

qtd_vists = int(prod_mes["VISTORIADOR"].nunique()) if not prod_mes.empty else 0

qtd_nao_bateu = int((base_mes["BATEU"] == False).sum()) if not base_mes.empty else 0
qtd_bateu = int((base_mes["BATEU"] == True).sum()) if not base_mes.empty else 0

cards_html = f"""
<div class="card-wrap">
  <div class='card'>
    <h4>Total bruto (mês)</h4>
    <h2>{_fmt_int(total_vist)}</h2>
    <span class='sub neu'>vistorias brutas</span>
  </div>
  <div class='card'>
    <h4>Total revistorias (mês)</h4>
    <h2>{_fmt_int(total_rev)}</h2>
    <span class='sub neu'>rev</span>
  </div>
  <div class='card'>
    <h4>Total líquido (mês)</h4>
    <h2>{_fmt_int(total_liq)}</h2>
    <span class='sub neu'>vist - rev</span>
  </div>
  <div class='card'>
    <h4>Vistoriadores no recorte</h4>
    <h2>{_fmt_int(qtd_vists)}</h2>
  </div>
  <div class='card'>
    <h4>Bateram meta</h4>
    <h2>{_fmt_int(qtd_bateu)}</h2>
    <span class='sub ok'>no mês selecionado</span>
  </div>
  <div class='card'>
    <h4>Não bateram meta</h4>
    <h2>{_fmt_int(qtd_nao_bateu)}</h2>
    <span class='sub bad'>no mês selecionado</span>
  </div>
</div>
"""
st.markdown(cards_html, unsafe_allow_html=True)

# ------------------ RESUMO DO MÊS (vista rápida) ------------------
st.markdown('<div class="section">Resumo por Vistoriador (mês selecionado)</div>', unsafe_allow_html=True)

if base_mes.empty:
    st.info("Sem produção no mês selecionado após filtros.")
    st.stop()

view_mes = base_mes.copy()
view_mes["STATUS"] = np.where(view_mes["BATEU"], "BATEU", "NÃO BATEU")

# ordena: quem não bateu primeiro, depois faltante desc
view_mes = view_mes.sort_values(["BATEU", "FALTANTE"], ascending=[True, False]).reset_index(drop=True)

cols_resumo = ["UNIDADE", "VISTORIADOR", "TIPO", "META_MENSAL", "liq", "FALTANTE", "STATUS"]
for c in cols_resumo:
    if c not in view_mes.columns:
        view_mes[c] = ""

tmp = view_mes[cols_resumo].rename(columns={"liq": "LIQUIDO"}).copy()
st.dataframe(tmp, use_container_width=True, hide_index=True)

# ------------------ HISTÓRICO VISUAL (MODELO QUALIDADE) ------------------
st.markdown("---")
st.markdown('<div class="section">Histórico de Meta (quem não bateu no mês selecionado)</div>', unsafe_allow_html=True)

# conjunto de vistoriadores-alvo: todos que NÃO bateram no mês selecionado (após filtros)
alvo = base_mes[base_mes["BATEU"] == False].copy()
if alvo.empty:
    st.info("No recorte atual, ninguém ficou abaixo da meta no mês selecionado.")
    st.stop()

# se o filtro de vistoriador foi preenchido, restringe o alvo (já está no base_mes)
alvo_names = sorted(alvo["VISTORIADOR"].unique().tolist())

# meses para mostrar: todos até o mês selecionado
idx_cur = ym_all.index(ym_sel) if ym_sel in ym_all else len(ym_all) - 1
meses_janela = ym_all[: idx_cur + 1]  # do primeiro até o selecionado

# preagrega produção e meta por mês (para performance)
@st.cache_data(ttl=300, show_spinner=False)
def build_month_maps(dfP_all: pd.DataFrame, dfM_all: pd.DataFrame) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    prod_map = {}
    meta_map = {}
    # produção por mês: VISTORIADOR+UNIDADE -> LIQ
    for ym in sorted(dfP_all["YM"].dropna().unique().tolist()):
        p = dfP_all[dfP_all["YM"] == ym].copy()
        if p.empty:
            prod_map[ym] = pd.DataFrame(columns=["VISTORIADOR", "UNIDADE", "liq"])
            continue
        pm = _make_prod(p)[["VISTORIADOR", "UNIDADE", "liq"]].copy()
        prod_map[ym] = pm

    # metas por mês: VISTORIADOR+UNIDADE -> META, TIPO
    if dfM_all is None or dfM_all.empty or "YM" not in dfM_all.columns:
        for ym in sorted(dfP_all["YM"].dropna().unique().tolist()):
            meta_map[ym] = pd.DataFrame(columns=["VISTORIADOR", "UNIDADE", "META_MENSAL", "TIPO"])
    else:
        dfM_all = dfM_all.copy()
        dfM_all["VISTORIADOR"] = dfM_all["VISTORIADOR"].astype(str).map(_upper)
        dfM_all["UNIDADE"] = dfM_all["UNIDADE"].astype(str).map(_upper)
        dfM_all["TIPO"] = dfM_all.get("TIPO", "").fillna("").astype(str).map(_upper)
        for ym in sorted(dfP_all["YM"].dropna().unique().tolist()):
            mm = dfM_all[dfM_all["YM"].astype(str) == ym][["VISTORIADOR", "UNIDADE", "META_MENSAL", "TIPO"]].copy()
            if "META_MENSAL" in mm.columns:
                mm["META_MENSAL"] = pd.to_numeric(mm["META_MENSAL"], errors="coerce").fillna(0).astype(int)
            meta_map[ym] = mm

    return prod_map, meta_map

prod_map, meta_map = build_month_maps(dfP, dfMetas)

# mapa vistoriador -> cidade/unidade (prioriza metas do mês selecionado; fallback: produção do mês)
city_map = {}
tipo_map = {}

# cidade/tipo do mês selecionado
try:
    mm = metas_mes.copy()
    if not mm.empty:
        mm = mm[mm["VISTORIADOR"].isin(alvo_names)].copy()
        mm = mm.drop_duplicates(subset=["VISTORIADOR"])
        if "UNIDADE" in mm.columns:
            city_map.update(dict(zip(mm["VISTORIADOR"], mm["UNIDADE"])))
        if "TIPO" in mm.columns:
            tipo_map.update(dict(zip(mm["VISTORIADOR"], mm["TIPO"])))
except Exception:
    pass

# fallback cidade pelo mês (produção)
try:
    if "UNIDADE" in viewP_mes.columns and "VISTORIADOR" in viewP_mes.columns:
        bc = viewP_mes[["VISTORIADOR", "UNIDADE"]].copy()
        bc["VISTORIADOR"] = bc["VISTORIADOR"].astype(str).map(_upper)
        bc["UNIDADE"] = bc["UNIDADE"].astype(str).map(_upper)
        bc = bc.drop_duplicates(subset=["VISTORIADOR"])
        for v, u in zip(bc["VISTORIADOR"], bc["UNIDADE"]):
            if v in alvo_names and (v not in city_map or not city_map.get(v)):
                city_map[v] = u
except Exception:
    pass

# monta tabela base
hist = pd.DataFrame({"VISTORIADOR": alvo_names})
hist["CIDADE"] = hist["VISTORIADOR"].map(city_map).fillna("")
hist["TIPO"] = hist["VISTORIADOR"].map(tipo_map).fillna("")

# função: bateu/não bateu por mês (considera meta do mês)
def _get_liq_meta(ym: str, vist: str, unid_pref: str = "") -> Tuple[Optional[int], Optional[int]]:
    # produção (pode ter mais de uma unidade; a do painel normalmente é uma)
    pm = prod_map.get(ym, pd.DataFrame(columns=["VISTORIADOR", "UNIDADE", "liq"]))
    mm = meta_map.get(ym, pd.DataFrame(columns=["VISTORIADOR", "UNIDADE", "META_MENSAL", "TIPO"]))

    # tenta casar pela unidade preferida, se existir
    liq = None
    meta = None

    if not pm.empty:
        q = pm[pm["VISTORIADOR"] == vist].copy()
        if unid_pref:
            q2 = q[q["UNIDADE"] == unid_pref]
            if not q2.empty:
                liq = int(q2["liq"].sum())
            else:
                liq = int(q["liq"].sum()) if not q.empty else None
        else:
            liq = int(q["liq"].sum()) if not q.empty else None

    if not mm.empty:
        q = mm[mm["VISTORIADOR"] == vist].copy()
        if unid_pref:
            q2 = q[q["UNIDADE"] == unid_pref]
            if not q2.empty:
                meta = int(pd.to_numeric(q2["META_MENSAL"], errors="coerce").fillna(0).sum())
            else:
                meta = int(pd.to_numeric(q["META_MENSAL"], errors="coerce").fillna(0).sum()) if not q.empty else None
        else:
            meta = int(pd.to_numeric(q["META_MENSAL"], errors="coerce").fillna(0).sum()) if not q.empty else None

    return liq, meta

def _bateu(liq: Optional[int], meta: Optional[int]) -> Optional[bool]:
    if liq is None or meta is None or meta <= 0:
        return None
    return liq >= meta

# calcula streak (meses consecutivos sem bater) terminando no mês selecionado
streaks = []
for v in hist["VISTORIADOR"].tolist():
    un = str(hist.loc[hist["VISTORIADOR"] == v, "CIDADE"].iloc[0] or "").strip().upper()
    cons = 0
    # percorre de trás (mês selecionado -> anteriores)
    for ym in reversed(meses_janela):
        liq, meta = _get_liq_meta(ym, v, unid_pref=un)
        b = _bateu(liq, meta)
        if b is None:
            # sem meta ou sem dado: interrompe a sequência (não assume falha)
            if ym == ym_sel:
                # se no mês atual não bateu, aqui não pode ser None (alvo). mas por segurança:
                pass
            break
        if b is False:
            cons += 1
        else:
            break
    streaks.append(cons)

hist["MESES_CONSECUTIVOS_SEM_META"] = streaks

def _sit(cons: int) -> str:
    if cons >= 3:
        return "3+ meses sem meta"
    if cons == 2:
        return "2 meses sem meta"
    if cons == 1:
        return "Entrou agora"
    return "—"

hist["SITUAÇÃO"] = hist["MESES_CONSECUTIVOS_SEM_META"].map(_sit)

# adiciona colunas por mês (VISUAL)
for ym in meses_janela:
    lab = _fmt_mes(ym)

    col_liq = f"Líquido {lab}"
    col_meta = f"Meta {lab}"
    col_flag = f"Não bateu {lab}"

    liqs, metas, flags = [], [], []
    for v in hist["VISTORIADOR"].tolist():
        un = str(hist.loc[hist["VISTORIADOR"] == v, "CIDADE"].iloc[0] or "").strip().upper()
        liq, meta = _get_liq_meta(ym, v, unid_pref=un)

        liqs.append(np.nan if liq is None else liq)
        metas.append(np.nan if meta is None else meta)

        b = _bateu(liq, meta)
        if b is None:
            flags.append("—")
        else:
            flags.append("🔴" if b is False else "—")

    hist[col_liq] = liqs
    hist[col_meta] = metas
    hist[col_flag] = flags

# formata números
num_cols = [c for c in hist.columns if c.startswith("Líquido ") or c.startswith("Meta ")]
for c in num_cols:
    hist[c] = pd.to_numeric(hist[c], errors="coerce")
    hist[c] = hist[c].map(lambda x: "—" if pd.isna(x) else f"{int(x):,}".replace(",", "."))

# ordena: mais reincidentes primeiro, depois maior faltante no mês atual (se existir)
lab_cur = _fmt_mes(ym_sel)
col_liq_cur = f"Líquido {lab_cur}"
col_meta_cur = f"Meta {lab_cur}"

def _to_int_or_nan(s):
    try:
        return float(str(s).replace(".", "").replace(",", "."))
    except Exception:
        return np.nan

k1 = hist["MESES_CONSECUTIVOS_SEM_META"].astype(int).values * 1_000_000
liq_num = hist[col_liq_cur].map(_to_int_or_nan).fillna(0).values
meta_num = hist[col_meta_cur].map(_to_int_or_nan).fillna(0).values
falt_num = (meta_num - liq_num).clip(min=0)
order_key = k1 + falt_num
hist = hist.iloc[np.argsort(-order_key)].reset_index(drop=True)

# monta ordem de colunas igual ao modelo
cols_show = ["CIDADE", "VISTORIADOR", "TIPO", "SITUAÇÃO", "MESES_CONSECUTIVOS_SEM_META"]
for ym in meses_janela:
    lab = _fmt_mes(ym)
    cols_show += [f"Líquido {lab}", f"Meta {lab}", f"Não bateu {lab}"]

out = hist[cols_show].copy()

st.dataframe(out, use_container_width=True, hide_index=True)
st.caption(
    "Coluna SITUAÇÃO e MESES_CONSECUTIVOS_SEM_META consideram a sequência de meses sem bater meta, "
    "terminando no mês selecionado."
)

# export CSV
csv_bytes = out.to_csv(index=False).encode("utf-8")
st.download_button(
    "Baixar histórico (CSV)",
    data=csv_bytes,
    file_name=f"historico_meta_producao_{ym_sel}.csv",
    mime="text/csv",
)

# ------------------ (Opcional) tabela detalhada do mês ------------------
if not fast_mode:
    st.markdown("---")
    st.markdown('<div class="section">Detalhamento (linhas da produção no mês selecionado)</div>', unsafe_allow_html=True)

    det = viewP_mes.copy()
    det_cols = []
    # tenta manter algumas colunas se existirem na planilha original
    for c in ["__DATA__", "UNIDADE", "VISTORIADOR", "IS_REV", "CHASSI", "PLACA", "CLIENTE", "VEICULO"]:
        if c in det.columns:
            det_cols.append(c)
    # sempre garante as básicas do painel
    for c in ["__DATA__", "UNIDADE", "VISTORIADOR", "IS_REV"]:
        if c not in det_cols and c in det.columns:
            det_cols.append(c)

    if det_cols:
        det2 = det[det_cols].copy()
        det2 = det2.sort_values(["__DATA__", "UNIDADE", "VISTORIADOR"], kind="mergesort")
        st.dataframe(det2, use_container_width=True, hide_index=True)
        st.caption("<div class='table-note'>Filtros desta tabela seguem os filtros do topo.</div>", unsafe_allow_html=True)

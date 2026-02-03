# -*- coding: utf-8 -*-
# ============================================================
# Painel de Produção por Vistoriador — MULTI-MESES (modelo Qualidade)
# (SEM googleapiclient / SEM Drive API)
# Ajustes:
# 1) Ler DIAS_UTEIS da aba METAS e exibir DIAS_UTEIS + NECESSIDADE_DIA corretamente
# 2) Voltar filtros completos: Unidades + (botões selecionar/limpar), Mês, Período dentro do mês, Vistoriadores + (botões)
# 3) Tendência/Projeção no RESUMO calculadas em cima do TOTAL BRUTO (VISTORIAS), como solicitado
# ============================================================

import os
import re
import json
import unicodedata
from datetime import datetime, date
from typing import Optional, Tuple, Dict, List

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt

import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ------------------ CONFIG BÁSICA ------------------
st.set_page_config(page_title="Painel de Produção por Vistoriador - Velox Vistorias", layout="wide")
st.title("Painel de Produção por Vistoriador - Velox Vistoria")

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


# ------------------ GOOGLE SHEETS (SEM DRIVE API) ------------------
def _load_sa_info() -> dict:
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
                return json.load(f)
        except Exception as e:
            st.error(f"Não consegui abrir o JSON da service account: {path}")
            with st.expander("Detalhes"):
                st.exception(e)
            st.stop()

    return dict(block)


def make_client():
    info = _load_sa_info()
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scopes)
    return gspread.authorize(creds)


client = make_client()


# ------------------ SECRETS: IDs ------------------
PROD_INDEX_ID = st.secrets.get("velox_index_sheet_id", "").strip()
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
    return f"{ym[5:7]}/{ym[:4]}"

def _is_workday(d):
    return isinstance(d, date) and d.weekday() < 5

def _nt(x):
    return x


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


# ------------------ LEITURA / PRODUÇÃO + METAS (GOOGLE SHEETS) ------------------
@st.cache_data(ttl=300, show_spinner=False)
def read_prod_month(month_sheet_id: str, ym: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Produção:
    - Cada linha = 1 vistoria
    - Revistoria = mesma UNIDADE + CHASSI a partir da 2ª ocorrência (no mês)
    - IS_REV = 1 (rev) / 0 (principal)
    Metas:
    - Aba 'METAS' (se existir)
    - VISTORIADOR, UNIDADE/CIDADE, META_MENSAL, opcional TIPO e DIAS_UTEIS
    """
    sh = client.open_by_key(month_sheet_id)
    title = sh.title or month_sheet_id

    # produção (aba 1)
    ws = sh.sheet1
    df = pd.DataFrame(ws.get_all_records())
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
        raise ValueError(f"Planilha {title}: precisa conter UNIDADE, DATA, CHASSI, PERITO/DIGITADOR.")

    df[col_unid] = df[col_unid].map(_upper)
    df["__DATA__"] = df[col_data].apply(parse_date_any)
    df[col_chas] = df[col_chas].map(_upper)

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

    # metas (aba METAS)
    metas = pd.DataFrame()
    try:
        ws_meta = sh.worksheet("METAS")
        rows = ws_meta.get_all_records()
        dm = pd.DataFrame(rows) if rows else pd.DataFrame()

        if not dm.empty:
            cols = list(dm.columns)
            c_vist = _find_col(cols, "VISTORIADOR")
            c_unid = _find_col(cols, "UNIDADE", "CIDADE")
            c_meta = _find_col(cols, "META_MENSAL", "META MENSAL", "META")
            c_tipo = _find_col(cols, "TIPO", "PERFIL")
            c_dias = _find_col(cols, "DIAS_UTEIS", "DIAS UTEIS", "DIAS ÚTEIS")

            out = pd.DataFrame()
            out["VISTORIADOR"] = dm[c_vist].astype(str).map(_upper) if c_vist else ""
            out["UNIDADE"] = dm[c_unid].astype(str).map(_upper) if c_unid else ""
            out["META_MENSAL"] = pd.to_numeric(dm[c_meta], errors="coerce").fillna(0).astype(int) if c_meta else 0
            out["TIPO"] = dm[c_tipo].astype(str).map(_upper) if c_tipo else ""
            out["DIAS_UTEIS"] = pd.to_numeric(dm[c_dias], errors="coerce").fillna(0).astype(int) if c_dias else 0
            out["YM"] = ym or ""
            metas = out
    except Exception:
        metas = pd.DataFrame()

    return df, metas, title


# ------------------ CARREGA MESES ------------------
idx_p = read_index(PROD_INDEX_ID)
idx_p = idx_p[idx_p["ATIVO"].map(_yes)].copy()

idx_p["YM"] = idx_p["MÊS"].map(_ym_token)
idx_p = idx_p[idx_p["YM"].notna()].copy()

if idx_p.empty:
    st.error("Índice de Produção (ARQUIVOS) sem meses válidos/ativos.")
    st.stop()

idx_p = idx_p.sort_values("YM").reset_index(drop=True)

dp_all, metas_all = [], []
errors = []

with st.spinner(f"Lendo {len(idx_p)} planilha(s) do índice..."):
    for _, r in idx_p.iterrows():
        sid = _sheet_id(r["URL"])
        ym = r["YM"]
        if not sid:
            continue
        try:
            dp, dm, _ = read_prod_month(sid, ym=ym)
            if not dp.empty:
                dp["YM"] = ym
                dp_all.append(dp)
            if not dm.empty:
                metas_all.append(dm)
        except Exception as e:
            errors.append((sid, str(e)))

if errors:
    with st.expander("Algumas planilhas falharam (clique para ver)"):
        for sid, msg in errors[:50]:
            st.write(f"- {sid}: {msg}")

if not dp_all:
    st.error("Não consegui ler Produção de nenhum mês ativo.")
    st.stop()

dfP = pd.concat(dp_all, ignore_index=True)
dfMetas = pd.concat(metas_all, ignore_index=True) if metas_all else pd.DataFrame(
    columns=["VISTORIADOR", "UNIDADE", "META_MENSAL", "TIPO", "DIAS_UTEIS", "YM"]
)

ym_all = sorted(dfP["YM"].dropna().unique().tolist())
label_map = {_fmt_mes(m): m for m in ym_all}


# ------------------ FILTROS (layout completo) ------------------
st.markdown('<div class="section">Filtros</div>', unsafe_allow_html=True)

# Unidades (geral) - lista considerando mês selecionado depois, mas precisamos de um mês inicial para montar lista
# então: mês como primeiro filtro
sel_label = st.selectbox("Mês de referência", options=list(label_map.keys()), index=len(ym_all) - 1, key="f_mesref")
ym_sel = label_map[sel_label]

viewP_mes_full = dfP[dfP["YM"] == ym_sel].copy()

unids_all = sorted(viewP_mes_full["UNIDADE"].dropna().unique().tolist()) if "UNIDADE" in viewP_mes_full.columns else []
vists_all = sorted(viewP_mes_full["VISTORIADOR"].dropna().unique().tolist()) if "VISTORIADOR" in viewP_mes_full.columns else []

# ---- Unidades com botões selecionar/limpar ----
cU1, cU2, cU3 = st.columns([6, 2, 2])
with cU1:
    f_unids = st.multiselect("Unidades", options=unids_all, default=unids_all, key="f_unids")
with cU2:
    if st.button("Selecionar todas (Unid.)", key="btn_unid_all"):
        st.session_state["f_unids"] = unids_all
        st.rerun()
with cU3:
    if st.button("Limpar (Unid.)", key="btn_unid_none"):
        st.session_state["f_unids"] = []
        st.rerun()

# ---- Período dentro do mês (min/max data do recorte de unidades) ----
# Primeiro aplica unidades para limitar range do período
tmp_for_period = viewP_mes_full.copy()
if "UNIDADE" in tmp_for_period.columns and st.session_state.get("f_unids") is not None:
    if len(st.session_state["f_unids"]) > 0:
        tmp_for_period = tmp_for_period[tmp_for_period["UNIDADE"].isin([_upper(u) for u in st.session_state["f_unids"]])].copy()

dmin = tmp_for_period["__DATA__"].min() if "__DATA__" in tmp_for_period.columns and not tmp_for_period.empty else None
dmax = tmp_for_period["__DATA__"].max() if "__DATA__" in tmp_for_period.columns and not tmp_for_period.empty else None

if not isinstance(dmin, date) or not isinstance(dmax, date):
    # fallback: mês todo (01 ao 28/30/31 não conhecido sem calendar aqui; usa dmin/dmax do próprio dataset)
    period_default = (None, None)
    st.caption("Período dentro do mês: sem datas suficientes para slider (verifique coluna DATA).")
    start_d = None
    end_d = None
else:
    period_default = (dmin, dmax)
    start_d, end_d = st.slider(
        "Período dentro do mês",
        min_value=dmin,
        max_value=dmax,
        value=period_default,
        format="DD/MM/YYYY",
        key="f_periodo"
    )

# ---- Vistoriadores com botões selecionar/limpar ----
cV1, cV2, cV3 = st.columns([6, 2, 2])
with cV1:
    f_vists = st.multiselect("Vistoriadores", options=vists_all, default=[], key="f_vists")
with cV2:
    if st.button("Selecionar todos", key="btn_vist_all"):
        st.session_state["f_vists"] = vists_all
        st.rerun()
with cV3:
    if st.button("Limpar", key="btn_vist_none"):
        st.session_state["f_vists"] = []
        st.rerun()


# ------------------ APLICA FILTROS ------------------
viewP_mes = viewP_mes_full.copy()

# unidades
if "UNIDADE" in viewP_mes.columns:
    sel_u = st.session_state.get("f_unids", unids_all)
    if sel_u is not None and len(sel_u) > 0:
        viewP_mes = viewP_mes[viewP_mes["UNIDADE"].isin([_upper(u) for u in sel_u])].copy()
    elif sel_u is not None and len(sel_u) == 0:
        viewP_mes = viewP_mes.iloc[0:0].copy()

# período
if isinstance(start_d, date) and isinstance(end_d, date) and "__DATA__" in viewP_mes.columns and not viewP_mes.empty:
    viewP_mes = viewP_mes[(viewP_mes["__DATA__"] >= start_d) & (viewP_mes["__DATA__"] <= end_d)].copy()

# vistoriadores
sel_v = st.session_state.get("f_vists", [])
if sel_v and "VISTORIADOR" in viewP_mes.columns:
    viewP_mes = viewP_mes[viewP_mes["VISTORIADOR"].isin([_upper(v) for v in sel_v])].copy()


# ------------------ AGREGAÇÃO BASE ------------------
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

metas_mes = dfMetas[dfMetas["YM"].astype(str) == ym_sel].copy() if "YM" in dfMetas.columns else dfMetas.copy()
if not metas_mes.empty:
    metas_mes["VISTORIADOR"] = metas_mes["VISTORIADOR"].astype(str).map(_upper)
    metas_mes["UNIDADE"] = metas_mes["UNIDADE"].astype(str).map(_upper)
    metas_mes["TIPO"] = metas_mes.get("TIPO","").fillna("").astype(str).map(_upper)
    metas_mes["DIAS_UTEIS"] = pd.to_numeric(metas_mes.get("DIAS_UTEIS", 0), errors="coerce").fillna(0).astype(int)

base_mes = prod_mes.merge(
    metas_mes[["VISTORIADOR", "UNIDADE", "META_MENSAL", "TIPO", "DIAS_UTEIS"]] if not metas_mes.empty else
    pd.DataFrame(columns=["VISTORIADOR", "UNIDADE", "META_MENSAL", "TIPO", "DIAS_UTEIS"]),
    on=["VISTORIADOR", "UNIDADE"],
    how="left",
)

base_mes["META_MENSAL"] = pd.to_numeric(base_mes.get("META_MENSAL", 0), errors="coerce").fillna(0).astype(int)
base_mes["DIAS_UTEIS"] = pd.to_numeric(base_mes.get("DIAS_UTEIS", 0), errors="coerce").fillna(0).astype(int)
base_mes["FALTANTE"] = (base_mes["META_MENSAL"] - base_mes["liq"]).clip(lower=0).astype(int)
base_mes["BATEU"] = base_mes["liq"] >= base_mes["META_MENSAL"]
base_mes["TIPO"] = base_mes.get("TIPO", "").fillna("").astype(str).map(_upper)


# ------------------ CARDS ------------------
total_vist = int(prod_mes["vist"].sum()) if not prod_mes.empty else 0
total_rev = int(prod_mes["rev"].sum()) if not prod_mes.empty else 0
total_liq = int(prod_mes["liq"].sum()) if not prod_mes.empty else 0
qtd_vists = int(prod_mes["VISTORIADOR"].nunique()) if not prod_mes.empty else 0
qtd_nao_bateu = int((base_mes["BATEU"] == False).sum()) if not base_mes.empty else 0
qtd_bateu = int((base_mes["BATEU"] == True).sum()) if not base_mes.empty else 0

st.markdown(
    f"""
<div class="card-wrap">
  <div class='card'><h4>Total bruto (mês)</h4><h2>{_fmt_int(total_vist)}</h2><span class='sub neu'>vistorias</span></div>
  <div class='card'><h4>Total revistorias (mês)</h4><h2>{_fmt_int(total_rev)}</h2><span class='sub neu'>rev</span></div>
  <div class='card'><h4>Total líquido (mês)</h4><h2>{_fmt_int(total_liq)}</h2><span class='sub neu'>vist - rev</span></div>
  <div class='card'><h4>Vistoriadores no recorte</h4><h2>{_fmt_int(qtd_vists)}</h2></div>
  <div class='card'><h4>Bateram meta</h4><h2>{_fmt_int(qtd_bateu)}</h2><span class='sub ok'>no mês</span></div>
  <div class='card'><h4>Não bateram meta</h4><h2>{_fmt_int(qtd_nao_bateu)}</h2><span class='sub bad'>no mês</span></div>
</div>
""",
    unsafe_allow_html=True,
)


# ------------------ RESUMO (mês selecionado) — MODELO ANTIGO (tendência no BRUTO) ------------------
st.markdown('<div class="section">Resumo por Vistoriador</div>', unsafe_allow_html=True)

view = viewP_mes.copy()
col_unid = "UNIDADE"

if view.empty:
    st.caption("Sem registros para os filtros aplicados.")
else:
    grp = (view
           .groupby("VISTORIADOR", dropna=False)
           .agg(
                VISTORIAS=("IS_REV", "size"),
                REVISTORIAS=("IS_REV", "sum"),
                DIAS_ATIVOS=("__DATA__", lambda s: s.dropna().nunique()),
                UNIDADES=(col_unid, lambda s: s.dropna().nunique()),
           )
           .reset_index())

    grp["LIQUIDO"] = grp["VISTORIAS"] - grp["REVISTORIAS"]

    def _calc_wd_passados(df_view: pd.DataFrame) -> pd.DataFrame:
        if df_view.empty or "__DATA__" not in df_view.columns or "VISTORIADOR" not in df_view.columns:
            return pd.DataFrame(columns=["VISTORIADOR", "DIAS_PASSADOS"])
        mask = df_view["__DATA__"].apply(_is_workday)
        if not mask.any():
            vists = df_view["VISTORIADOR"].dropna().unique()
            return pd.DataFrame({"VISTORIADOR": vists, "DIAS_PASSADOS": np.zeros(len(vists), dtype=int)})
        out = (df_view.loc[mask]
               .groupby("VISTORIADOR")["__DATA__"]
               .nunique()
               .reset_index()
               .rename(columns={"__DATA__": "DIAS_PASSADOS"}))
        out["DIAS_PASSADOS"] = out["DIAS_PASSADOS"].astype(int)
        return out

    wd_passados = _calc_wd_passados(view)
    grp = grp.merge(wd_passados, on="VISTORIADOR", how="left").fillna({"DIAS_PASSADOS": 0})
    grp["DIAS_PASSADOS"] = grp["DIAS_PASSADOS"].astype(int)

    # METAS do mês selecionado
    metas_ref = dfMetas[dfMetas["YM"].astype(str) == ym_sel].copy() if not dfMetas.empty else pd.DataFrame()

    if not metas_ref.empty:
        metas_ref["META_MENSAL"] = pd.to_numeric(metas_ref.get("META_MENSAL", 0), errors="coerce").fillna(0)
        metas_ref["DIAS_UTEIS"] = pd.to_numeric(metas_ref.get("DIAS_UTEIS", 0), errors="coerce").fillna(0)
        metas_ref = (metas_ref
                     .groupby("VISTORIADOR", dropna=False)
                     .agg(
                        UNIDADE=("UNIDADE", lambda s: s.dropna().iloc[0] if s.dropna().size else ""),
                        TIPO=("TIPO", lambda s: s.dropna().iloc[0] if s.dropna().size else ""),
                        META_MENSAL=("META_MENSAL", "sum"),
                        DIAS_UTEIS=("DIAS_UTEIS", "max"),
                     )
                     .reset_index())
    else:
        metas_ref = pd.DataFrame(columns=["VISTORIADOR", "UNIDADE", "TIPO", "META_MENSAL", "DIAS_UTEIS"])

    grp = grp.merge(metas_ref[["VISTORIADOR", "UNIDADE", "TIPO", "META_MENSAL", "DIAS_UTEIS"]],
                    on="VISTORIADOR", how="left")

    grp["UNIDADE"] = grp["UNIDADE"].fillna("")
    grp["TIPO"] = grp["TIPO"].fillna("")
    grp["META_MENSAL"] = pd.to_numeric(grp.get("META_MENSAL", 0), errors="coerce").fillna(0).astype(int)
    grp["DIAS_UTEIS"] = pd.to_numeric(grp.get("DIAS_UTEIS", 0), errors="coerce").fillna(0).astype(int)

    # cálculos
    grp["META_DIA"] = np.where(grp["DIAS_UTEIS"] > 0, grp["META_MENSAL"] / grp["DIAS_UTEIS"], 0.0)
    grp["FALTANTE_MES"] = np.maximum(grp["META_MENSAL"] - grp["LIQUIDO"], 0)

    grp["DIAS_RESTANTES"] = np.maximum(grp["DIAS_UTEIS"] - grp["DIAS_PASSADOS"], 0)

    grp["NECESSIDADE_DIA"] = np.where(
        grp["DIAS_RESTANTES"] > 0,
        grp["FALTANTE_MES"] / grp["DIAS_RESTANTES"],
        0.0
    )

    # tendência no BRUTO (VISTORIAS)
    grp["MEDIA_DIA_ATUAL"] = np.where(
        grp["DIAS_PASSADOS"] > 0,
        grp["VISTORIAS"] / grp["DIAS_PASSADOS"],
        0.0
    )
    grp["PROJECAO_MES"] = (grp["VISTORIAS"] + grp["MEDIA_DIA_ATUAL"] * grp["DIAS_RESTANTES"]).round(0)
    grp["TENDENCIA_%"] = np.where(grp["META_MENSAL"] > 0, (grp["PROJECAO_MES"] / grp["META_MENSAL"]) * 100, np.nan)

    # normalização tipo + filtro só para tabela
    grp["TIPO_NORM"] = grp.get("TIPO", "").astype(str).str.upper().str.replace("MOVEL", "MÓVEL").str.strip()
    grp.loc[grp["TIPO_NORM"] == "", "TIPO_NORM"] = "—"

    tipo_options = [t for t in ["FIXO", "MÓVEL"] if t in grp["TIPO_NORM"].unique().tolist()]
    if "—" in grp["TIPO_NORM"].unique():
        tipo_options.append("—")

    sel_tipos = st.multiselect(
        "Tipo (filtro apenas desta tabela)",
        options=tipo_options,
        default=tipo_options,
        key="resumo_tipo_filter"
    )
    grp_tbl = grp if not sel_tipos else grp[grp["TIPO_NORM"].isin(sel_tipos)]

    # ordenação e formatação
    grp_tbl = grp_tbl.sort_values(["PROJECAO_MES", "LIQUIDO"], ascending=[False, False])
    fmt = grp_tbl.copy()

    def chip_tend(p):
        if pd.isna(p): return "—"
        p = float(p)
        if p >= 100: return f"{p:.0f}% 🚀"
        if p >= 95:  return f"{p:.0f}% 💪"
        if p >= 85:  return f"{p:.0f}% 😬"
        return f"{p:.0f}% 😟"

    def chip_nec(x):
        try:
            v = float(x)
        except:
            return "—"
        return "0 ✅" if v <= 0 else f"{int(round(v))} 🔥"

    fmt["TIPO"] = fmt["TIPO_NORM"].map({"FIXO": "🏢 FIXO", "MÓVEL": "🚗 MÓVEL"}).fillna("—")
    fmt["META_MENSAL"]      = fmt["META_MENSAL"].map(lambda x: f"{int(x):,}".replace(",", "."))
    fmt["DIAS_UTEIS"]       = fmt["DIAS_UTEIS"].map(lambda x: f"{int(x)}")
    fmt["META_DIA"]         = fmt["META_DIA"].map(lambda x: f"{x:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))
    fmt["VISTORIAS"]        = fmt["VISTORIAS"].map(lambda x: f"{int(x)}")
    fmt["REVISTORIAS"]      = fmt["REVISTORIAS"].map(lambda x: f"{int(x)}")
    fmt["LIQUIDO"]          = fmt["LIQUIDO"].map(lambda x: f"{int(x)}")
    fmt["FALTANTE_MES"]     = fmt["FALTANTE_MES"].map(lambda x: f"{int(x)}")
    fmt["NECESSIDADE_DIA"]  = fmt["NECESSIDADE_DIA"].apply(chip_nec)
    fmt["TENDÊNCIA"]        = fmt["TENDENCIA_%"].apply(chip_tend)
    fmt["PROJECAO_MES"]     = fmt["PROJECAO_MES"].map(lambda x: "—" if pd.isna(x) else f"{int(round(x))}")

    cols_show = [
        "VISTORIADOR", "UNIDADE", "TIPO",
        "META_MENSAL", "DIAS_UTEIS", "META_DIA",
        "VISTORIAS", "REVISTORIAS", "LIQUIDO",
        "FALTANTE_MES", "NECESSIDADE_DIA", "TENDÊNCIA", "PROJECAO_MES"
    ]
    cols_show_avail = [c for c in cols_show if c in fmt.columns]

    if fmt.empty or not cols_show_avail:
        st.caption("Sem registros para os filtros aplicados.")
    else:
        st.dataframe(fmt[cols_show_avail], use_container_width=True, hide_index=True)
        csv = fmt[cols_show_avail].to_csv(index=False).encode("utf-8-sig")
        st.download_button("Baixar resumo (CSV)", data=csv, file_name="resumo_vistoriador.csv", mime="text/csv")


# ------------------ HISTÓRICO VISUAL (MODELO QUALIDADE) ------------------
st.markdown("---")
st.markdown('<div class="section">Histórico de Meta (quem não bateu no mês selecionado)</div>', unsafe_allow_html=True)

alvo = base_mes[base_mes["BATEU"] == False].copy()
if alvo.empty:
    st.info("No recorte atual, ninguém ficou abaixo da meta no mês selecionado.")
    st.stop()

alvo_names = sorted(alvo["VISTORIADOR"].unique().tolist())

idx_cur = ym_all.index(ym_sel) if ym_sel in ym_all else len(ym_all) - 1
meses_janela = ym_all[: idx_cur + 1]

@st.cache_data(ttl=300, show_spinner=False)
def build_month_maps(dfP_all: pd.DataFrame, dfM_all: pd.DataFrame):
    prod_map = {}
    meta_map = {}
    for ym in sorted(dfP_all["YM"].dropna().unique().tolist()):
        p = dfP_all[dfP_all["YM"] == ym].copy()
        pm = _make_prod(p)[["VISTORIADOR", "UNIDADE", "liq"]].copy() if not p.empty else pd.DataFrame(columns=["VISTORIADOR","UNIDADE","liq"])
        prod_map[ym] = pm

    if dfM_all is None or dfM_all.empty or "YM" not in dfM_all.columns:
        for ym in sorted(dfP_all["YM"].dropna().unique().tolist()):
            meta_map[ym] = pd.DataFrame(columns=["VISTORIADOR", "UNIDADE", "META_MENSAL", "TIPO"])
    else:
        dm = dfM_all.copy()
        dm["VISTORIADOR"] = dm["VISTORIADOR"].astype(str).map(_upper)
        dm["UNIDADE"] = dm["UNIDADE"].astype(str).map(_upper)
        dm["TIPO"] = dm.get("TIPO","").fillna("").astype(str).map(_upper)
        dm["META_MENSAL"] = pd.to_numeric(dm.get("META_MENSAL", 0), errors="coerce").fillna(0).astype(int)

        for ym in sorted(dfP_all["YM"].dropna().unique().tolist()):
            mm = dm[dm["YM"].astype(str) == ym][["VISTORIADOR", "UNIDADE", "META_MENSAL", "TIPO"]].copy()
            meta_map[ym] = mm

    return prod_map, meta_map

prod_map, meta_map = build_month_maps(dfP, dfMetas)

city_map = {}
tipo_map = {}

try:
    mm = metas_mes.copy()
    if not mm.empty:
        mm = mm[mm["VISTORIADOR"].isin(alvo_names)].copy()
        mm = mm.drop_duplicates(subset=["VISTORIADOR"])
        city_map.update(dict(zip(mm["VISTORIADOR"], mm["UNIDADE"])))
        tipo_map.update(dict(zip(mm["VISTORIADOR"], mm["TIPO"])))
except Exception:
    pass

try:
    bc = viewP_mes[["VISTORIADOR", "UNIDADE"]].copy()
    bc["VISTORIADOR"] = bc["VISTORIADOR"].astype(str).map(_upper)
    bc["UNIDADE"] = bc["UNIDADE"].astype(str).map(_upper)
    bc = bc.drop_duplicates(subset=["VISTORIADOR"])
    for v, u in zip(bc["VISTORIADOR"], bc["UNIDADE"]):
        if v in alvo_names and (v not in city_map or not city_map.get(v)):
            city_map[v] = u
except Exception:
    pass

hist = pd.DataFrame({"VISTORIADOR": alvo_names})
hist["CIDADE"] = hist["VISTORIADOR"].map(city_map).fillna("")
hist["TIPO"] = hist["VISTORIADOR"].map(tipo_map).fillna("")

def _get_liq_meta(ym: str, vist: str, unid_pref: str = "") -> Tuple[Optional[int], Optional[int]]:
    pm = prod_map.get(ym, pd.DataFrame(columns=["VISTORIADOR","UNIDADE","liq"]))
    mm = meta_map.get(ym, pd.DataFrame(columns=["VISTORIADOR","UNIDADE","META_MENSAL","TIPO"]))

    liq = None
    meta = None

    if not pm.empty:
        q = pm[pm["VISTORIADOR"] == vist]
        if unid_pref:
            q2 = q[q["UNIDADE"] == unid_pref]
            liq = int(q2["liq"].sum()) if not q2.empty else (int(q["liq"].sum()) if not q.empty else None)
        else:
            liq = int(q["liq"].sum()) if not q.empty else None

    if not mm.empty:
        q = mm[mm["VISTORIADOR"] == vist]
        if unid_pref:
            q2 = q[q["UNIDADE"] == unid_pref]
            meta = int(q2["META_MENSAL"].sum()) if not q2.empty else (int(q["META_MENSAL"].sum()) if not q.empty else None)
        else:
            meta = int(q["META_MENSAL"].sum()) if not q.empty else None

    return liq, meta

def _bateu(liq: Optional[int], meta: Optional[int]) -> Optional[bool]:
    if liq is None or meta is None or meta <= 0:
        return None
    return liq >= meta

streaks = []
for v in hist["VISTORIADOR"].tolist():
    un = str(hist.loc[hist["VISTORIADOR"] == v, "CIDADE"].iloc[0] or "").strip().upper()
    cons = 0
    for ym in reversed(meses_janela):
        liq, meta = _get_liq_meta(ym, v, unid_pref=un)
        b = _bateu(liq, meta)
        if b is None:
            break
        if b is False:
            cons += 1
        else:
            break
    streaks.append(cons)

hist["MESES_CONSECUTIVOS_SEM_META"] = streaks

def _sit(cons: int) -> str:
    if cons >= 3: return "3+ meses sem meta"
    if cons == 2: return "2 meses sem meta"
    if cons == 1: return "Entrou agora"
    return "—"

hist["SITUAÇÃO"] = hist["MESES_CONSECUTIVOS_SEM_META"].map(_sit)

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
        flags.append("🔴" if b is False else "—" if b is True else "—")

    hist[col_liq] = liqs
    hist[col_meta] = metas
    hist[col_flag] = flags

num_cols = [c for c in hist.columns if c.startswith("Líquido ") or c.startswith("Meta ")]
for c in num_cols:
    hist[c] = pd.to_numeric(hist[c], errors="coerce")
    hist[c] = hist[c].map(lambda x: "—" if pd.isna(x) else f"{int(x):,}".replace(",", "."))

lab_cur = _fmt_mes(ym_sel)
col_liq_cur = f"Líquido {lab_cur}"
col_meta_cur = f"Meta {lab_cur}"

def _to_num(s):
    try:
        return float(str(s).replace(".", "").replace(",", "."))
    except Exception:
        return np.nan

liq_num = hist[col_liq_cur].map(_to_num).fillna(0).values
meta_num = hist[col_meta_cur].map(_to_num).fillna(0).values
falt_num = (meta_num - liq_num).clip(min=0)

order_key = hist["MESES_CONSECUTIVOS_SEM_META"].astype(int).values * 1_000_000 + falt_num
hist = hist.iloc[np.argsort(-order_key)].reset_index(drop=True)

cols_show = ["CIDADE", "VISTORIADOR", "TIPO", "SITUAÇÃO", "MESES_CONSECUTIVOS_SEM_META"]
for ym in meses_janela:
    lab = _fmt_mes(ym)
    cols_show += [f"Líquido {lab}", f"Meta {lab}", f"Não bateu {lab}"]

out = hist[cols_show].copy()

st.dataframe(out, use_container_width=True, hide_index=True)
st.caption("SITUAÇÃO e MESES_CONSECUTIVOS_SEM_META consideram a sequência terminando no mês selecionado.")

csv_bytes = out.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    "Baixar histórico (CSV)",
    data=csv_bytes,
    file_name=f"historico_meta_producao_{ym_sel}.csv",
    mime="text/csv",
)


# =========================
# Evolução diária
# =========================
st.markdown("<div class='section-title'>Evolução diária</div>", unsafe_allow_html=True)
if view.empty:
    st.caption("Sem dados no período selecionado.")
else:
    daily = (view.groupby("__DATA__", dropna=False)
             .agg(VISTORIAS=("IS_REV","size"), REVISTORIAS=("IS_REV","sum"))
             .reset_index())
    daily = daily[pd.notna(daily["__DATA__"])].sort_values("__DATA__")
    daily["LIQUIDO"] = daily["VISTORIAS"] - daily["REVISTORIAS"]
    daily_melt = daily.melt(id_vars="__DATA__", value_vars=["VISTORIAS","REVISTORIAS","LIQUIDO"], var_name="Métrica", value_name="Valor")

    if daily_melt.empty:
        st.caption("Sem evolução diária para exibir.")
    else:
        line = (alt.Chart(daily_melt)
                .mark_line(point=True)
                .encode(
                    x=alt.X("__DATA__:T", title="Data"),
                    y=alt.Y("Valor:Q", title="Quantidade"),
                    color=alt.Color("Métrica:N", title="Métrica"),
                    tooltip=[alt.Tooltip("__DATA__:T", title="Data"),
                             alt.Tooltip("Métrica:N", title="Métrica"),
                             alt.Tooltip("Valor:Q", title="Valor")]
                ).properties(height=360))
        st.altair_chart(line, use_container_width=True)


# =========================
# Produção por Unidade (Líquido)
# =========================
st.markdown("<div class='section-title'>Produção por Unidade (Líquido)</div>", unsafe_allow_html=True)
if view.empty:
    st.caption("Sem dados de unidades para o período.")
else:
    by_unid = (view.groupby(col_unid, dropna=False)
                    .agg(liq=("IS_REV", lambda s: s.size - s.sum()))
                    .reset_index()
                    .sort_values("liq", ascending=False))
    if by_unid.empty:
        st.caption("Sem produção por unidade dentro dos filtros.")
    else:
        bar_unid = (alt.Chart(by_unid)
                    .mark_bar()
                    .encode(
                        x=alt.X(f"{col_unid}:N", sort='-y', title="Unidade",
                                axis=alt.Axis(labelAngle=-30)),
                        y=alt.Y("liq:Q", title="Líquido"),
                        tooltip=[alt.Tooltip(f"{col_unid}:N", title="Unidade"),
                                 alt.Tooltip("liq:Q", title="Líquido")]
                    ).properties(height=420))
        st.altair_chart(bar_unid, use_container_width=True)


# =========================
# Auditoria – Chassis com múltiplas vistorias
# =========================
st.markdown("<div class='section-title'>Chassis com múltiplas vistorias</div>", unsafe_allow_html=True)

if view.empty:
    st.caption("Nenhum chassi com múltiplas vistorias dentro dos filtros.")
else:
    # Descobre qual coluna é o CHASSI dentro do dataframe "view"
    col_chas_view = _find_col(list(view.columns), "CHASSI")

    if not col_chas_view:
        st.caption("Não encontrei a coluna CHASSI no recorte atual para montar a auditoria.")
    else:
        dup = (
            view.groupby(col_chas_view, dropna=False)
                .agg(
                    QTD=("VISTORIADOR", "size"),
                    PRIMEIRA_DATA=("__DATA__", "min"),
                    ULTIMA_DATA=("__DATA__", "max"),
                )
                .reset_index()
        )

        dup = dup[dup["QTD"] >= 2].sort_values("QTD", ascending=False)

        if dup.empty:
            st.caption("Nenhum chassi com múltiplas vistorias dentro dos filtros.")
        else:
            first_map = (
                view.sort_values(["__DATA__"])
                    .drop_duplicates(subset=[col_chas_view], keep="first")
                    .set_index(col_chas_view)["VISTORIADOR"]
                    .to_dict()
            )

            last_map = (
                view.sort_values(["__DATA__"])
                    .drop_duplicates(subset=[col_chas_view], keep="last")
                    .set_index(col_chas_view)["VISTORIADOR"]
                    .to_dict()
            )

            dup["PRIMEIRO_VIST"] = dup[col_chas_view].map(first_map)
            dup["ULTIMO_VIST"]   = dup[col_chas_view].map(last_map)

            st.dataframe(dup, use_container_width=True, hide_index=True)


# =========================
# CONSOLIDADO DO MÊS + RANKING MENSAL (TOP/BOTTOM)
# =========================
TOP_LABEL = "TOP BOX"
BOTTOM_LABEL = "BOTTOM BOX"

st.markdown("---")
st.markdown("<div class='section-title'>Consolidado do Mês + Ranking por Vistoriador</div>", unsafe_allow_html=True)

datas_ok = [d for d in view["__DATA__"] if isinstance(d, date)]
if len(datas_ok) == 0:
    st.info("Sem datas dentro dos filtros atuais para montar o consolidado do mês.")
else:
    ref = sorted(datas_ok)[-1]
    ref_ano, ref_mes = ref.year, ref.month
    mes_label = f"{ref_mes:02d}/{ref_ano}"
    mask_mes = view["__DATA__"].apply(lambda d: isinstance(d, date) and d.year == ref_ano and d.month == ref_mes)
    view_mes = view[mask_mes].copy()

    prod_mes = (view_mes.groupby("VISTORIADOR", dropna=False)
                .agg(VISTORIAS=("IS_REV","size"), REVISTORIAS=("IS_REV","sum")).reset_index())
    prod_mes["LIQUIDO"] = prod_mes["VISTORIAS"] - prod_mes["REVISTORIAS"]

    metas_join = (
        dfMetas[dfMetas["YM"] == f"{ref_ano}-{ref_mes:02d}"][["VISTORIADOR","TIPO","META_MENSAL"]].copy()
        if not dfMetas.empty and "YM" in dfMetas.columns
        else pd.DataFrame(columns=["VISTORIADOR","TIPO","META_MENSAL"])
    )

    base_mes = prod_mes.merge(metas_join, on="VISTORIADOR", how="left")
    base_mes["TIPO"] = (
    base_mes["TIPO"]
    .fillna("")
    .astype(str)
    .map(_upper)                # já existe no seu código
    .replace({"MOVEL": "MÓVEL"})
    .replace("", "—")
)
    base_mes["META_MENSAL"] = pd.to_numeric(base_mes["META_MENSAL"], errors="coerce").fillna(0)
    base_mes["ATING_%"] = np.where(base_mes["META_MENSAL"]>0, (base_mes["VISTORIAS"]/base_mes["META_MENSAL"])*100, np.nan)

    meta_tot = int(base_mes["META_MENSAL"].sum())
    vist_tot = int(base_mes["VISTORIAS"].sum())
    rev_tot  = int(base_mes["REVISTORIAS"].sum())
    liq_tot  = int(base_mes["LIQUIDO"].sum())
    ating_g  = (vist_tot / meta_tot * 100) if meta_tot > 0 else np.nan

    def chip_pct(p):
        if pd.isna(p): return "—"
        p = float(p)
        if p >= 110: emo = "🏆"
        elif p >= 100: emo = "🚀"
        elif p >= 90: emo = "💪"
        elif p >= 80: emo = "😬"
        else: emo = "😟"
        return f"{p:.0f}% {emo}"

    cards_mes = [
        ("Mês de referência", mes_label),
        ("Meta (soma)", f"{meta_tot:,}".replace(",", ".")),
        ("Vistorias (geral)", f"{vist_tot:,}".replace(",", ".")),
        ("Revistorias", f"{rev_tot:,}".replace(",", ".")),
        ("Líquido", f"{liq_tot:,}".replace(",", ".")),
        ("% Ating. (sobre geral)", chip_pct(ating_g)),
    ]
    st.markdown(
        '<div class="card-wrap">' +
        "".join([f"<div class='card'><h4>{t}</h4><h2>{v}</h2></div>" for t, v in cards_mes]) +
        "</div>",
        unsafe_allow_html=True
    )

    def chip_pct_row(p):
        if pd.isna(p): return "—"
        p = float(p)
        if p >= 110: emo = "🏆"
        elif p >= 100: emo = "🚀"
        elif p >= 90: emo = "💪"
        elif p >= 80: emo = "😬"
        else: emo = "😟"
        return f"{p:.0f}% {emo}"

    def render_ranking(df_sub, titulo):
        if len(df_sub) == 0:
            st.caption(f"Sem dados para {titulo} em {mes_label}.")
            return
        rk = df_sub[df_sub["META_MENSAL"] > 0].copy()
        if len(rk) == 0:
            st.caption(f"Ninguém com META cadastrada para {titulo}.")
            return

        rk = rk.sort_values("ATING_%", ascending=False)

        top = rk.head(5).copy()
        medals = ["🥇","🥈","🥉","🏅","🏅"]
        top["🏅"] = [medals[i] if i < len(medals) else "🏅" for i in range(len(top))]
        top_fmt = pd.DataFrame({
            " ": top["🏅"],
            "Vistoriador": top["VISTORIADOR"],
            "Meta (mês)": top["META_MENSAL"].map(lambda x: f"{int(x):,}".replace(",", ".")),
            "Vistorias (geral)": top["VISTORIAS"].map(int),
            "Revistorias": top["REVISTORIAS"].map(int),
            "Líquido": top["LIQUIDO"].map(int),
            "% Ating. (geral/meta)": top["ATING_%"].map(chip_pct_row),
        })

        bot = rk.tail(5).sort_values("ATING_%", ascending=True).copy()
        badgies = ["🆘","🪫","🐢","⚠️","⚠️"]
        bot["⚠️"] = [badgies[i] if i < len(badgies) else "⚠️" for i in range(len(bot))]
        bot_fmt = pd.DataFrame({
            " ": bot["⚠️"],
            "Vistoriador": bot["VISTORIADOR"],
            "Meta (mês)": bot["META_MENSAL"].map(lambda x: f"{int(x):,}".replace(",", ".")),
            "Vistorias (geral)": bot["VISTORIAS"].map(int),
            "Revistorias": bot["REVISTORIAS"].map(int),
            "Líquido": bot["LIQUIDO"].map(int),
            "% Ating. (geral/meta)": bot["ATING_%"].map(chip_pct_row),
        })

        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**{_nt(TOP_LABEL)} — {mes_label}**", unsafe_allow_html=True)
            st.dataframe(top_fmt, use_container_width=True, hide_index=True)
        with c2:
            st.markdown(f"**{_nt(BOTTOM_LABEL)} — {mes_label}**", unsafe_allow_html=True)
            st.dataframe(bot_fmt, use_container_width=True, hide_index=True)

    st.markdown("#### FIXO")
    render_ranking(base_mes[base_mes["TIPO"] == "FIXO"], "vistoriadores FIXO")

    st.markdown("#### MÓVEL")
    render_ranking(base_mes[base_mes["TIPO"].isin(["MÓVEL","MOVEL"])], "vistoriadores MÓVEL")


# =========================
# RANKING DO DIA POR VISTORIADOR (TOP/BOTTOM)
# =========================
st.markdown("---")
st.markdown("<div class='section-title'>Ranking do Dia por Vistoriador</div>", unsafe_allow_html=True)

dates_avail = sorted([d for d in view["__DATA__"] if isinstance(d, date)])
if not dates_avail:
    st.info("Sem datas dentro dos filtros atuais para montar o ranking diário.")
else:
    default_day = dates_avail[-1]
    rank_day = st.date_input(
        "Dia para o ranking",
        value=st.session_state.get("rank_day_sel", default_day),
        format="DD/MM/YYYY",
        key="rank_day_sel"
    )

    if rank_day in dates_avail:
        used_day = rank_day
        info_msg = None
    else:
        cands = [d for d in dates_avail if d <= rank_day]
        used_day = cands[-1] if cands else dates_avail[-1]
        info_msg = f"Sem dados em {rank_day.strftime('%d/%m/%Y')}. Exibindo {used_day.strftime('%d/%m/%Y')}."

    if info_msg:
        st.caption(info_msg)

    view_dia = view[view["__DATA__"] == used_day].copy()

    prod_dia = (view_dia.groupby("VISTORIADOR", dropna=False)
                .agg(VISTORIAS_DIA=("IS_REV", "size"),
                     REVISTORIAS_DIA=("IS_REV", "sum")).reset_index())
    prod_dia["LIQUIDO_DIA"] = prod_dia["VISTORIAS_DIA"] - prod_dia["REVISTORIAS_DIA"]

    ym_day = f"{used_day.year}-{used_day.month:02d}"
    metas_join = (
        dfMetas[dfMetas["YM"] == ym_day][["VISTORIADOR","TIPO","META_MENSAL","DIAS_UTEIS"]].copy()
        if not dfMetas.empty and "YM" in dfMetas.columns
        else pd.DataFrame(columns=["VISTORIADOR","TIPO","META_MENSAL","DIAS_UTEIS"])
    )

    base_dia = prod_dia.merge(metas_join, on="VISTORIADOR", how="left")
    base_dia["TIPO"] = base_dia["TIPO"].astype(str).str.upper().replace({"MOVEL":"MÓVEL"}).replace("", "—")
    for c in ["META_MENSAL","DIAS_UTEIS"]:
        base_dia[c] = pd.to_numeric(base_dia.get(c,0), errors="coerce").fillna(0)

    base_dia["META_DIA"] = np.where(base_dia["DIAS_UTEIS"]>0, base_dia["META_MENSAL"]/base_dia["DIAS_UTEIS"], 0.0)
    base_dia["ATING_DIA_%"] = np.where(base_dia["META_DIA"]>0, (base_dia["VISTORIAS_DIA"]/base_dia["META_DIA"])*100, np.nan)

    def chip_pct_row_dia(p):
        if pd.isna(p): return "—"
        p = float(p)
        if p >= 110: emo = "🏆"
        elif p >= 100: emo = "🚀"
        elif p >= 90: emo = "💪"
        elif p >= 80: emo = "😬"
        else: emo = "😟"
        return f"{p:.0f}% {emo}"

    def render_ranking_dia(df_sub, titulo):
        if df_sub.empty:
            st.caption(f"Sem dados para {titulo} em {used_day.strftime('%d/%m/%Y')}.")
            return
        rk = df_sub[df_sub["META_DIA"] > 0].copy()
        if rk.empty:
            st.caption(f"Ninguém com META do dia cadastrada para {titulo}.")
            return

        rk = rk.sort_values("ATING_DIA_%", ascending=False)

        top = rk.head(5).copy()
        medals = ["🥇","🥈","🥉","🏅","🏅"]
        top["🏅"] = [medals[i] if i < len(medals) else "🏅" for i in range(len(top))]
        top_fmt = pd.DataFrame({
            " ": top["🏅"], "Vistoriador": top["VISTORIADOR"],
            "Meta (dia)": top["META_DIA"].map(lambda x: int(round(x))),
            "Vistorias (dia)": top["VISTORIAS_DIA"].map(int),
            "Revistorias": top["REVISTORIAS_DIA"].map(int),
            "Líquido (dia)": top["LIQUIDO_DIA"].map(int),
            "% Ating. (dia)": top["ATING_DIA_%"].map(chip_pct_row_dia),
        })

        bot = rk.tail(5).sort_values("ATING_DIA_%", ascending=True).copy()
        badgies = ["🆘","🪫","🐢","⚠️","⚠️"]
        bot["⚠️"] = [badgies[i] if i < len(badgies) else "⚠️" for i in range(len(bot))]
        bot_fmt = pd.DataFrame({
            " ": bot["⚠️"], "Vistoriador": bot["VISTORIADOR"],
            "Meta (dia)": bot["META_DIA"].map(lambda x: int(round(x))),
            "Vistorias (dia)": bot["VISTORIAS_DIA"].map(int),
            "Revistorias": bot["REVISTORIAS_DIA"].map(int),
            "Líquido (dia)": bot["LIQUIDO_DIA"].map(int),
            "% Ating. (dia)": bot["ATING_DIA_%"].map(chip_pct_row_dia),
        })

        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**{_nt(TOP_LABEL)}**", unsafe_allow_html=True)
            st.dataframe(top_fmt, use_container_width=True, hide_index=True)
        with c2:
            st.markdown(f"**{_nt(BOTTOM_LABEL)}**", unsafe_allow_html=True)
            st.dataframe(bot_fmt, use_container_width=True, hide_index=True)

    st.markdown("#### FIXO")
    render_ranking_dia(base_dia[base_dia["TIPO"] == "FIXO"], "vistoriadores FIXO")

    st.markdown("#### MÓVEL")
    render_ranking_dia(base_dia[base_dia["TIPO"].isin(["MÓVEL","MOVEL"])], "vistoriadores MÓVEL")



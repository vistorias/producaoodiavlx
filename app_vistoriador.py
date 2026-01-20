# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# Painel de Produção por Vistoriador (Streamlit) - MULTI-MESES (VELOX VISTORIAS)
# - Lê automaticamente os arquivos listados na planilha-índice (secrets.velox_index_sheet_id)
# - Junta dados de todos os meses e lê METAS por mês
# - KPIs, Resumo, Gráficos, Auditoria, Rankings
#
# NOVA VISUALIZAÇÃO (ADICIONADA):
# - "Histórico de Meta (quem não bateu no mês selecionado)"
#   Mostra há quantos meses consecutivos o colaborador está sem bater a meta,
#   além de um resumo dos últimos meses e um detalhamento por vistoriador.
# ------------------------------------------------------------

import os, re, json
from datetime import datetime, date
from typing import Tuple, List, Optional

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt

import gspread
from oauth2client.service_account import ServiceAccountCredentials


# =========================
# CONFIG BÁSICA
# =========================
st.set_page_config(page_title="Produção por Vistoriador - VELOX VISTORIAS (multi-meses)", layout="wide")
st.title("Painel de Produção por Vistoriador - VELOX VISTORIAS")

# === Planilha-Índice (ARQUIVOS) ===
INDEX_SHEET_ID = (st.secrets.get("velox_index_sheet_id", "") or "").strip()
INDEX_TAB_NAME = "ARQUIVOS"

if not INDEX_SHEET_ID:
    st.error("Defina no secrets.toml a chave velox_index_sheet_id com o ID da planilha-índice da VELOX.")
    st.stop()

# --- estilos ---
st.markdown("""
<style>
  .notranslate { unicode-bidi: plaintext; }
  .card-container { display:flex; gap:18px; margin:12px 0 22px; flex-wrap:wrap; }
  .card { background:#f5f5f5; padding:18px 20px; border-radius:12px; box-shadow:0 2px 6px rgba(0,0,0,.10); text-align:center; min-width:200px; flex:1; }
  .card h4 { color:#cc3300; margin:0 0 8px; font-size:16px; font-weight:700; }
  .card h2 { margin:0; font-size:26px; font-weight:800; color:#222; }
  .section-title { font-size:20px; font-weight:800; margin:22px 0 8px; }
  .small { color:#7b7b7b; font-size:13px; }
</style>
""", unsafe_allow_html=True)

def _nt(txt: str) -> str:
    return f"<span class='notranslate' translate='no'>{txt}</span>"


# =========================
# Conexão Google Sheets
# =========================
SERVICE_EMAIL = None

def _load_sa_info():
    try:
        block = st.secrets["gcp_service_account"]
    except Exception as e:
        st.error("Não encontrei [gcp_service_account] no .streamlit/secrets.toml.")
        with st.expander("Detalhes"):
            st.exception(e)
        st.stop()

    if "json_path" in block:
        path = block["json_path"]
        if not os.path.isabs(path):
            path = os.path.join(os.path.dirname(__file__), path)
        try:
            with open(path, "r", encoding="utf-8") as f:
                info = json.load(f)
            return info, f"file:{path}"
        except Exception as e:
            st.error(f"Não consegui abrir o JSON: {path}")
            with st.expander("Detalhes"):
                st.exception(e)
            st.stop()

    return dict(block), "dict"

def make_client():
    global SERVICE_EMAIL
    info, _ = _load_sa_info()
    SERVICE_EMAIL = info.get("client_email", "(sem client_email)")
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scopes)
    return gspread.authorize(creds)


# ---- util: pegar ID de URL/ID
ID_RE = re.compile(r'/d/([a-zA-Z0-9-_]+)')

def extract_sheet_id(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    m = ID_RE.search(s)
    if m:
        return m.group(1)
    if re.fullmatch(r'[a-zA-Z0-9-_]{20,}', s):
        return s
    return None


# ---- helpers diversos
def parse_date_any(x):
    if pd.isna(x) or x == "":
        return pd.NaT
    s = str(x).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    try:
        return pd.to_datetime(s).date()
    except:
        return pd.NaT

def _upper_strip(x):
    return str(x).upper().strip() if pd.notna(x) else ""

def infer_year_month_from_sheet(sh_title: str, df_data: pd.DataFrame) -> Optional[str]:
    m = re.search(r'(\d{2})/(\d{4})', sh_title or "")
    if m:
        mm, yyyy = m.group(1), m.group(2)
        return f"{yyyy}-{mm}"
    if "DATA" in df_data.columns:
        d = df_data["DATA"].dropna()
        if len(d):
            try:
                dd = min(d)
                if isinstance(dd, date):
                    return f"{dd.year}-{dd.month:02d}"
            except:
                pass
    return None

def ym_to_label(ym: str) -> str:
    # "2026-01" -> "01/2026"
    if not ym or len(ym) != 7 or ym[4] != "-":
        return "—"
    return f"{ym[5:7]}/{ym[0:4]}"

def ym_key(ym: str) -> int:
    # "2026-01" -> 202601
    try:
        return int(ym.replace("-", ""))
    except:
        return -1


# =========================
# Lê UMA planilha de mês (dados + METAS) e devolve AAAA-MM
# =========================
@st.cache_data(show_spinner=False, ttl=300)
def read_one_sheet_cached(sheet_id: str) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    gs_client = make_client()
    return read_one_sheet(gs_client, sheet_id)

def read_one_sheet(gs_client, sheet_id: str) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    sh = gs_client.open_by_key(sheet_id)
    title = sh.title or sheet_id

    ws = sh.sheet1
    data = pd.DataFrame(ws.get_all_records())

    if not data.empty:
        data.columns = [c.strip().upper() for c in data.columns]
        col_unid  = "UNIDADE"   if "UNIDADE"   in data.columns else None
        col_data  = "DATA"      if "DATA"      in data.columns else None
        col_chas  = "CHASSI"    if "CHASSI"    in data.columns else None
        col_per   = "PERITO"    if "PERITO"    in data.columns else None
        col_dig   = "DIGITADOR" if "DIGITADOR" in data.columns else None

        req = [col_unid, col_data, col_chas, (col_per or col_dig)]
        if any(r is None for r in req):
            raise ValueError(f"Planilha {title}: precisa conter UNIDADE, DATA, CHASSI, PERITO/DIGITADOR.")

        data[col_unid] = data[col_unid].map(_upper_strip)
        data[col_chas] = data[col_chas].map(_upper_strip)
        data["__DATA__"] = data[col_data].apply(parse_date_any)

        # VISTORIADOR
        if col_per and col_dig:
            data["VISTORIADOR"] = np.where(
                data[col_per].astype(str).str.strip() != "",
                data[col_per].map(_upper_strip),
                data[col_dig].map(_upper_strip)
            )
        elif col_per:
            data["VISTORIADOR"] = data[col_per].map(_upper_strip)
        else:
            data["VISTORIADOR"] = data[col_dig].map(_upper_strip)

        # revistoria
        data = data.sort_values(["__DATA__", col_chas], kind="mergesort").reset_index(drop=True)
        data["__ORD__"] = data.groupby(col_chas).cumcount()
        data["IS_REV"] = (data["__ORD__"] >= 1).astype(int)

        # limpa unidades inválidas
        BAN_UNIDS = {"POSTO CÓDIGO", "POSTO CODIGO", "CÓDIGO", "CODIGO", "", "—", "NAN"}
        data = data[~data[col_unid].isin(BAN_UNIDS)].copy()

    # METAS (opcional)
    try:
        metas_ws = sh.worksheet("METAS")
        dfm = pd.DataFrame(metas_ws.get_all_records())
    except Exception:
        dfm = pd.DataFrame()

    if not dfm.empty:
        dfm.columns = [c.strip().upper() for c in dfm.columns]
        ren = {}
        for cand in ["META_MENSAL", "META MEN SAL", "META_MEN SAL", "META_MEN.SAL", "META MENSA"]:
            if cand in dfm.columns:
                ren[cand] = "META_MENSAL"
        for cand in ["DIAS UTEIS", "DIAS ÚTEIS", "DIAS_UTEIS"]:
            if cand in dfm.columns:
                ren[cand] = "DIAS_UTEIS"
        dfm = dfm.rename(columns=ren)

        if "VISTORIADOR" in dfm.columns:
            dfm["VISTORIADOR"] = dfm["VISTORIADOR"].map(_upper_strip)
        if "UNIDADE" in dfm.columns:
            dfm["UNIDADE"] = dfm["UNIDADE"].astype(str).map(_upper_strip)

        dfm["TIPO"] = dfm.get("TIPO", "").astype(str).map(_upper_strip)
        dfm["META_MENSAL"] = pd.to_numeric(dfm.get("META_MENSAL", 0), errors="coerce").fillna(0).astype(int)
        dfm["DIAS_UTEIS"]  = pd.to_numeric(dfm.get("DIAS_UTEIS", 0),  errors="coerce").fillna(0).astype(int)

    ym = infer_year_month_from_sheet(title, data.rename(columns={"__DATA__": "DATA"}) if "__DATA__" in data.columns else data)
    if ym is None:
        ym = "0000-00"

    if not data.empty:
        data["__YM__"] = data["__DATA__"].apply(lambda d: f"{d.year}-{d.month:02d}" if isinstance(d, date) else None)
    if not dfm.empty:
        dfm["__YM__"] = ym

    return data, dfm, title


# =========================
# Leitura da PLANILHA-ÍNDICE
# =========================
def _yes(v) -> bool:
    return str(v).strip().upper() in {"S", "SIM", "TRUE", "T", "1", "Y", "YES"}

@st.cache_data(show_spinner=False, ttl=120)
def load_ids_from_index_cached(index_sheet_id: str, tab_name: str) -> List[str]:
    gs_client = make_client()

    sh = gs_client.open_by_key(index_sheet_id)

    try:
        ws = sh.worksheet(tab_name)
    except Exception:
        tabs = [w.title for w in sh.worksheets()]
        alts = [t for t in tabs if t.strip().upper() == tab_name.strip().upper()]
        if alts:
            ws = sh.worksheet(alts[0])
        else:
            raise

    rows = ws.get_all_records()
    if not rows:
        return []

    norm = []
    for r in rows:
        rr = {}
        for k in r:
            rr[str(k).strip().upper()] = r[k]
        norm.append(rr)

    def pick_url(row: dict) -> str:
        for key in ["URL", "LINK", "PLANILHA", "ARQUIVO"]:
            if key in row and str(row.get(key) or "").strip():
                return str(row.get(key) or "")
        return ""

    def pick_ativo(row: dict) -> str:
        for key in ["ATIVO", "ATIVA", "STATUS"]:
            if key in row:
                return str(row.get(key) or "")
        return "S"

    ids = []
    for r in norm:
        if not _yes(pick_ativo(r)):
            continue
        sid = extract_sheet_id(pick_url(r))
        if sid:
            ids.append(sid)

    return ids


# =========================
# Entrada – múltiplas planilhas (via índice)
# =========================
with st.spinner("Conectando ao Google Sheets e lendo a planilha-índice..."):
    try:
        client = make_client()
    except Exception as e:
        st.error("Falha ao autenticar no Google (Service Account).")
        with st.expander("Detalhes"):
            st.exception(e)
        st.stop()

    st.caption(f"Conta de serviço: {SERVICE_EMAIL}")

    try:
        sheet_ids: List[str] = load_ids_from_index_cached(INDEX_SHEET_ID, INDEX_TAB_NAME)
    except Exception as e:
        st.error("Não consegui ler a planilha-índice. Motivo provável: falta de compartilhamento, aba inexistente, ou permissão.")
        with st.expander("Detalhes"):
            st.exception(e)
        st.stop()

if not sheet_ids:
    st.error("Não encontrei nenhum item ATIVO com URL/ID válido na aba ARQUIVOS da planilha-índice.")
    st.info("Verifique: (1) compartilhamento da planilha-índice com a conta de serviço; (2) coluna URL/LINK; (3) coluna ATIVO marcada como S.")
    st.stop()

with st.spinner(f"Lendo {len(sheet_ids)} planilha(s) do índice e consolidando..."):
    all_df, all_metas = [], []
    errors = []

    for sid in sheet_ids:
        try:
            dfi, dmf, _ = read_one_sheet_cached(sid)
            if not dfi.empty:
                all_df.append(dfi)
            if not dmf.empty:
                all_metas.append(dmf)
        except Exception as e:
            errors.append((sid, str(e)))

    if errors and not all_df:
        st.error("Não consegui montar dados de nenhuma planilha do índice.")
        with st.expander("Erros (por planilha)"):
            for sid, msg in errors[:50]:
                st.write(f"- {sid}: {msg}")
        st.stop()

    if errors:
        with st.expander("Algumas planilhas falharam (clique para ver)"):
            for sid, msg in errors[:50]:
                st.write(f"- {sid}: {msg}")

df = pd.concat(all_df, ignore_index=True)
df_metas_all = pd.concat(all_metas, ignore_index=True) if len(all_metas) else pd.DataFrame()


# =========================
# Continuação
# =========================
orig_cols = [c for c in df.columns]
col_unid  = "UNIDADE" if "UNIDADE" in orig_cols else None
col_chassi= "CHASSI"  if "CHASSI"  in orig_cols else None

if not col_unid or not col_chassi or "__DATA__" not in df.columns or "VISTORIADOR" not in df.columns:
    st.error("Base consolidada não contém as colunas necessárias (UNIDADE, CHASSI, __DATA__, VISTORIADOR).")
    st.stop()


# =========================
# Estado / Callbacks dos filtros
# =========================
def _init_state():
    st.session_state.setdefault("unids_tmp", [])
    st.session_state.setdefault("vists_tmp", [])
_init_state()

unidades_opts = sorted([u for u in df[col_unid].dropna().unique()])
vist_opts = sorted([v for v in df["VISTORIADOR"].dropna().unique() if v])

def cb_sel_all_vists():
    st.session_state.vists_tmp = vist_opts[:]
    st.rerun()

def cb_clear_vists():
    st.session_state.vists_tmp = []
    st.rerun()

def cb_sel_all_unids():
    st.session_state.unids_tmp = unidades_opts[:]
    st.rerun()

def cb_clear_unids():
    st.session_state.unids_tmp = []
    st.rerun()


# =========================
# Filtros (UI)
# =========================
st.subheader("Filtros")

# Unidades
colU1, colU2 = st.columns([4,2])
with colU1:
    st.multiselect("Unidades", options=unidades_opts, key="unids_tmp", help="Selecione as unidades desejadas")
with colU2:
    b1, b2 = st.columns(2)
    b1.button("Selecionar todas (Unid.)", use_container_width=True, on_click=cb_sel_all_unids)
    b2.button("Limpar (Unid.)", use_container_width=True, on_click=cb_clear_unids)

# Mês de referência + Período (dentro do mês)
datas_validas = [d for d in df["__DATA__"] if isinstance(d, date)]
if not datas_validas:
    st.error("Base sem datas válidas em __DATA__.")
    st.stop()

ser_datas = pd.Series(datas_validas)
ym_all = sorted(ser_datas.map(lambda d: f"{d.year}-{d.month:02d}").unique().tolist(), key=ym_key)
label_map = {f"{m[5:]}/{m[:4]}": m for m in ym_all}

sel_label = st.selectbox(
    "Mês de referência",
    options=list(label_map.keys()),
    index=len(ym_all) - 1
)
ym_sel = label_map[sel_label]
ref_year, ref_month = int(ym_sel[:4]), int(ym_sel[5:7])

datas_mes = [d for d in datas_validas if d.year == ref_year and d.month == ref_month]
min_d = min(datas_mes)
max_d = max(datas_mes)

drange = st.date_input(
    "Período (dentro do mês)",
    value=(min_d, max_d),
    min_value=min_d,
    max_value=max_d,
    format="DD/MM/YYYY",
    key="dt_range"
)
if isinstance(drange, tuple) and len(drange) == 2:
    start_d, end_d = drange
else:
    start_d, end_d = min_d, max_d

# Vistoriadores
colV1, colV2 = st.columns([4,2])
with colV1:
    st.multiselect("Vistoriadores", options=vist_opts, key="vists_tmp", help="Filtra pela(s) pessoa(s).")
with colV2:
    b3, b4 = st.columns(2)
    b3.button("Selecionar todos", use_container_width=True, on_click=cb_sel_all_vists)
    b4.button("Limpar", use_container_width=True, on_click=cb_clear_vists)


# =========================
# Aplicar filtros globais (VISUALIZAÇÕES DO MÊS)
# =========================
view = df.copy()

if st.session_state.unids_tmp:
    view = view[view[col_unid].isin(st.session_state.unids_tmp)]

# filtro pelo mês selecionado
view = view[view["__DATA__"].apply(lambda d: isinstance(d, date) and d.year == ref_year and d.month == ref_month)]

# filtro pelo período dentro do mês
view = view[(view["__DATA__"] >= start_d) & (view["__DATA__"] <= end_d)]

if st.session_state.vists_tmp:
    view = view[view["VISTORIADOR"].isin(st.session_state.vists_tmp)]

if view.empty:
    st.info("Nenhum registro para os filtros aplicados.")


# =========================
# KPIs
# =========================
vistorias_total   = int(len(view))
revistorias_total = int(view["IS_REV"].sum()) if not view.empty else 0
liq_total         = int(vistorias_total - revistorias_total)
pct_rev           = (100 * revistorias_total / vistorias_total) if vistorias_total else 0.0

cards = [
    ("Vistorias (geral)",   f"{vistorias_total:,}".replace(",", ".")),
    ("Vistorias líquidas",  f"{liq_total:,}".replace(",", ".")),
    (_nt("Revistorias"),    f"{revistorias_total:,}".replace(",", ".")),
    (_nt("% Revistorias"),  f"{pct_rev:,.1f}%".replace(",", "X").replace(".", ",").replace("X", ".")),
]
st.markdown(
    '<div class="card-container">' +
    "".join([f"<div class='card'><h4>{t}</h4><h2>{v}</h2></div>" for t, v in cards]) +
    "</div>",
    unsafe_allow_html=True
)


# =========================
# Resumo por Vistoriador
# =========================
st.markdown("<div class='section-title'>Resumo por Vistoriador</div>", unsafe_allow_html=True)

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

def _is_workday(d):
    return isinstance(d, date) and d.weekday() < 5

def _calc_wd_passados(df_view: pd.DataFrame) -> pd.DataFrame:
    if df_view.empty or "__DATA__" not in df_view.columns or "VISTORIADOR" not in df_view.columns:
        return pd.DataFrame(columns=["VISTORIADOR", "DIAS_PASSADOS"])
    mask = df_view["__DATA__"].apply(_is_workday)
    if not mask.any():
        vists = df_view["VISTORIADOR"].dropna().unique()
        return pd.DataFrame({"VISTORIADOR": vists, "DIAS_PASSADOS": np.zeros(len(vists), dtype=int)})
    out = (df_view.loc[mask].groupby("VISTORIADOR")["__DATA__"].nunique().reset_index().rename(columns={"__DATA__": "DIAS_PASSADOS"}))
    out["DIAS_PASSADOS"] = out["DIAS_PASSADOS"].astype(int)
    return out

wd_passados = _calc_wd_passados(view)
grp = grp.merge(wd_passados, on="VISTORIADOR", how="left").fillna({"DIAS_PASSADOS":0})
grp["DIAS_PASSADOS"] = grp["DIAS_PASSADOS"].astype(int)

# METAS (mês ref dentro do filtro)
ref_ym = ym_sel

if ref_ym and not df_metas_all.empty:
    metas_ref = df_metas_all[df_metas_all["__YM__"] == ref_ym].copy()
else:
    metas_ref = pd.DataFrame()

if not metas_ref.empty:
    metas_cols = [c for c in ["VISTORIADOR","UNIDADE","TIPO","META_MENSAL","DIAS_UTEIS"] if c in metas_ref.columns]
    grp = grp.merge(metas_ref[metas_cols], on="VISTORIADOR", how="left")
else:
    grp["UNIDADE"] = ""
    grp["TIPO"] = ""
    grp["META_MENSAL"] = 0
    grp["DIAS_UTEIS"]  = 0

for c in ["META_MENSAL","DIAS_UTEIS"]:
    grp[c] = pd.to_numeric(grp.get(c,0), errors="coerce").fillna(0)

grp["META_MENSAL"] = grp["META_MENSAL"].astype(int)
grp["DIAS_UTEIS"]  = grp["DIAS_UTEIS"].astype(int)

# cálculos
grp["META_DIA"] = np.where(grp["DIAS_UTEIS"]>0, grp["META_MENSAL"]/grp["DIAS_UTEIS"], 0.0)
grp["FALTANTE_MES"] = np.maximum(grp["META_MENSAL"] - grp["LIQUIDO"], 0)
grp["DIAS_RESTANTES"] = np.maximum(grp["DIAS_UTEIS"] - grp["DIAS_PASSADOS"], 0)
grp["NECESSIDADE_DIA"] = np.where(grp["DIAS_RESTANTES"]>0, grp["FALTANTE_MES"]/grp["DIAS_RESTANTES"], 0.0)
grp["MEDIA_DIA_ATUAL"] = np.where(grp["DIAS_PASSADOS"]>0, grp["LIQUIDO"]/grp["DIAS_PASSADOS"], 0.0)
grp["PROJECAO_MES"] = (grp["LIQUIDO"] + grp["MEDIA_DIA_ATUAL"] * grp["DIAS_RESTANTES"]).round(0)
grp["TENDENCIA_%"] = np.where(grp["META_MENSAL"]>0, (grp["PROJECAO_MES"]/grp["META_MENSAL"])*100, np.nan)

# normalização tipo + filtro só para tabela
grp["TIPO_NORM"] = grp.get("TIPO","").astype(str).str.upper().str.replace("MOVEL","MÓVEL").str.strip()
grp.loc[grp["TIPO_NORM"]=="", "TIPO_NORM"] = "—"

tipo_options = [t for t in ["FIXO","MÓVEL"] if t in grp["TIPO_NORM"].unique().tolist()]
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
grp_tbl = grp_tbl.sort_values(["PROJECAO_MES","LIQUIDO"], ascending=[False, False])
fmt = grp_tbl.copy()

def chip_tend(p):
    if pd.isna(p): return "—"
    p = float(p)
    if p >= 100: return f"{p:.0f}%"
    if p >= 95:  return f"{p:.0f}%"
    if p >= 85:  return f"{p:.0f}%"
    return f"{p:.0f}%"

def chip_nec(x):
    try:
        v = float(x)
    except:
        return "—"
    return "0" if v <= 0 else f"{int(round(v))}"

fmt["TIPO"] = fmt["TIPO_NORM"].map({"FIXO":"FIXO","MÓVEL":"MÓVEL"}).fillna("—")
fmt["META_MENSAL"]      = fmt["META_MENSAL"].map(lambda x: f"{int(x):,}".replace(",", "."))
fmt["DIAS_UTEIS"]       = fmt["DIAS_UTEIS"].map(lambda x: f"{int(x)}")
fmt["META_DIA"]         = fmt["META_DIA"].map(lambda x: f"{x:,.1f}".replace(",", "X").replace(".", ",").replace("X","."))
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


# =========================
# NOVA VISUALIZAÇÃO: HISTÓRICO DE META (STREAK)
# =========================
st.markdown("<div class='section-title'>Histórico de Meta (quem não bateu no mês selecionado)</div>", unsafe_allow_html=True)

# Base histórica respeitando Unidades + Vistoriadores (mas NÃO limita ao período do mês)
hist = df.copy()
if st.session_state.unids_tmp:
    hist = hist[hist[col_unid].isin(st.session_state.unids_tmp)]
if st.session_state.vists_tmp:
    hist = hist[hist["VISTORIADOR"].isin(st.session_state.vists_tmp)]

# Garante __YM__
if "__YM__" not in hist.columns or hist["__YM__"].isna().all():
    hist["__YM__"] = hist["__DATA__"].apply(lambda d: f"{d.year}-{d.month:02d}" if isinstance(d, date) else None)

hist = hist[pd.notna(hist["__YM__"])].copy()

if hist.empty:
    st.caption("Sem base histórica para calcular o histórico de metas.")
else:
    # produção mensal
    prod_m = (hist.groupby(["__YM__", "VISTORIADOR"], dropna=False)
              .agg(VISTORIAS=("IS_REV", "size"), REVISTORIAS=("IS_REV", "sum"))
              .reset_index())
    prod_m["LIQUIDO"] = prod_m["VISTORIAS"] - prod_m["REVISTORIAS"]

    # metas mensais (por vistoriador e mês)
    if df_metas_all.empty:
        metas_m = pd.DataFrame(columns=["__YM__", "VISTORIADOR", "TIPO", "META_MENSAL"])
    else:
        metas_m = df_metas_all.copy()
        metas_m["VISTORIADOR"] = metas_m.get("VISTORIADOR", "").astype(str).map(_upper_strip)
        metas_m["__YM__"] = metas_m.get("__YM__", "").astype(str)
        keep = [c for c in ["__YM__", "VISTORIADOR", "TIPO", "META_MENSAL"] if c in metas_m.columns]
        metas_m = metas_m[keep].copy()
        metas_m["META_MENSAL"] = pd.to_numeric(metas_m.get("META_MENSAL", 0), errors="coerce").fillna(0).astype(int)
        metas_m["TIPO"] = metas_m.get("TIPO", "").astype(str).map(_upper_strip).replace({"MOVEL":"MÓVEL"})
        metas_m = metas_m.drop_duplicates(subset=["__YM__", "VISTORIADOR"], keep="last")

    base_hist = prod_m.merge(metas_m, on=["__YM__", "VISTORIADOR"], how="left")
    base_hist["META_MENSAL"] = pd.to_numeric(base_hist.get("META_MENSAL", 0), errors="coerce").fillna(0).astype(int)
    base_hist["TIPO"] = base_hist.get("TIPO", "").astype(str).replace("", "—")
    base_hist["ATINGIU"] = np.where(base_hist["META_MENSAL"] > 0, base_hist["LIQUIDO"] >= base_hist["META_MENSAL"], np.nan)
    base_hist["ATINGIU_TXT"] = np.where(base_hist["META_MENSAL"] > 0, np.where(base_hist["ATINGIU"], "SIM", "NÃO"), "SEM META")

    # limita até o mês selecionado
    base_hist = base_hist[base_hist["__YM__"].apply(lambda x: ym_key(x) <= ym_key(ym_sel))].copy()

    # quem não bateu no mês selecionado (considerando o mês cheio)
    mes_ref = base_hist[base_hist["__YM__"] == ym_sel].copy()
    mes_ref = mes_ref[mes_ref["META_MENSAL"] > 0].copy()

    if mes_ref.empty:
        st.caption(f"Sem metas cadastradas (ou sem produção) para {ym_to_label(ym_sel)}.")
    else:
        mes_ref["NAO_BATEU"] = mes_ref["LIQUIDO"] < mes_ref["META_MENSAL"]
        alvo = mes_ref[mes_ref["NAO_BATEU"]].copy()

        if alvo.empty:
            st.caption(f"Ninguém ficou abaixo da meta em {ym_to_label(ym_sel)} (considerando LIQUIDO vs META).")
        else:
            # cálculo do streak (meses consecutivos sem bater meta, terminando em ym_sel)
            def calc_streak_for(vist: str) -> Tuple[int, Optional[str]]:
                sub = base_hist[(base_hist["VISTORIADOR"] == vist) & (base_hist["META_MENSAL"] > 0)].copy()
                if sub.empty:
                    return 0, None
                sub = sub.sort_values("__YM__", key=lambda s: s.map(ym_key))
                sub = sub[sub["__YM__"].apply(lambda x: ym_key(x) <= ym_key(ym_sel))].copy()
                if sub.empty:
                    return 0, None
                # precisa estar abaixo no mês selecionado para contar
                if not ((sub["__YM__"] == ym_sel) & (sub["LIQUIDO"] < sub["META_MENSAL"])).any():
                    return 0, None
                # percorre de trás para frente enquanto não bateu
                sub_rev = sub.sort_values("__YM__", key=lambda s: s.map(ym_key), ascending=False).copy()
                streak = 0
                inicio = None
                for _, r in sub_rev.iterrows():
                    if r["__YM__"] == ym_sel or streak > 0:
                        if r["LIQUIDO"] < r["META_MENSAL"]:
                            streak += 1
                            inicio = r["__YM__"]
                            continue
                        else:
                            break
                return int(streak), inicio

            rows = []
            for _, r in alvo.iterrows():
                vist = r["VISTORIADOR"]
                streak, inicio = calc_streak_for(vist)

                # últimos 6 meses (com meta) para contexto
                sub6 = base_hist[(base_hist["VISTORIADOR"] == vist) & (base_hist["META_MENSAL"] > 0)].copy()
                sub6 = sub6.sort_values("__YM__", key=lambda s: s.map(ym_key), ascending=False).head(6)
                sub6 = sub6.sort_values("__YM__", key=lambda s: s.map(ym_key))

                ult = []
                for _, rr in sub6.iterrows():
                    ult.append(f"{ym_to_label(rr['__YM__'])}: {int(rr['LIQUIDO'])}/{int(rr['META_MENSAL'])} ({rr['ATINGIU_TXT']})")
                ult_txt = " | ".join(ult) if ult else "—"

                rows.append({
                    "VISTORIADOR": vist,
                    "TIPO": str(r.get("TIPO", "—") or "—"),
                    "META_MENSAL": int(r["META_MENSAL"]),
                    "LIQUIDO": int(r["LIQUIDO"]),
                    "FALTANTE": int(max(r["META_MENSAL"] - r["LIQUIDO"], 0)),
                    "MESES_CONSECUTIVOS_SEM_META": streak,
                    "INICIO_DA_SEQUENCIA": ym_to_label(inicio) if inicio else "—",
                    "ULTIMOS_MESES (liq/meta)": ult_txt
                })

            hist_tbl = pd.DataFrame(rows)
            hist_tbl = hist_tbl.sort_values(
                ["MESES_CONSECUTIVOS_SEM_META", "FALTANTE"],
                ascending=[False, False]
            )

            st.dataframe(hist_tbl, use_container_width=True, hide_index=True)

            st.download_button(
                "Baixar histórico (CSV)",
                data=hist_tbl.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"historico_metas_{ym_sel}.csv",
                mime="text/csv"
            )

            st.markdown("#### Detalhar um vistoriador (histórico mensal)")
            v_choices = hist_tbl["VISTORIADOR"].unique().tolist()
            v_sel = st.selectbox("Vistoriador", options=v_choices, index=0, key="hist_vist_sel")

            subd = base_hist[(base_hist["VISTORIADOR"] == v_sel) & (base_hist["META_MENSAL"] > 0)].copy()
            subd = subd.sort_values("__YM__", key=lambda s: s.map(ym_key))
            subd["MES"] = subd["__YM__"].map(ym_to_label)
            subd["FALTANTE"] = np.maximum(subd["META_MENSAL"] - subd["LIQUIDO"], 0).astype(int)
            subd["STATUS"] = np.where(subd["LIQUIDO"] >= subd["META_MENSAL"], "BATEU", "NÃO BATEU")

            # streak acumulado (para enxergar a sequência mês a mês)
            streak_run = []
            cur = 0
            for _, rr in subd.iterrows():
                if rr["STATUS"] == "NÃO BATEU":
                    cur += 1
                else:
                    cur = 0
                streak_run.append(cur)
            subd["MESES_SEGUIDOS_SEM_BATER (até o mês)"] = streak_run

            show_cols = ["MES", "LIQUIDO", "META_MENSAL", "FALTANTE", "STATUS", "MESES_SEGUIDOS_SEM_BATER (até o mês)"]
            st.dataframe(subd[show_cols], use_container_width=True, hide_index=True)


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
    dup = (view.groupby(col_chassi, dropna=False)
                .agg(QTD=("VISTORIADOR","size"),
                     PRIMEIRA_DATA=("__DATA__", "min"),
                     ULTIMA_DATA=("__DATA__", "max"))
                .reset_index())
    dup = dup[dup["QTD"] >= 2].sort_values("QTD", ascending=False)
    if len(dup) == 0:
        st.caption("Nenhum chassi com múltiplas vistorias dentro dos filtros.")
    else:
        first_map = (view.sort_values(["__DATA__"])
                        .drop_duplicates(subset=[col_chassi], keep="first")
                        .set_index(col_chassi)["VISTORIADOR"].to_dict())
        last_map = (view.sort_values(["__DATA__"])
                        .drop_duplicates(subset=[col_chassi], keep="last")
                        .set_index(col_chassi)["VISTORIADOR"].to_dict())
        dup["PRIMEIRO_VIST"] = dup[col_chassi].map(first_map)
        dup["ULTIMO_VIST"]   = dup[col_chassi].map(last_map)
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

    if not df_metas_all.empty:
        metas_join = df_metas_all[df_metas_all["__YM__"] == f"{ref_ano}-{ref_mes:02d}"][["VISTORIADOR","TIPO","META_MENSAL"]].copy()
    else:
        metas_join = pd.DataFrame(columns=["VISTORIADOR","TIPO","META_MENSAL"])

    base_mes = prod_mes.merge(metas_join, on="VISTORIADOR", how="left")
    base_mes["TIPO"] = base_mes["TIPO"].astype(str).map(_upper_strip).replace({"MOVEL":"MÓVEL"}).replace("", "—")
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
        return f"{p:.0f}%"

    cards_mes = [
        ("Mês de referência", mes_label),
        ("Meta (soma)", f"{meta_tot:,}".replace(",", ".")),
        ("Vistorias (geral)", f"{vist_tot:,}".replace(",", ".")),
        (_nt("Revistorias"), f"{rev_tot:,}".replace(",", ".")),
        ("Líquido", f"{liq_tot:,}".replace(",", ".")),
        ("% Ating. (sobre geral)", chip_pct(ating_g)),
    ]
    st.markdown(
        '<div class="card-container">' +
        "".join([f"<div class='card'><h4>{t}</h4><h2>{v}</h2></div>" for t, v in cards_mes]) +
        "</div>",
        unsafe_allow_html=True
    )

    def chip_pct_row(p):
        if pd.isna(p): return "—"
        p = float(p)
        return f"{p:.0f}%"

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
        top_fmt = pd.DataFrame({
            "Vistoriador": top["VISTORIADOR"],
            "Meta (mês)": top["META_MENSAL"].map(lambda x: f"{int(x):,}".replace(",", ".")),
            "Vistorias (geral)": top["VISTORIAS"].map(int),
            "Revistorias": top["REVISTORIAS"].map(int),
            "Líquido": top["LIQUIDO"].map(int),
            "% Ating. (geral/meta)": top["ATING_%"].map(chip_pct_row),
        })

        bot = rk.tail(5).sort_values("ATING_%", ascending=True).copy()
        bot_fmt = pd.DataFrame({
            "Vistoriador": bot["VISTORIADOR"],
            "Meta (mês)": bot["META_MENSAL"].map(lambda x: f"{int(x):,}".replace(",", ".")),
            "Vistorias (geral)": bot["VISTORIAS"].map(int),
            "Revistorias": bot["REVISTORIAS"].map(int),
            "Líquido": bot["LIQUIDO"].map(int),
            "% Ating. (geral/meta)": bot["ATING_%"].map(chip_pct_row),
        })

        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**{_nt('TOP BOX')} — {mes_label}**", unsafe_allow_html=True)
            st.dataframe(top_fmt, use_container_width=True, hide_index=True)
        with c2:
            st.markdown(f"**{_nt('BOTTOM BOX')} — {mes_label}**", unsafe_allow_html=True)
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
    rank_day = st.date_input("Dia para o ranking", value=st.session_state.get("rank_day_sel", default_day),
                             format="DD/MM/YYYY", key="rank_day_sel")

    if rank_day in dates_avail:
        used_day = rank_day
        info_msg = None
    else:
        cands = [d for d in dates_avail if d <= rank_day]
        used_day = cands[-1] if cands else dates_avail[-1]
        info_msg = f"Sem dados em {rank_day.strftime('%d/%m/%Y')}. Exibindo {used_day.strftime('%d/%m/%Y')}."

    dia_label = used_day.strftime("%d/%m/%Y")
    if info_msg:
        st.caption(info_msg)
    st.caption(f"Dia exibido no ranking: {dia_label}")

    view_dia = view[view["__DATA__"] == used_day].copy()

    prod_dia = (view_dia.groupby("VISTORIADOR", dropna=False)
                .agg(VISTORIAS_DIA=("IS_REV", "size"),
                     REVISTORIAS_DIA=("IS_REV", "sum")).reset_index())
    prod_dia["LIQUIDO_DIA"] = prod_dia["VISTORIAS_DIA"] - prod_dia["REVISTORIAS_DIA"]

    ym_day = f"{used_day.year}-{used_day.month:02d}"
    if not df_metas_all.empty:
        metas_join = df_metas_all[df_metas_all["__YM__"] == ym_day][["VISTORIADOR","TIPO","META_MENSAL","DIAS_UTEIS"]].copy()
    else:
        metas_join = pd.DataFrame(columns=["VISTORIADOR","TIPO","META_MENSAL","DIAS_UTEIS"])

    base_dia = prod_dia.merge(metas_join, on="VISTORIADOR", how="left")
    base_dia["TIPO"] = base_dia["TIPO"].astype(str).str.upper().replace({"MOVEL":"MÓVEL"}).replace("", "—")
    for c in ["META_MENSAL","DIAS_UTEIS"]:
        base_dia[c] = pd.to_numeric(base_dia.get(c,0), errors="coerce").fillna(0)

    base_dia["META_DIA"] = np.where(base_dia["DIAS_UTEIS"]>0, base_dia["META_MENSAL"]/base_dia["DIAS_UTEIS"], 0.0)
    base_dia["ATING_DIA_%"] = np.where(base_dia["META_DIA"]>0, (base_dia["VISTORIAS_DIA"]/base_dia["META_DIA"])*100, np.nan)

    def chip_pct_row_dia(p):
        if pd.isna(p): return "—"
        p = float(p)
        return f"{p:.0f}%"

    def render_ranking_dia(df_sub, titulo):
        if df_sub.empty:
            st.caption(f"Sem dados para {titulo} em {dia_label}.")
            return
        rk = df_sub[df_sub["META_DIA"] > 0].copy()
        if rk.empty:
            st.caption(f"Ninguém com META do dia cadastrada para {titulo}.")
            return

        rk = rk.sort_values("ATING_DIA_%", ascending=False)

        top = rk.head(5).copy()
        top_fmt = pd.DataFrame({
            "Vistoriador": top["VISTORIADOR"],
            "Meta (dia)": top["META_DIA"].map(lambda x: int(round(x))),
            "Vistorias (dia)": top["VISTORIAS_DIA"].map(int),
            "Revistorias": top["REVISTORIAS_DIA"].map(int),
            "Líquido (dia)": top["LIQUIDO_DIA"].map(int),
            "% Ating. (dia)": top["ATING_DIA_%"].map(chip_pct_row_dia),
        })

        bot = rk.tail(5).sort_values("ATING_DIA_%", ascending=True).copy()
        bot_fmt = pd.DataFrame({
            "Vistoriador": bot["VISTORIADOR"],
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

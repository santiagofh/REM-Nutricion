from io import BytesIO
from pathlib import Path
import pandas as pd
import streamlit as st


st.set_page_config(page_title="Dashboard REM Nutricion", layout="wide")

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = BASE_DIR / "output"
DEFAULT_DEIS_PATH = BASE_DIR / "data_DEIS" / "20250424_est_deis.csv"

CAT_ORDER = ["NORMAL", "RIESGO", "DESNUTRIDO", "SOBREPESO", "OBESO"]
CAT_LABELS = {
    "NORMAL": "Normal",
    "RIESGO": "Riesgo",
    "DESNUTRIDO": "Desnutrido",
    "SOBREPESO": "Sobrepeso",
    "OBESO": "Obeso",
}


def _clean_text_col(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().replace("nan", pd.NA)


@st.cache_data(show_spinner=False)
def load_summary(output_dir: str, year: int, level: str) -> pd.DataFrame:
    file_map = {
        "SS": f"{year}_estado_nutricional_por_SS.xlsx",
        "comuna": f"{year}_estado_nutricional_por_comuna.xlsx",
        "est": f"{year}_estado_nutricional_por_est.xlsx",
    }
    path = Path(output_dir) / file_map[level]
    if not path.exists():
        raise FileNotFoundError(f"No existe: {path}")

    df = pd.read_excel(path, sheet_name="Resumen")
    df.columns = [str(c).strip() for c in df.columns]

    if "nombre_ss" in df.columns:
        df["nombre_ss"] = _clean_text_col(df["nombre_ss"])
    if "nombre_comuna" in df.columns:
        df["nombre_comuna"] = _clean_text_col(df["nombre_comuna"])
    if "nombre_establecimiento" in df.columns:
        df["nombre_establecimiento"] = _clean_text_col(df["nombre_establecimiento"])
    return df


@st.cache_data(show_spinner=False)
def load_deis_mappings(deis_path: str):
    path = Path(deis_path)
    if not path.exists():
        return None, None

    df = pd.read_csv(path, sep=";")
    need_cols = [
        "Nombre Comuna",
        "Nombre Dependencia Jerárquica (SEREMI / Servicio de Salud)",
        "Nombre Oficial",
    ]
    if any(c not in df.columns for c in need_cols):
        return None, None

    d = df[need_cols].copy()
    d.columns = ["nombre_comuna", "nombre_ss", "nombre_establecimiento"]
    for c in d.columns:
        d[c] = _clean_text_col(d[c])

    # Comuna -> SS (elige el SS mas frecuente por comuna)
    comuna_ss = (
        d.dropna(subset=["nombre_comuna", "nombre_ss"])
        .groupby(["nombre_comuna", "nombre_ss"], as_index=False)
        .size()
        .sort_values(["nombre_comuna", "size"], ascending=[True, False])
        .drop_duplicates(subset=["nombre_comuna"])
        .loc[:, ["nombre_comuna", "nombre_ss"]]
    )

    # (Comuna, Establecimiento) -> SS
    est_ss = (
        d.dropna(subset=["nombre_comuna", "nombre_establecimiento", "nombre_ss"])
        .groupby(["nombre_comuna", "nombre_establecimiento", "nombre_ss"], as_index=False)
        .size()
        .sort_values(["nombre_comuna", "nombre_establecimiento", "size"], ascending=[True, True, False])
        .drop_duplicates(subset=["nombre_comuna", "nombre_establecimiento"])
        .loc[:, ["nombre_comuna", "nombre_establecimiento", "nombre_ss"]]
    )
    return comuna_ss, est_ss


def detect_groups(columns) -> list[str]:
    groups = []
    for c in columns:
        c = str(c)
        if c.endswith("_total"):
            groups.append(c[:-6])
    return sorted(groups, key=lambda x: (x.lower() != "1m-6a", x))


def available_categories(df: pd.DataFrame, group: str) -> list[str]:
    out = []
    for cat in CAT_ORDER:
        if f"{group}_{cat}" in df.columns:
            out.append(cat)
    return out


def weighted_pct(df: pd.DataFrame, total_col: str, pct_col: str):
    sub = df[[total_col, pct_col]].dropna()
    if sub.empty:
        return None
    total = sub[total_col].sum()
    if total == 0:
        return None
    return float((sub[total_col] * sub[pct_col]).sum() / total)


def style_pct_bold(df: pd.DataFrame):
    pct_cols = [c for c in df.columns if str(c).endswith("(%)")]
    styler = df.style
    if pct_cols:
        styler = styler.format({c: "{:.2f}%" for c in pct_cols}, na_rep="")

    def _style_col(col: pd.Series):
        if col.name in pct_cols:
            return ["font-weight: 700"] * len(col)
        return [""] * len(col)

    return styler.apply(_style_col, axis=0).set_table_styles(
        [{"selector": "th", "props": [("font-weight", "700")]}]
    )


st.title("Dashboard REM Nutricion")
st.caption("Selecciona año, nivel, población/rango y estado nutricional.")

output_dir = str(DEFAULT_OUTPUT_DIR)
deis_path = str(DEFAULT_DEIS_PATH)

with st.sidebar:
    st.header("Filtros")
    year = st.selectbox("Año", [2024, 2025], index=1)
    level_map = {
        "Servicio de Salud": "SS",
        "Comuna": "comuna",
        "Establecimiento": "est",
    }
    level_label = st.selectbox("Nivel", list(level_map.keys()), index=0)
    level = level_map[level_label]

try:
    df = load_summary(output_dir, year, level)
except Exception as exc:
    st.error(f"No se pudo cargar datos: {exc}")
    st.stop()

comuna_ss_map, est_ss_map = load_deis_mappings(deis_path)

if level == "comuna":
    if comuna_ss_map is not None:
        df = df.merge(comuna_ss_map, on="nombre_comuna", how="left")
    else:
        df["nombre_ss"] = pd.NA

if level == "est":
    if est_ss_map is not None and {"nombre_comuna", "nombre_establecimiento"}.issubset(df.columns):
        df = df.merge(est_ss_map, on=["nombre_comuna", "nombre_establecimiento"], how="left")
    else:
        df["nombre_ss"] = pd.NA

groups = detect_groups(df.columns)
if not groups:
    st.error("No se detectaron columnas de grupo (ej: '*_total').")
    st.stop()

with st.sidebar:
    states = []
    for cat in CAT_ORDER:
        if any((f"{g}_{cat}" in df.columns) or (f"{g}_pct_{cat}" in df.columns) for g in groups):
            states.append(cat)
    if not states:
        st.error("No se detectaron estados nutricionales en la base.")
        st.stop()
    group_choice = st.selectbox("Poblacion / rango", ["Todos"] + groups, index=0)
    state_choice = st.selectbox(
        "Estado nutricional",
        ["Todos"] + states,
        index=0,
        format_func=lambda x: "Todos" if x == "Todos" else CAT_LABELS.get(x, x),
    )
    show_only_pct = st.checkbox("Mostrar solo porcentajes", value=False)

# Filtros jerarquicos
work = df.copy()
if "nombre_ss" in work.columns:
    ss_opts = sorted([x for x in work["nombre_ss"].dropna().unique().tolist()])
    if ss_opts:
        selected_ss = st.multiselect("Filtrar SS", ss_opts, default=ss_opts)
        work = work[work["nombre_ss"].isin(selected_ss)]

if level in ("comuna", "est") and "nombre_comuna" in work.columns:
    comuna_opts = sorted([x for x in work["nombre_comuna"].dropna().unique().tolist()])
    if comuna_opts:
        selected_comuna = st.multiselect("Filtrar comuna", comuna_opts, default=comuna_opts)
        work = work[work["nombre_comuna"].isin(selected_comuna)]

if level == "est" and "nombre_establecimiento" in work.columns:
    est_opts = sorted([x for x in work["nombre_establecimiento"].dropna().unique().tolist()])
    if est_opts:
        selected_est = st.multiselect("Filtrar establecimiento", est_opts, default=est_opts)
        work = work[work["nombre_establecimiento"].isin(selected_est)]

if work.empty:
    st.warning("Sin filas luego de aplicar filtros.")
    st.stop()

id_cols = []
if level == "SS":
    id_cols = ["nombre_ss"]
elif level == "comuna":
    id_cols = ["nombre_ss", "nombre_comuna"] if "nombre_ss" in work.columns else ["nombre_comuna"]
else:
    id_cols = ["nombre_ss", "nombre_comuna", "nombre_establecimiento"] if "nombre_ss" in work.columns else ["nombre_comuna", "nombre_establecimiento"]

selected_groups = groups if group_choice == "Todos" else [group_choice]
selected_states = states if state_choice == "Todos" else [state_choice]

cols_to_show = list(id_cols)
rename_map = {}
single_group = len(selected_groups) == 1

for g in selected_groups:
    g_total = f"{g}_total"
    if g_total in work.columns:
        cols_to_show.append(g_total)
        rename_map[g_total] = "Total bajo control" if single_group else f"{g} - Total bajo control"

    for st_cat in selected_states:
        g_n = f"{g}_{st_cat}"
        g_pct = f"{g}_pct_{st_cat}"
        st_label = CAT_LABELS.get(st_cat, st_cat)

        if g_n in work.columns:
            cols_to_show.append(g_n)
            rename_map[g_n] = f"{st_label} (N)" if single_group else f"{g} - {st_label} (N)"
        if g_pct in work.columns:
            cols_to_show.append(g_pct)
            rename_map[g_pct] = f"{st_label} (%)" if single_group else f"{g} - {st_label} (%)"

cols_to_show = [c for c in dict.fromkeys(cols_to_show) if c in work.columns]
display = work[cols_to_show].copy().rename(columns=rename_map)

for c in display.columns:
    if c.endswith("(%)"):
        display[c] = (display[c] * 100).round(2)

if show_only_pct:
    pct_cols_only = [c for c in display.columns if c.endswith("(%)")]
    keep_cols = [c for c in id_cols if c in display.columns] + pct_cols_only
    if keep_cols:
        display = display[keep_cols]

left, right = st.columns(2)
with left:
    st.metric("Filas visibles", len(display))
with right:
    total_cols_display = [c for c in display.columns if c.endswith("Total bajo control")]
    if total_cols_display:
        total_sum = display[total_cols_display].fillna(0).sum().sum()
        st.metric("Total bajo control (suma)", f"{int(total_sum):,}".replace(",", "."))

st.subheader("Tabla")
st.dataframe(style_pct_bold(display), use_container_width=True, hide_index=True)

st.subheader("Resumen rapido (porcentajes ponderados)")
rows = []
for g in selected_groups:
    g_total = f"{g}_total"
    for st_cat in selected_states:
        g_pct = f"{g}_pct_{st_cat}"
        if g_total in work.columns and g_pct in work.columns:
            val = weighted_pct(work, g_total, g_pct)
            rows.append(
                {
                    "Poblacion / rango": g,
                    "Estado nutricional": CAT_LABELS.get(st_cat, st_cat),
                    "Porcentaje ponderado (%)": None if val is None else round(val * 100, 2),
                }
            )

if rows:
    summary_df = pd.DataFrame(rows)
    st.dataframe(style_pct_bold(summary_df), use_container_width=True, hide_index=True)
else:
    st.info("No hay suficientes columnas para calcular ponderados.")

download_suffix = f"{group_choice}_{state_choice}".replace(" ", "_")

excel_buffer = BytesIO()
with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
    display.to_excel(writer, index=False, sheet_name="tabla_filtrada")
excel_buffer.seek(0)

st.download_button(
    "Descargar tabla filtrada (Excel)",
    data=excel_buffer.getvalue(),
    file_name=f"dashboard_{year}_{level}_{download_suffix}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

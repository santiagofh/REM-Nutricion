# %% ------------------------------------------------------------
# 1. Librerías y helpers
# ---------------------------------------------------------------
import pandas as pd
import unicodedata

def _norm(txt: str) -> str:
    txt = str(txt).strip().lower()
    return unicodedata.normalize("NFD", txt).encode("ascii", "ignore").decode()

def sumar_cat_por_ss(df, cat_norm, base, sufijo):
    df_cat  = df[df["Categoria"].apply(_norm) == cat_norm]
    colsval = df_cat.select_dtypes(include="number").columns.difference(
        ["Unnamed: 0","IdEstablecimiento","nombre_establecimiento","IdComuna",
         "nombre_comuna","IdServicio","nombre_ss","CodigoPrestacion","Ano","Mes"]
    )
    out = df_cat.groupby("nombre_ss")[colsval].sum()
    out[f"{base}{sufijo}"] = out.sum(axis=1)
    return out[[f"{base}{sufijo}"]]

def sumar_total_por_ss(df, base):
    colsval = df.select_dtypes(include="number").columns.difference(
        ["Unnamed: 0","IdEstablecimiento","nombre_establecimiento","IdComuna",
         "nombre_comuna","IdServicio","nombre_ss","CodigoPrestacion","Ano","Mes"]
    )
    out = df.groupby("nombre_ss")[colsval].sum()
    out[f"{base}_total"] = out.sum(axis=1)
    return out[[f"{base}_total"]]

# %% ------------------------------------------------------------
# 2. Cargar REM
# ---------------------------------------------------------------
xls_ninos        = pd.ExcelFile("output/REM_2024_P2_SECCION_A_A1_NIÑOS.xlsx")
xls_gestantes    = pd.ExcelFile("output/REM_2024_P1_SECCION_D_GESTANTES.xlsx")
xls_nodrizas     = pd.ExcelFile("output/REM_2024_P1_SECCION_D_NODRIZAS.xlsx")
xls_mayores      = pd.ExcelFile("output/REM_2024_P5_SECCION_B_ADULTO_MAYOR.xlsx")
xls_adolescentes = pd.ExcelFile("output/REM_2024_P9_SECCION_A_ADOLESCENTES.xlsx")

dfs_ninos = {
    sh: xls_ninos.parse(sh)
    for sh in ["1m-4a", "4-6a", "5-9a", "1m-6a"]
}

bases = [
    ("1m-4a",        dfs_ninos["1m-4a"]),
    ("4-6a",         dfs_ninos["4-6a"]),
    ("5-9a",         dfs_ninos["5-9a"]),
    ("1m-6a",        dfs_ninos["1m-6a"]),
    ("Gestantes",    xls_gestantes.parse("Sheet1")),
    ("Nodrizas",     xls_nodrizas.parse("Sheet1")),
    ("Persona_Mayor",xls_mayores.parse("Sheet1")),
    ("Adolescente",  xls_adolescentes.parse("Sheet1")),
]

categorias = [
    ("NORMAL",              "normal",                "_NORMAL"),
    ("RIESGO_DE_DESNUTRIR", "riesgo de desnutrir",   "_RIESGO"),
    ("DESNUTRIDO",        "desnutrido",          "_DESNUTRIDO"),
    ("SOBREPESO",           "sobrepeso",             "_SOBREPESO"),
    ("OBESIDAD",            "obesidad",              "_OBESO"),
]

# %% ------------------------------------------------------------
# 3. Tabla RESUMEN (totales + todas las categorías)
# ---------------------------------------------------------------
tabla_final = None
# Totales
for base, df in bases:
    tabla_final = (
        sumar_total_por_ss(df, base)
        if tabla_final is None else
        tabla_final.join(sumar_total_por_ss(df, base))
    )

# Categorías
for _, cat_norm, sufijo in categorias:
    frs = [sumar_cat_por_ss(df, cat_norm, base, sufijo) for base, df in bases]
    bloque = frs[0]
    for fr in frs[1:]:
        bloque = bloque.join(fr)
    tabla_final = tabla_final.join(bloque)

# Porcentajes
for base, _ in bases:
    for _, _, sufijo in categorias:
        tabla_final[f"{base}_pct{sufijo}"] = (
            tabla_final[f"{base}{sufijo}"] /
            tabla_final[f"{base}_total"]
        )

tabla_final.columns = tabla_final.columns.str.replace(" ", "_", regex=False)
# %% ------------------------------------------------------------
# 3.1 Reordenar columnas en tabla_final
# ---------------------------------------------------------------
orden_columnas = []
for base, _ in bases:
    orden_columnas.append(f"{base}_total")
    for _, _, sufijo in categorias:
        orden_columnas.append(f"{base}{sufijo}")
        orden_columnas.append(f"{base}_pct{sufijo}")

# Reordenar las columnas en tabla_final
tabla_final = tabla_final[orden_columnas]
# %% ------------------------------------------------------------
# 4. Exportar a Excel: una hoja Resumen + una hoja por categoría
# ---------------------------------------------------------------
with pd.ExcelWriter("output/2024_estado_nutricional_por_SS.xlsx",
                    engine="xlsxwriter",
                    datetime_format="yyyy-mm-dd") as writer:

    # Hoja global
    tabla_final.to_excel(writer, sheet_name="Resumen")

    # Hoja por categoría
    for legible, _, sufijo in categorias:
        hoja = legible.title().replace("_", " ")  # “Normal”, “Riesgo De Desnutrir”, …
        cols = [c for c in tabla_final.columns
                if c.endswith("_total") or
                   c.endswith(sufijo)   or
                   c.endswith(f"pct{sufijo}")]
        tabla_final[cols].to_excel(writer, sheet_name=hoja)

print("OK Archivo 'estado_nutricional_por_SS.xlsx' creado con 1+5 hojas.")



#%%
import pandas as pd
import numpy as np
import os
import glob
codigos_de_interes = [
"P2060000",
"P2070501",
"P2070502",
"P2070503",
"P2070504",
"P2070505",
"P2070506",
"P2501800",
"P2400150",
"P2400300",
"P2400310",
"P2400320",
"P2400330",
"P2400340",
"P2400350",
"P2400360",
"P2501801",
]

path = r"C:\Users\fariass\OneDrive - SUBSECRETARIA DE SALUD PUBLICA\Escritorio\DATA\REM\REM_2024\Datos\SerieP2024.csv"

chunk_size = 50000  
filtered_data = pd.DataFrame()
for chunk in pd.read_csv(path, sep=";", chunksize=chunk_size):
    filtered_chunk = chunk[(chunk['CodigoPrestacion'].isin(codigos_de_interes)) & (chunk['Mes'] == 12) & (chunk['IdRegion']==13)]
    filtered_data = pd.concat([filtered_data, filtered_chunk])
#%%
# Cargar base diaria de establecimientos (último CSV disponible)
base_est = r"C:\Users\fariass\OneDrive - SUBSECRETARIA DE SALUD PUBLICA\Escritorio\DATA\ESTABLECIMIENTOS"
files_est = sorted(glob.glob(os.path.join(base_est, "establecimientos_*.csv")))
if not files_est:
    raise FileNotFoundError(f"No se encontraron CSV en {base_est}")
path_est = files_est[-1]

df_deis = pd.read_csv(path_est, sep=';')
df_deis.columns = df_deis.columns.str.strip()

df_deis['codigo_establecimiento'] = df_deis['EstablecimientoCodigo']
df_deis['nombre_ss'] = df_deis['SeremiSaludGlosa_ServicioDeSaludGlosa']
df_deis['nombre_establecimiento'] = df_deis['EstablecimientoGlosa']
df_deis['nombre_comuna'] = df_deis['ComunaGlosa']

df_deis.drop_duplicates(subset=['codigo_establecimiento'], inplace=True)
ls_codigo_ss=list(df_deis.codigo_establecimiento)
ls_nombre_ss=list(df_deis.nombre_ss)
dict_ss = dict(zip(ls_codigo_ss, ls_nombre_ss))
ls_codigo_est=list(df_deis.codigo_establecimiento)
ls_nombre_est=list(df_deis.nombre_establecimiento)
dict_establecimiento= dict(zip(ls_codigo_ss, ls_nombre_est))
ls_codigo_comuna=list(df_deis.codigo_establecimiento)
ls_nombre_comuna=list(df_deis.nombre_comuna)
dict_comuna= dict(zip(ls_codigo_ss, ls_nombre_comuna))
filtered_data['nombre_ss']=filtered_data.IdEstablecimiento.map(dict_ss)
filtered_data['nombre_establecimiento']=filtered_data.IdEstablecimiento.map(dict_establecimiento)
filtered_data['nombre_comuna']=filtered_data.IdEstablecimiento.map(dict_comuna)

# Ahora filtered_data contiene solo las filas con los códigos de prestación deseados y en el
#%%
# Seccion A
seccion_p2_a={
    "P2060000": "TOTAL DE NIÑOS EN CONTROL",
    "P2070501": "DIAGNOSTICO NUTRICIONAL INTEGRADO - RIESGO DE DESNUTRIR/ DEFICIT PONDERAL*",
    "P2070502": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRIDO",
    "P2070503": "DIAGNOSTICO NUTRICIONAL INTEGRADO - SOBREPESO / RIESGO OBESIDAD",
    "P2070504": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO",
    "P2070505": "DIAGNOSTICO NUTRICIONAL INTEGRADO - NORMAL",
    "P2070506": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRICIÓN SECUNDARIA",
    "P2501800": "NIÑOS SIN EVALUACIÓN NUTRICIONAL POR CURSO DE VIDA (RECIÉN NACIDOS) O POR CONDICIÓN ESPECIAL DE SALUD"
}

columnas_seccion_p2_a={
    "Col01": "TOTAL - Ambos sexos",
    "Col02": "TOTAL - Hombres",
    "Col03": "TOTAL - Mujeres",
    "Col04": "Menor de 1 mes - Hombres",
    "Col05": "Menor de 1 mes - Mujeres",
    "Col06": "1 mes - Hombres",
    "Col07": "1 mes - Mujeres",
    "Col08": "2 meses - Hombres",
    "Col09": "2 meses - Mujeres",
    "Col10": "3 meses - Hombres",
    "Col11": "3 meses - Mujeres",
    "Col12": "4 meses - Hombres",
    "Col13": "4 meses - Mujeres",
    "Col14": "5 meses - Hombres",
    "Col15": "5 meses - Mujeres",
    "Col16": "6 meses - Hombres",
    "Col17": "6 meses - Mujeres",
    "Col18": "7 a 11 meses - Hombres",
    "Col19": "7 a 11 meses - Mujeres",
    "Col20": "12 a 17 meses - Hombres",
    "Col21": "12 a 17 meses - Mujeres",
    "Col22": "18 a 23 meses - Hombres",
    "Col23": "18 a 23 meses - Mujeres",
    "Col24": "24 a 35 meses - Hombres",
    "Col25": "24 a 35 meses - Mujeres",
    "Col26": "36 a 41 meses - Hombres",
    "Col27": "36 a 41 meses - Mujeres",
    "Col28": "42 a 47 meses - Hombres",
    "Col29": "42 a 47 meses - Mujeres",
    "Col30": "48 a 59 meses - Hombres",
    "Col31": "48 a 59 meses - Mujeres",
    "Col32": "Pueblos Originarios - Hombres",
    "Col33": "Pueblos Originarios - Mujeres",
    "Col34": "Población Migrantes - Hombres",
    "Col35": "Población Migrantes - Mujeres"
}

# Seccion A1
seccion_p2_a1 = {
    "P2400150": "TOTAL DE NIÑOS EN CONTROL",
    "P2400300": "DIAGNOSTICO NUTRICIONAL INTEGRADO - RIESGO DE DESNUTRIR/ DEFICIT PONDERAL*",
    "P2400310": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRIDO",
    "P2400320": "DIAGNOSTICO NUTRICIONAL INTEGRADO - SOBREPESO / RIESGO OBESIDAD",
    "P2400330": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO",
    "P2400340": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO SEVERO",
    "P2400350": "DIAGNOSTICO NUTRICIONAL INTEGRADO - NORMAL",
    "P2400360": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRICIÓN SECUNDARIA",
    "P2501801": "NIÑOS SIN EVALUACIÓN NUTRICIONAL POR CURSO DE VIDA (RECIÉN NACIDOS) O POR CONDICIÓN ESPECIAL DE SALUD"
}

columnas_seccion_p2_a1={
    "Col01": "TOTAL - Ambos sexos",
    "Col02": "TOTAL - Hombres",
    "Col03": "TOTAL - Mujeres",
    "Col04": "60 a 71 meses - Hombres",
    "Col05": "60 a 71 meses - Mujeres",
    "Col06": "6 a 6 años 11 meses - Hombres",
    "Col07": "6 a 6 años 11 meses - Mujeres",
    "Col08": "7 a 7 años 11 meses - Hombres",
    "Col09": "7 a 7 años 11 meses - Mujeres",
    "Col10": "8 a 8 años 11 meses - Hombres",
    "Col11": "8 a 8 años 11 meses - Mujeres",
    "Col12": "9 a 9 años 11 meses - Hombres",
    "Col13": "9 a 9 años 11 meses - Mujeres",
    "Col14": "Pueblos Originarios - Hombres",
    "Col15": "Pueblos Originarios - Mujeres",
    "Col16": "Migrantes - Hombres",
    "Col17": "Migrantes - Mujeres"
}

###
#%%
categoria_map = {
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - NORMAL": "Normal",
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - RIESGO DE DESNUTRIR/ DEFICIT PONDERAL*": "Riesgo de Desnutrir",
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRIDO": "Desnutrido",
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRICIÓN SECUNDARIA": "Desnutrido",  # Agrupado con desnutrido
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - SOBREPESO / RIESGO OBESIDAD": "Sobrepeso",
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO": "Obesidad",
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO SEVERO": "Obesidad",  # Agrupado con obeso
}
def categorize(row):
    if row['Descripcion'] in categoria_map:
        return categoria_map[row['Descripcion']]
    return "Otra" 
# %% Sección: Niños de 4 a 6 años (48–59 y 60–71 meses)
# ---------------------------------------------------------
# Sección: Niños de 4 a 6 años (48–59 y 60–71 meses)
# ---------------------------------------------------------

# 1) Mapeos de códigos y columnas
seccion_4a6_a = {
    "P2070501": "DIAGNOSTICO NUTRICIONAL INTEGRADO - RIESGO DE DESNUTRIR/ DEFICIT PONDERAL*",
    "P2070502": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRIDO",
    "P2070503": "DIAGNOSTICO NUTRICIONAL INTEGRADO - SOBREPESO / RIESGO OBESIDAD",
    "P2070504": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO",
    "P2070505": "DIAGNOSTICO NUTRICIONAL INTEGRADO - NORMAL",
    "P2070506": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRICIÓN SECUNDARIA",
    "P2501800": "NIÑOS SIN EVALUACIÓN NUTRICIONAL POR CURSO DE VIDA (RECIÉN NACIDOS) O POR CONDICIÓN ESPECIAL DE SALUD"
}
columnas_4a6_a = {
    "Col30": "48 a 59 meses - Hombres",
    "Col31": "48 a 59 meses - Mujeres"
}

seccion_4a6_a1 = {
    "P2400300": "DIAGNOSTICO NUTRICIONAL INTEGRADO - RIESGO DE DESNUTRIR/ DEFICIT PONDERAL*",
    "P2400310": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRIDO",
    "P2400320": "DIAGNOSTICO NUTRICIONAL INTEGRADO - SOBREPESO / RIESGO OBESIDAD",
    "P2400330": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO",
    "P2400340": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO SEVERO",
    "P2400350": "DIAGNOSTICO NUTRICIONAL INTEGRADO - NORMAL",
    "P2400360": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRICIÓN SECUNDARIA",
    "P2501801": "NIÑOS SIN EVALUACIÓN NUTRICIONAL POR CURSO DE VIDA (RECIÉN NACIDOS) O POR CONDICIÓN ESPECIAL DE SALUD"
}
columnas_4a6_a1 = {
    "Col04": "60 a 71 meses - Hombres",
    "Col05": "60 a 71 meses - Mujeres"
}

# 2) Filtrado y transformación: 48–59 meses
df_4a6_a = (
    filtered_data
    .loc[filtered_data['CodigoPrestacion'].isin(seccion_4a6_a.keys())]
    .assign(
        Descripcion=lambda d: d['CodigoPrestacion'].map(seccion_4a6_a),
        Categoria=lambda d: d.apply(categorize, axis=1)
    )
    .rename(columns=columnas_4a6_a)
)
# Eliminamos columnas irrelevantes
df_4a6_a.drop(columns=['Mes','IdRegion','IdComuna','IdServicio'], inplace=True)

# 3) Filtrado y transformación: 60–71 meses
df_4a6_a1 = (
    filtered_data
    .loc[filtered_data['CodigoPrestacion'].isin(seccion_4a6_a1.keys())]
    .assign(
        Descripcion=lambda d: d['CodigoPrestacion'].map(seccion_4a6_a1),
        Categoria=lambda d: d.apply(categorize, axis=1)
    )
    .rename(columns=columnas_4a6_a1)
)
df_4a6_a1.drop(columns=['Mes','IdRegion','IdComuna','IdServicio'], inplace=True)

# 4) Unión de ambos rangos de edad
df_4a6 = (
    df_4a6_a
    .merge(
        df_4a6_a1,
        on=['Ano','IdEstablecimiento','Descripcion','nombre_establecimiento','nombre_comuna','nombre_ss','Categoria'],
        how='outer'
    )
)

# 5) Reordenar columnas
column_order_4a6 = [
    'Ano', 'IdEstablecimiento',
    'nombre_establecimiento', 'nombre_comuna', 'nombre_ss',
    'Descripcion', 'Categoria',
    '48 a 59 meses - Hombres', '48 a 59 meses - Mujeres',
    '60 a 71 meses - Hombres', '60 a 71 meses - Mujeres'
]
df_4a6 = df_4a6[column_order_4a6]

# Ahora df_4a6 contiene únicamente los registros de niños de 4 a 6 años,
# con sus diagnósticos y conteos desagregados por sexo y rango de edad.

# %% Sección: Niños de 5 años a 9 años 11 meses
# ---------------------------------------------------------
# Sección: Niños de 5 años a 9 años 11 meses
# (60–71 meses y 6–9 años, desagregado por sexo y subrangos de edad)
# ---------------------------------------------------------

# 1) Definir el mapeo de prestaciones y columnas de interés
seccion_5a9 = {
    "P2400300": "DIAGNOSTICO NUTRICIONAL INTEGRADO - RIESGO DE DESNUTRIR/ DEFICIT PONDERAL*",
    "P2400310": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRIDO",
    "P2400320": "DIAGNOSTICO NUTRICIONAL INTEGRADO - SOBREPESO / RIESGO OBESIDAD",
    "P2400330": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO",
    "P2400340": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO SEVERO",
    "P2400350": "DIAGNOSTICO NUTRICIONAL INTEGRADO - NORMAL",
    "P2400360": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRICIÓN SECUNDARIA",
    "P2501801": "NIÑOS SIN EVALUACIÓN NUTRICIONAL POR CURSO DE VIDA (RECIÉN NACIDOS) O POR CONDICIÓN ESPECIAL DE SALUD"
}

columnas_5a9 = {
    # 60–71 meses (5 a <6 años)
    "Col04": "60 a 71 meses - Hombres",
    "Col05": "60 a 71 meses - Mujeres",
    # 6–9 años 11 meses (agrupado)
    "Col06": "6 a 6 años 11 meses - Hombres",
    "Col07": "6 a 6 años 11 meses - Mujeres",
    # desglose por año completo (opcional)
    "Col08": "7 a 7 años 11 meses - Hombres",
    "Col09": "7 a 7 años 11 meses - Mujeres",
    "Col10": "8 a 8 años 11 meses - Hombres",
    "Col11": "8 a 8 años 11 meses - Mujeres",
    "Col12": "9 a 9 años 11 meses - Hombres",
    "Col13": "9 a 9 años 11 meses - Mujeres"
}

# 2) Filtrar y transformar
df_5a9 = (
    filtered_data
    .loc[filtered_data['CodigoPrestacion'].isin(seccion_5a9.keys())]
    .assign(
        Descripcion=lambda d: d['CodigoPrestacion'].map(seccion_5a9),
        Categoria=lambda d: d.apply(categorize, axis=1)
    )
    .rename(columns=columnas_5a9)
)

# 3) Eliminar columnas auxiliares
df_5a9.drop(columns=['Mes', 'IdRegion', 'IdComuna', 'IdServicio'], inplace=True)

# 4) Reordenar columnas para presentación
column_order_5a9 = [
    'Ano', 'IdEstablecimiento',
    'nombre_establecimiento', 'nombre_comuna', 'nombre_ss',
    'CodigoPrestacion', 'Descripcion', 'Categoria',
    '60 a 71 meses - Hombres', '60 a 71 meses - Mujeres',
    '6 a 6 años 11 meses - Hombres', '6 a 6 años 11 meses - Mujeres',
    '7 a 7 años 11 meses - Hombres',  '7 a 7 años 11 meses - Mujeres',
    '8 a 8 años 11 meses - Hombres',  '8 a 8 años 11 meses - Mujeres',
    '9 a 9 años 11 meses - Hombres',  '9 a 9 años 11 meses - Mujeres'
]
df_5a9 = df_5a9[column_order_5a9]
#%% Sección: Niños de 1 mes hasta menores de 4 años (1–47 meses)
#%% Sección: Niños Menores de 6 años y mayores de 1 mes
# 
columnas_seccion_p2_a_MenoresDe6Años={
    "Col06": "1 mes - Hombres",
    "Col07": "1 mes - Mujeres",
    "Col08": "2 meses - Hombres",
    "Col09": "2 meses - Mujeres",
    "Col10": "3 meses - Hombres",
    "Col11": "3 meses - Mujeres",
    "Col12": "4 meses - Hombres",
    "Col13": "4 meses - Mujeres",
    "Col14": "5 meses - Hombres",
    "Col15": "5 meses - Mujeres",
    "Col16": "6 meses - Hombres",
    "Col17": "6 meses - Mujeres",
    "Col18": "7 a 11 meses - Hombres",
    "Col19": "7 a 11 meses - Mujeres",
    "Col20": "12 a 17 meses - Hombres",
    "Col21": "12 a 17 meses - Mujeres",
    "Col22": "18 a 23 meses - Hombres",
    "Col23": "18 a 23 meses - Mujeres",
    "Col24": "24 a 35 meses - Hombres",
    "Col25": "24 a 35 meses - Mujeres",
    "Col26": "36 a 41 meses - Hombres",
    "Col27": "36 a 41 meses - Mujeres",
    "Col28": "42 a 47 meses - Hombres",
    "Col29": "42 a 47 meses - Mujeres",
    "Col30": "48 a 59 meses - Hombres",
    "Col31": "48 a 59 meses - Mujeres"
}
columnas_seccion_p2_a1_MenoresDe6Años={
    "Col04": "60 a 71 meses - Hombres",
    "Col05": "60 a 71 meses - Mujeres"
}
seccion_p2_a_MenoresDe6Años={
    "P2070501": "DIAGNOSTICO NUTRICIONAL INTEGRADO - RIESGO DE DESNUTRIR/ DEFICIT PONDERAL*",
    "P2070502": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRIDO",
    "P2070503": "DIAGNOSTICO NUTRICIONAL INTEGRADO - SOBREPESO / RIESGO OBESIDAD",
    "P2070504": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO",
    "P2070505": "DIAGNOSTICO NUTRICIONAL INTEGRADO - NORMAL",
    "P2070506": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRICIÓN SECUNDARIA",
    "P2501800": "NIÑOS SIN EVALUACIÓN NUTRICIONAL POR CURSO DE VIDA (RECIÉN NACIDOS) O POR CONDICIÓN ESPECIAL DE SALUD"
}
seccion_p2_a1_MenoresDe6Años = {
    "P2400300": "DIAGNOSTICO NUTRICIONAL INTEGRADO - RIESGO DE DESNUTRIR/ DEFICIT PONDERAL*",
    "P2400310": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRIDO",
    "P2400320": "DIAGNOSTICO NUTRICIONAL INTEGRADO - SOBREPESO / RIESGO OBESIDAD",
    "P2400330": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO",
    "P2400340": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO SEVERO",
    "P2400350": "DIAGNOSTICO NUTRICIONAL INTEGRADO - NORMAL",
    "P2400360": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRICIÓN SECUNDARIA",
    "P2501801": "NIÑOS SIN EVALUACIÓN NUTRICIONAL POR CURSO DE VIDA (RECIÉN NACIDOS) O POR CONDICIÓN ESPECIAL DE SALUD	"
 }

# Filtrar y renombrar columnas para sección A
df_menores_de_6_a = filtered_data[filtered_data['CodigoPrestacion'].isin(seccion_p2_a_MenoresDe6Años.keys())]
df_menores_de_6_a['Descripcion'] = df_menores_de_6_a['CodigoPrestacion'].map(seccion_p2_a_MenoresDe6Años)
df_menores_de_6_a["Categoria"] = df_menores_de_6_a.apply(categorize, axis=1)
df_menores_de_6_a.rename(columns=columnas_seccion_p2_a_MenoresDe6Años, inplace=True)  # Key: Renombrar columnas A
df_menores_de_6_a = df_menores_de_6_a.loc[:, ~df_menores_de_6_a.columns.str.startswith('Col')]
col=['IdComuna',
'IdServicio',
'IdRegion']
df_menores_de_6_a=df_menores_de_6_a.drop(columns=col)
df_menores_de_6_a1 = filtered_data[filtered_data['CodigoPrestacion'].isin(seccion_p2_a1_MenoresDe6Años.keys())]
df_menores_de_6_a1['Descripcion'] = df_menores_de_6_a1['CodigoPrestacion'].map(seccion_p2_a1_MenoresDe6Años)
df_menores_de_6_a1["Categoria"] = df_menores_de_6_a1.apply(categorize, axis=1)
df_menores_de_6_a1.rename(columns=columnas_seccion_p2_a1_MenoresDe6Años, inplace=True)  # Key: Renombrar columnas A1
df_menores_de_6_a1 = df_menores_de_6_a1.loc[:, ~df_menores_de_6_a1.columns.str.startswith('Col')]
col=['IdComuna',
'IdServicio',
'IdRegion']
df_menores_de_6_a1=df_menores_de_6_a1.drop(columns=col)
df_menores_de_6 = df_menores_de_6_a.merge(
    df_menores_de_6_a1,
    how='outer',
    on=['Mes','Ano','IdEstablecimiento', 'Descripcion', 'nombre_establecimiento','nombre_comuna','nombre_ss'],
    suffixes=['_a', '_a1']
)
df_menores_de_6['Categoria'] = df_menores_de_6['Categoria_a'].fillna(df_menores_de_6['Categoria_a1'])

# Filtrar filas donde ambos valores no son nulos y son diferentes
diferencias = df_menores_de_6[
    df_menores_de_6['Categoria_a'].notna() &
    df_menores_de_6['Categoria_a1'].notna() &
    (df_menores_de_6['Categoria_a'] != df_menores_de_6['Categoria_a1'])
]

# Mostrar filas con diferencias
print(diferencias[['Categoria_a', 'Categoria_a1']].drop_duplicates())
#%%

column_order = [
    'Ano', 'Mes', 'IdEstablecimiento',
    # Información nominal
    'nombre_establecimiento', 'nombre_comuna', 'nombre_ss',
    # Prestación y descripción
    'CodigoPrestacion_a', 'CodigoPrestacion_a1', 'Descripcion',
    # Categorías
    'Categoria',
    # Datos por edad y sexo (de menor a mayor)
    '1 mes - Hombres', '1 mes - Mujeres',
    '2 meses - Hombres', '2 meses - Mujeres',
    '3 meses - Hombres', '3 meses - Mujeres',
    '4 meses - Hombres', '4 meses - Mujeres',
    '5 meses - Hombres', '5 meses - Mujeres',
    '6 meses - Hombres', '6 meses - Mujeres',
    '7 a 11 meses - Hombres', '7 a 11 meses - Mujeres',
    '12 a 17 meses - Hombres', '12 a 17 meses - Mujeres',
    '18 a 23 meses - Hombres', '18 a 23 meses - Mujeres',
    '24 a 35 meses - Hombres', '24 a 35 meses - Mujeres',
    '36 a 41 meses - Hombres', '36 a 41 meses - Mujeres',
    '42 a 47 meses - Hombres', '42 a 47 meses - Mujeres',
    '48 a 59 meses - Hombres', '48 a 59 meses - Mujeres',
    '60 a 71 meses - Hombres', '60 a 71 meses - Mujeres',
]
df_menores_de_6 = df_menores_de_6[column_order]


# ---------------------------------------------------------
# Sección: Niños de 1 mes hasta menores de 4 años (1–47 meses)
# ---------------------------------------------------------

# 1) Mapeo de prestaciones y descripciones
seccion_1m_4a = {
    "P2070501": "DIAGNOSTICO NUTRICIONAL INTEGRADO - RIESGO DE DESNUTRIR/ DEFICIT PONDERAL*",
    "P2070502": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRIDO",
    "P2070503": "DIAGNOSTICO NUTRICIONAL INTEGRADO - SOBREPESO / RIESGO OBESIDAD",
    "P2070504": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO",
    "P2070505": "DIAGNOSTICO NUTRICIONAL INTEGRADO - NORMAL",
    "P2070506": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRICIÓN SECUNDARIA",
    "P2501800": "NIÑOS SIN EVALUACIÓN NUTRICIONAL POR CURSO DE VIDA (RECIÉN NACIDOS) O POR CONDICIÓN ESPECIAL DE SALUD"
}

# 2) Columnas de interés: de 1 a 47 meses, por sexo
columnas_1m_4a = {
    "Col06": "1 mes - Hombres",
    "Col07": "1 mes - Mujeres",
    "Col08": "2 meses - Hombres",
    "Col09": "2 meses - Mujeres",
    "Col10": "3 meses - Hombres",
    "Col11": "3 meses - Mujeres",
    "Col12": "4 meses - Hombres",
    "Col13": "4 meses - Mujeres",
    "Col14": "5 meses - Hombres",
    "Col15": "5 meses - Mujeres",
    "Col16": "6 meses - Hombres",
    "Col17": "6 meses - Mujeres",
    "Col18": "7 a 11 meses - Hombres",
    "Col19": "7 a 11 meses - Mujeres",
    "Col20": "12 a 17 meses - Hombres",
    "Col21": "12 a 17 meses - Mujeres",
    "Col22": "18 a 23 meses - Hombres",
    "Col23": "18 a 23 meses - Mujeres",
    "Col24": "24 a 35 meses - Hombres",
    "Col25": "24 a 35 meses - Mujeres",
    "Col26": "36 a 41 meses - Hombres",
    "Col27": "36 a 41 meses - Mujeres",
    "Col28": "42 a 47 meses - Hombres",
    "Col29": "42 a 47 meses - Mujeres"
}

# 3) Filtrar, asignar descripción y categoría, renombrar columnas
df_1m_4a = (
    filtered_data
    .loc[filtered_data['CodigoPrestacion'].isin(seccion_1m_4a.keys())]
    .assign(
        Descripcion=lambda d: d['CodigoPrestacion'].map(seccion_1m_4a),
        Categoria=lambda d: d.apply(categorize, axis=1)
    )
    .rename(columns=columnas_1m_4a)
)

# 4) Eliminar columnas auxiliares
df_1m_4a.drop(columns=['Mes', 'IdRegion', 'IdComuna', 'IdServicio'], inplace=True)

# 5) Reordenar columnas para la salida
column_order_1m_4a = [
    'Ano', 'IdEstablecimiento',
    'nombre_establecimiento', 'nombre_comuna', 'nombre_ss',
    'CodigoPrestacion', 'Descripcion', 'Categoria',
    '1 mes - Hombres',     '1 mes - Mujeres',
    '2 meses - Hombres',   '2 meses - Mujeres',
    '3 meses - Hombres',   '3 meses - Mujeres',
    '4 meses - Hombres',   '4 meses - Mujeres',
    '5 meses - Hombres',   '5 meses - Mujeres',
    '6 meses - Hombres',   '6 meses - Mujeres',
    '7 a 11 meses - Hombres', '7 a 11 meses - Mujeres',
    '12 a 17 meses - Hombres','12 a 17 meses - Mujeres',
    '18 a 23 meses - Hombres','18 a 23 meses - Mujeres',
    '24 a 35 meses - Hombres','24 a 35 meses - Mujeres',
    '36 a 41 meses - Hombres','36 a 41 meses - Mujeres',
    '42 a 47 meses - Hombres','42 a 47 meses - Mujeres'
]
df_1m_4a = df_1m_4a[column_order_1m_4a]

# Resultado: df_1m_4a con niños de 1 mes hasta menores de 4 años,
# desagregado por sexo y subrango de edad.
# %% Exportar resultados a un Excel con varias pestañas
output_path = r"output/REM_2024_P2_SECCION_A_A1_NIÑOS.xlsx"

with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
    # Niños de 1 mes hasta menores de 4 años
    df_1m_4a.to_excel(writer, sheet_name='1m-4a', index=False)
    
    # Niños de 4 a 6 años
    df_4a6.to_excel(writer, sheet_name='4-6a', index=False)
    
    # Niños de 5 a 9 años 11 meses
    df_5a9.to_excel(writer, sheet_name='5-9a', index=False)
    
    # Niños menores de 6 años (1–71 meses)
    df_menores_de_6.to_excel(writer, sheet_name='1m-6a', index=False)

print(f"Archivo Excel generado en: {output_path}")

# %%



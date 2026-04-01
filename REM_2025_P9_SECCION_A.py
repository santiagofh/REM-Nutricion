#%%
import pandas as pd
import numpy as np
import os
import glob
codigos_de_interes = [
"P9100250",
"P9100260",
"P9100270",
"P9100280",
"P9100290",
"P9100300",
"P9100310",
]

path = r"C:\Users\fariass\OneDrive - SUBSECRETARIA DE SALUD PUBLICA\Escritorio\DATA\REM\REM_2025\Datos\SerieP2025.csv"

chunk_size = 50000  
filtered_data = pd.DataFrame()
for chunk in pd.read_csv(path, sep=";", chunksize=chunk_size):
    filtered_chunk = chunk[(chunk['CodigoPrestacion'].isin(codigos_de_interes)) & (chunk['Mes'] == 12) & (chunk['IdRegion']==13)]
    filtered_data = pd.concat([filtered_data, filtered_chunk])
#%%
df_deis = pd.read_csv('data_DEIS/20250424_est_deis.csv', sep=';')
df_deis.columns = df_deis.columns.str.strip()
df_deis['codigo_establecimiento'] = df_deis['Código Vigente']
df_deis['nombre_ss'] = df_deis['Nombre Dependencia Jerárquica (SEREMI / Servicio de Salud)']
df_deis['nombre_establecimiento'] = df_deis['Nombre Oficial']
df_deis['nombre_comuna'] = df_deis['Nombre Comuna']
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

#%%
Seccion_p9_a = {
    "P9100100": "TOTAL ADOLESCENTES EN CONTROL",
    "P9100250": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DEFICIT PONDERALO BAJO PESO",
    "P9100260": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRIDO",
    "P9100270": "DIAGNOSTICO NUTRICIONAL INTEGRADO - SOBREPESO / RIESGO OBESIDAD",
    "P9100280": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO",
    "P9100290": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO SEVERO",
    "P9100300": "DIAGNOSTICO NUTRICIONAL INTEGRADO - NORMAL O EUTROFIA",
    "P9100310": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRICIÓN SECUNDARIA"
}

columnas_seccion_p9_a = {
    "Col01": "TOTAL - Ambos sexos",
    "Col02": "TOTAL - Hombres",
    "Col03": "TOTAL - Mujeres",
    "Col04": "Adolescentes 10 a 14 años - Ambos sexos",
    "Col05": "Adolescentes 10 a 14 años - Hombres",
    "Col06": "Adolescentes 10 a 14 años - Mujeres",
    "Col07": "Adolescentes 15 a 19 años - Ambos sexos",
    "Col08": "Adolescentes 15 a 19 años - Hombres",
    "Col09": "Adolescentes 15 a 19 años - Mujeres",
    "Col10": "Adolescentes de Pueblos Originarios - Ambos sexos",
    "Col11": "Adolescentes de Pueblos Originarios - Hombres",
    "Col12": "Adolescentes de Pueblos Originarios - Mujeres",
    "Col13": "Adolescentes Migrantes - Ambos sexos",
    "Col14": "Adolescentes Migrantes - Hombres",
    "Col15": "Adolescentes Migrantes - Mujeres"
}
###
# %%
categoria_map = {
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - DEFICIT PONDERALO BAJO PESO":"Riesgo de Desnutrir",
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRIDO":"Desnutrido",
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - SOBREPESO / RIESGO OBESIDAD":"Sobrepeso",
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO":"Obesidad",
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO SEVERO":"Obesidad",
    "DIAGNOSTICO NUTRICIONAL INTEGRADO - NORMAL O EUTROFIA":"Normal"
}
def categorize(row):
    if row['Descripcion'] in categoria_map:
        return categoria_map[row['Descripcion']]
    return "Otra" 
#%%
# Códigos de prestación para Diagnóstico Nutricional Integrado en adolescentes
diagnostico_nutricional_adolescentes = {
    "P9100250": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DEFICIT PONDERALO BAJO PESO",
    "P9100260": "DIAGNOSTICO NUTRICIONAL INTEGRADO - DESNUTRIDO",
    "P9100270": "DIAGNOSTICO NUTRICIONAL INTEGRADO - SOBREPESO / RIESGO OBESIDAD",
    "P9100280": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO",
    "P9100290": "DIAGNOSTICO NUTRICIONAL INTEGRADO - OBESO SEVERO",
    "P9100300": "DIAGNOSTICO NUTRICIONAL INTEGRADO - NORMAL O EUTROFIA"
}

# Asignar etiquetas a las columnas relevantes (generalizando si es necesario para simplificar)
columnas_diagnostico_nutricional = {
    # "Col01": "TOTAL - Ambos sexos",
    # "Col02": "TOTAL - Hombres",
    # "Col03": "TOTAL - Mujeres",
    # "Col04": "Adolescentes 10 a 14 años - Ambos sexos",
    "Col05": "Adolescentes 10 a 14 años - Hombres",
    "Col06": "Adolescentes 10 a 14 años - Mujeres",
    # "Col07": "Adolescentes 15 a 19 años - Ambos sexos",
    "Col08": "Adolescentes 15 a 19 años - Hombres",
    "Col09": "Adolescentes 15 a 19 años - Mujeres",
    # "Col10": "Adolescentes de Pueblos Originarios - Ambos sexos",
    # "Col11": "Adolescentes de Pueblos Originarios - Hombres",
    # "Col12": "Adolescentes de Pueblos Originarios - Mujeres",
    # "Col13": "Adolescentes Migrantes - Ambos sexos",
    # "Col14": "Adolescentes Migrantes - Hombres",
    # "Col15": "Adolescentes Migrantes - Mujeres"
}
# Filtrar los datos para diagnóstico nutricional integrado
df_adolescentes = filtered_data[filtered_data['CodigoPrestacion'].isin(diagnostico_nutricional_adolescentes.keys())]
df_adolescentes['Descripcion'] = df_adolescentes['CodigoPrestacion'].map(diagnostico_nutricional_adolescentes)

# Seleccionar y renombrar columnas
columnas_seleccionadas_diagnostico = ['IdEstablecimiento','nombre_establecimiento','IdComuna','nombre_comuna', 'IdServicio','nombre_ss', 'CodigoPrestacion', 'Descripcion'] + list(columnas_diagnostico_nutricional.keys())
df_adolescentes = df_adolescentes[columnas_seleccionadas_diagnostico]
df_adolescentes = df_adolescentes.rename(columns=columnas_diagnostico_nutricional)
df_adolescentes['Categoria'] = df_adolescentes.apply(categorize, axis=1)

# Mostrar resultados
print(df_adolescentes[['Descripcion', 'Categoria']])

# %%
df_adolescentes.to_excel('output/REM_2025_P9_SECCION_A_ADOLESCENTES.xlsx')
# %%




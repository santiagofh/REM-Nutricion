#%%
import pandas as pd
import numpy as np
import os
import glob
codigos_de_interes = [
"P1030101",
"P1030102",
"P1030103",
"P1030104",
"P1030201",
"P1030202",
"P1030203",
"P1030204",
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
#%%


# Seccion P1 D
Seccion_p1_d = {
    "P1030101": "GESTANTES EN CONTROL - OBESA",
    "P1030102": "GESTANTES EN CONTROL - SOBREPESO",
    "P1030103": "GESTANTES EN CONTROL - NORMAL",
    "P1030104": "GESTANTES EN CONTROL - BAJO PESO",
    "P1030201": "CONTROL AL 8º MES POST-PARTO - OBESA",
    "P1030202": "CONTROL AL 8º MES POST-PARTO - SOBREPESO",
    "P1030203": "CONTROL AL 8º MES POST-PARTO - NORMAL",
    "P1030204": "CONTROL AL 8º MES POST-PARTO - BAJO PESO"

}

columnas_seccion_p1_d = {
    "Col01": "total",
    "Col02": "Menos 15 años",
    "Col03": "15 a 19 años",
    "Col04": "20 a 24 años",
    "Col05": "25 a 29 años",
    "Col06": "30 a 34 años",
    "Col07": "35 a 39 años",
    "Col08": "40 a 44 años",
    "Col09": "45 a 49 años",
    "Col10": "50 a 54 años"
}

#%%
categoria_map = {
    # Gestantes
    "GESTANTES EN CONTROL - OBESA": "Obesidad",
    "GESTANTES EN CONTROL - SOBREPESO": "Sobrepeso",
    "GESTANTES EN CONTROL - NORMAL": "Normal",
    "GESTANTES EN CONTROL - BAJO PESO": "Riesgo de Desnutrir",
    "CONTROL AL 8º MES POST-PARTO - SOBREPESO":"Sobrepeso",
    "CONTROL AL 8º MES POST-PARTO - NORMAL":"Normal",
    "CONTROL AL 8º MES POST-PARTO - OBESA":"Obesidad",
    "CONTROL AL 8º MES POST-PARTO - SOBREPESO":"Sobrepeso",
    "CONTROL AL 8º MES POST-PARTO - NORMAL":"Normal",
    "CONTROL AL 8º MES POST-PARTO - BAJO PESO": "Riesgo de Desnutrir"
}
def categorize(row):
    if row['Descripcion'] in categoria_map:
        return categoria_map[row['Descripcion']]
    return "Otra" 
#%%
# %%
# Definición de los códigos de prestación para gestantes
seccion_p1_d_gestantes = {
    "P1030101": "GESTANTES EN CONTROL - OBESA",
    "P1030102": "GESTANTES EN CONTROL - SOBREPESO",
    "P1030103": "GESTANTES EN CONTROL - NORMAL",
    "P1030104": "GESTANTES EN CONTROL - BAJO PESO"
}

# Definición de las columnas específicas para la sección de gestantes
columnas_seccion_p1_d_gestantes = {
    # "Col01": "total",
    "Col02": "Menos 15 años",
    "Col03": "15 a 19 años",
    "Col04": "20 a 24 años",
    "Col05": "25 a 29 años",
    "Col06": "30 a 34 años",
    "Col07": "35 a 39 años",
    "Col08": "40 a 44 años",
    "Col09": "45 a 49 años",
    "Col10": "50 a 54 años"
}

# Filtrar los datos para gestantes
codigos_de_interes_gestantes = {**seccion_p1_d_gestantes}
columnas_de_interes_gestantes = {**columnas_seccion_p1_d_gestantes}

# Filtrar los datos
df_gestantes = filtered_data[filtered_data['CodigoPrestacion'].isin(codigos_de_interes_gestantes.keys())]
df_gestantes['Descripcion'] = df_gestantes['CodigoPrestacion'].map(codigos_de_interes_gestantes)

# Seleccionar y renombrar columnas
columnas_seleccionadas_gestantes = ['IdEstablecimiento','nombre_establecimiento','IdComuna','nombre_comuna', 'IdServicio','nombre_ss', 'CodigoPrestacion', 'Descripcion'] + list(columnas_de_interes_gestantes.keys())
df_gestantes = df_gestantes[columnas_seleccionadas_gestantes]
df_gestantes = df_gestantes.rename(columns=columnas_de_interes_gestantes)

# Calcular totales si es necesario y aplicar categorización
# Asumiendo que tenemos una función categorize() o una lógica de categorización similar
df_gestantes['Categoria'] = df_gestantes.apply(categorize, axis=1)  # Esto dependerá de cómo quieras categorizar las gestantes

# Mostrar resultados
print(df_gestantes[['Descripcion', 'Categoria']])

df_gestantes.to_excel('output/REM_2024_P1_SECCION_D_GESTANTES.xlsx')
#%%
# Códigos de prestación para nodrizas al 8º mes post parto
seccion_p1_d_nodrizas = {
    "P1030201": "CONTROL AL 8º MES POST-PARTO - OBESA",
    "P1030202": "CONTROL AL 8º MES POST-PARTO - SOBREPESO",
    "P1030203": "CONTROL AL 8º MES POST-PARTO - NORMAL",
    "P1030204": "CONTROL AL 8º MES POST-PARTO - BAJO PESO"
}

# Columnas específicas para la sección de nodrizas
columnas_seccion_p1_d_nodrizas = {
    # "Col01": "total",
    "Col02": "Menos 15 años",
    "Col03": "15 a 19 años",
    "Col04": "20 a 24 años",
    "Col05": "25 a 29 años",
    "Col06": "30 a 34 años",
    "Col07": "35 a 39 años",
    "Col08": "40 a 44 años",
    "Col09": "45 a 49 años",
    "Col10": "50 a 54 años"
}
# Filtrar los datos para nodrizas al 8º mes post parto
codigos_de_interes_nodrizas = {**seccion_p1_d_nodrizas}
columnas_de_interes_nodrizas = {**columnas_seccion_p1_d_nodrizas}

# Filtrar los datos
df_nodrizas = filtered_data[filtered_data['CodigoPrestacion'].isin(codigos_de_interes_nodrizas.keys())]
df_nodrizas['Descripcion'] = df_nodrizas['CodigoPrestacion'].map(codigos_de_interes_nodrizas)

# Seleccionar y renombrar columnas
columnas_seleccionadas_nodrizas = ['IdEstablecimiento','nombre_establecimiento','IdComuna','nombre_comuna', 'IdServicio','nombre_ss', 'CodigoPrestacion', 'Descripcion'] + list(columnas_de_interes_nodrizas.keys())
df_nodrizas = df_nodrizas[columnas_seleccionadas_nodrizas]
df_nodrizas = df_nodrizas.rename(columns=columnas_de_interes_nodrizas)

# Calcular totales si es necesario y aplicar categorización
# Aquí podrías añadir alguna lógica de categorización si fuera necesaria.
df_nodrizas['Categoria'] = df_nodrizas.apply(categorize, axis=1)  # Esto dependerá de cómo quieras categorizar las gestantes

# Mostrar resultados
print(df_nodrizas[['Descripcion', 'Categoria']])

df_nodrizas.to_excel('output/REM_2024_P1_SECCION_D_NODRIZAS.xlsx')
# %%



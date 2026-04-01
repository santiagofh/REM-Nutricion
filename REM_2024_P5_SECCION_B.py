#%%
import pandas as pd
import numpy as np
import os
import glob
codigos_de_interes = [
"P5190200",
"P5190300",
"P5190400",
"P5200100",
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
Seccion_p5_b = {
    "P5190200": "BAJO PESO",
    "P5190300": "NORMAL",
    "P5190400": "SOBREPESO",
    "P5200100": "OBESO"
}

columnas_seccion_p5_b = {
    "Col01": "TOTAL - Ambos sexos",
    "Col02": "TOTAL - Hombres",
    "Col03": "TOTAL - Mujeres",
    "Col04": "65 a 69 años - Hombres",
    "Col05": "65 a 69 años - Mujeres",
    "Col06": "70 a 74 años - Hombres",
    "Col07": "70 a 74 años - Mujeres",
    "Col08": "75 a 79 años - Hombres",
    "Col09": "75 a 79 años - Mujeres",
    "Col10": "80 a 84 años - Hombres",
    "Col11": "80 a 84 años - Mujeres",
    "Col12": "85 a 89 años - Hombres",
    "Col13": "85 a 89 años - Mujeres",
    "Col14": "90 a 94 años - Hombres",
    "Col15": "90 a 94 años - Mujeres",
    "Col16": "95 a 99 años - Hombres",
    "Col17": "95 a 99 años - Mujeres",
    "Col18": "100 y mas años - Hombres",
    "Col19": "100 y mas años - Mujeres",
    "Col20": "Pueblos Originarios - Hombres",
    "Col21": "Pueblos Originarios - Mujeres",
    "Col22": "Migrantes - Hombres",
    "Col23": "Migrantes - Mujeres",
    "Col24": "Personas Mayores en ELEAM - Hombres",
    "Col25": "Personas Mayores en ELEAM - Mujeres",
    
}
#%%
categoria_map = {
    #Adulto mayor
    "BAJO PESO": "Riesgo de Desnutrir",
    "NORMAL": "Normal",
    "SOBREPESO": "Sobrepeso",
    "OBESO": "Obesidad",
}
def categorize(row):
    if row['Descripcion'] in categoria_map:
        return categoria_map[row['Descripcion']]
    return "Otra" 
#%%
#%%
# Adulto Mayor (Persona Mayor) - P.5 - Sección B (Toda la Población)
seccion_p5_b_adulto_mayor = {
    "P5190200": "BAJO PESO",
    "P5190300": "NORMAL",
    "P5190400": "SOBREPESO",
    "P5200100": "OBESO"
}

# Definición de las columnas específicas para la sección de Adulto Mayor
columnas_seccion_p5_b = {
    # "Col01": "TOTAL - Ambos sexos",
    # "Col02": "TOTAL - Hombres",
    # "Col03": "TOTAL - Mujeres",
    "Col04": "65 a 69 años - Hombres",
    "Col05": "65 a 69 años - Mujeres",
    "Col06": "70 a 74 años - Hombres",
    "Col07": "70 a 74 años - Mujeres",
    "Col08": "75 a 79 años - Hombres",
    "Col09": "75 a 79 años - Mujeres",
    "Col10": "80 a 84 años - Hombres",
    "Col11": "80 a 84 años - Mujeres",
    "Col12": "85 a 89 años - Hombres",
    "Col13": "85 a 89 años - Mujeres",
    "Col14": "90 a 94 años - Hombres",
    "Col15": "90 a 94 años - Mujeres",
    "Col16": "95 a 99 años - Hombres",
    "Col17": "95 a 99 años - Mujeres",
    "Col18": "100 y más años - Hombres",
    "Col19": "100 y más años - Mujeres",
    # "Col20": "Pueblos Originarios - Hombres",
    # "Col21": "Pueblos Originarios - Mujeres",
    # "Col22": "Migrantes - Hombres",
    # "Col23": "Migrantes - Mujeres",
    # "Col24": "Personas Mayores en ELEAM - Hombres",
    # "Col25": "Personas Mayores en ELEAM - Mujeres"
}

# Filtrar los datos para Adulto Mayor
codigos_de_interes_adulto_mayor = {**seccion_p5_b_adulto_mayor}
columnas_de_interes_adulto_mayor = {**columnas_seccion_p5_b}

df_adulto_mayor = filtered_data[filtered_data['CodigoPrestacion'].isin(codigos_de_interes_adulto_mayor.keys())]
df_adulto_mayor['Descripcion'] = df_adulto_mayor['CodigoPrestacion'].map(seccion_p5_b_adulto_mayor)

# Seleccionar y renombrar columnas
columnas_seleccionadas_adulto_mayor = ['IdEstablecimiento','nombre_establecimiento','IdComuna','nombre_comuna', 'IdServicio','nombre_ss', 'CodigoPrestacion', 'Descripcion'] + list(columnas_de_interes_adulto_mayor.keys())
df_adulto_mayor = df_adulto_mayor[columnas_seleccionadas_adulto_mayor]
df_adulto_mayor = df_adulto_mayor.rename(columns=columnas_de_interes_adulto_mayor)

# Aplicar categorización y calcular totales si es necesario
df_adulto_mayor['Categoria'] = df_adulto_mayor.apply(categorize, axis=1)

# Mostrar resultados
print(df_adulto_mayor[['Descripcion', 'Categoria']])

#%%
df_adulto_mayor.to_excel('output/REM_2024_P5_SECCION_B_ADULTO_MAYOR.xlsx')
# %%



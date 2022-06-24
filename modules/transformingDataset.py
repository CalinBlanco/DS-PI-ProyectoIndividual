import pandas as pd
import numpy as np
import re
import json

import modules.utils as util
import shutil
import modules.createDataDictionary as dic_datos

f = open('modules/defaultDataDictionary.json', "r")
defaultJSON = json.load(f)

prepDatasets = "datasets/2_preprocessing_datasets/"
normalizeDatasets = "datasets/3_normalize_datasets/"
logDatasets = "datasets/4_log_datasets/"
reporteDicDatosNorma = "datasets/3_normalize_datasets/reportes/"

tipoDatoColumna = {}
listaIndexDrop = []

def iniciando_transformacion():
  prep = util.getArchivosDatasets(prepDatasets)
  norma = util.getArchivosDatasets(normalizeDatasets)

  print("Iniciando el transformación de los datasets...")

  if(prep == []):
    print("No hay dataset nuevos...")
  else:
    normalizandoDirectorios(prep, norma)
    print("Dataset normalizados...")

def normalizandoDirectorios(listaDirectorioPre, listaDirectorioNorma):
  if (listaDirectorioNorma == []):
    preNormalizarSoloDirectorioPreprocesado(listaDirectorioPre)
  else:
    preNormalizarConDirectorioNormalizado(listaDirectorioNorma, listaDirectorioPre)

def preNormalizarSoloDirectorioPreprocesado(listaDirectorioPre):
  global tipoDatoColumna
  for i, dset in enumerate(listaDirectorioPre):
    if(dset == "localidad.csv"):
      shutil.move(prepDatasets+dset, normalizeDatasets+ dset)
      continue
    tipoDatoColumna = {}
    df = cargar_dataset(prepDatasets,dset)
    df = init_df_transforming(df, dset)

    dataframe_to_csv(df,normalizeDatasets,dset)

  


def preNormalizarConDirectorioNormalizado(listaDirectorioNorma, listaDirectorioPre):
  return ""

def cargar_dataset(path, dSet):
  df = pd.read_csv(path + dSet, sep = ',', engine = 'python', encoding='utf8')
  return df

def dataframe_to_csv(dfFinal,path, dSet):
  dfFinal.to_csv(path+dSet.split('.')[0].lower()+'.csv',encoding = "UTF-8",index=False)



def init_df_transforming(dataF, dSet):
  # print("Iniciando init_df_transforming")
  df_transforming = change_column_name(dataF, dSet)
  # print("Iniciando change_column_name")
  df_transforming = drop_duplicate(df_transforming)
  # print("Iniciando drop_duplicate")
  df_transforming = delete_empty_column_row(df_transforming)
  # print("Iniciando delete_empty_column_row")
  df_transforming = detect_type_column(df_transforming)
  # print("Iniciando detect_type_column")
  df_transforming = change_datatype_column(df_transforming)
  # print("Iniciando change_datatype_column")
  # print("\n")

  ## Generando el diccionario de datos
  dd = dic_datos.create_data_dictionary()
  df_dd = dd.make_my_data_dictionary(df_transforming,dSet)
  util.generate_html(df_dd,dSet,reporteDicDatosNorma,"norma")
  return df_transforming

    
def change_column_name(dataF, dSet):
  lista_columnas = dataF.columns.values
  for i, e in enumerate(lista_columnas):
    if(dataF[e].isna().sum() != dataF.shape[0]):
      nomDataSet = dSet.split('.')[0]
      df_column = defaultJSON[nomDataSet][e].get("cambioColumna")
      new_df_column = defaultJSON[nomDataSet][e].get("descripcionColumna")
      getKey = list(defaultJSON[nomDataSet][e].keys())[1]
      # print("getKey",getKey)
      dataF.rename(columns={e:df_column}, inplace=True)
      generate_new_data_dictionary(dSet,e,getKey,new_df_column)
  return dataF
  
def generate_new_data_dictionary(dset,col,campo,content):
  dset = dset.split('.')[0]
  filename = 'newDataDictionary.json'
  
  newJSON = {
    dset.lower() : {
      col.lower() : {
        campo.lower() : content
      }
    }
  }
  with open(filename, 'w') as file:
    json.dump(newJSON, file, indent=2)

def delete_empty_column_row(dataF):
  dataF = dataF.dropna(how='all', axis=1)
  dataF = dataF.dropna(how='all', axis=0)
  return dataF


def detect_type_column(dataF):
  global tipoDatoColumna
  # print("Iniciando detect_type_column")
  # print("\n")
  # print(tipoDatoColumna)
  lista_columnas = dataF.columns.values
  # print(lista_columnas)

  # print("Empezando funcion detect_type_column")
  # print(lista_columnas)

  for i, col in enumerate(lista_columnas):
    listaAcertados = []
    listadoIndicesDrop = []

    if (dataF.dtypes[col] !=  'float64'):
      for i, valor in enumerate(dataF[col]):
        a,b = detectando_tipo_dato_valor(valor,"entero", i)
        listaAcertados.append(a)
        if (b != 0): listadoIndicesDrop.append(b)

      if listaAcertados.count(True) > listaAcertados.count(False):
        tipoDatoColumna[col] = 'int64'
        if len(listadoIndicesDrop) != 0:
          dataF = delete_fake_rows(dataF, listadoIndicesDrop)
        continue
      listaAcertados = []
      listadoIndicesDrop = []

      for i, valor in enumerate(dataF[col]):
        a,b = detectando_tipo_dato_valor(valor,"flotante", i)
        listaAcertados.append(a)
        if (b != 0): listadoIndicesDrop.append(b)

      if listaAcertados.count(True) > listaAcertados.count(False):
        tipoDatoColumna[col] = 'float64'
        if len(listadoIndicesDrop) != 0:
          dataF = delete_fake_rows(dataF, listadoIndicesDrop)
        continue

      listaAcertados = []
      listadoIndicesDrop = []

      for i, valor in enumerate(dataF[col]):
        dFinal = replace_with_sin_dato(dataF, col)
        tipoDatoColumna[col] = 'object'
        continue
      # print("COLUMNA:", col)
  
  # print(tipoDatoColumna)
  # print("dataF",dataF)
  # if dFinal != None:
  #   dataF = dFinal
    
  return dataF

def drop_duplicate(dataF):
  dataF = dataF.drop_duplicates(keep='last')
  return dataF

def change_datatype_column(dataF):
  global tipoDatoColumna
  dataF = dataF.astype(tipoDatoColumna)
  # for k, v in enumerate(tipoDatoColumna.items()):
  #     dataF = dataF.astype({k:v})
  return dataF



def delete_fake_rows(dataF, idx):
  if (len(idx) != 0):
    # print("delete_fake_rows",idx)
    # for i in range(len(idx)):
    dataF = dataF.drop(idx, errors="ignore")
    # print("IDX:",idx)
    # print("dataF:",dataF)
    # print(dataF[dataF.longitud.eq("!")].index[0])
    # print("dataF, delete_fake_rows",dataF)
  return dataF

def replace_with_sin_dato(dataF, col):
  dataF = dataF[col].fillna("Sin Dato", inplace = True)
  return dataF

def get_nan_string_columns(dataF, dSet):
  lista_columnas = dataF.columns.values
  lista_columnas_objeto = []
  for i, e in enumerate(lista_columnas):
      if(dataF.dtypes[e] == np.object0):
          lista_columnas_objeto.append(e)
  # print(lista_columnas_objeto)
  # return dataF

def detectando_tipo_dato_valor(valor, tipo, idx):
  retornoIndex = 0
  if (tipo == "entero"):
    formatoRX = re.compile(r'^\-?[0-9]+?$')
  else:
    formatoRX = re.compile(r'^\-?[0-9]+(.[0-9]+)?$')

  # listaAcertados = []
  try:
    re.match(formatoRX,str(int(valor)) if tipo == "entero" else str(float(valor))) 
    valorRetorno =True
  except:
    valorRetorno =False
    retornoIndex = idx

  return valorRetorno, retornoIndex if retornoIndex!=0 else 0
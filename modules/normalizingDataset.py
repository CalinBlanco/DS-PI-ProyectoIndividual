import pandas as pd
import modules.utils as util
import json

f = open('modules/defaultDataDictionary.json', "r")
defaultJSON = json.load(f)

normalizeDatasets = "datasets/3_normalize_datasets/"
logDatasets = "datasets/4_log_datasets/"
reporteDicDatosNorma = "datasets/3_normalize_datasets/reportes/"

tipoDatoColumna = {}
listaIndexDrop = []


def iniciando_transformacion():
  norma = util.getArchivosDatasets(normalizeDatasets)

  print("Iniciando la normalización de los datasets...")

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


def init_normalizing(self, dataF, dSet):
    lista_columnas = dataF.columns.values
    print(lista_columnas)
    # data_types = pd.DataFrame(
    #     dataF.dtypes,
    #     columns=['Tipo ƒde Datos']
    # )
    # present_Data = pd.DataFrame(
    #     dataF.notnull().sum(),
    #     columns=['Datos NO Vacíos']
    # )

    # missing_data = pd.DataFrame(
    #     dataF.isnull().sum(),
    #     columns=['Datos Vacíos']
    # )

    # unique_values = pd.DataFrame(
    #     columns=['Valores Únicos']
    # )
    # for row in list(dataF.columns.values):
    #     unique_values.loc[row] = [dataF[row].nunique()]

    # maximum_values = pd.DataFrame(
    #     dataF.max(numeric_only=True),
    #     columns=['Dato Máximo']
    # )

    # minimum_values = pd.DataFrame(
    #     dataF.min(numeric_only=True),
    #     columns=['Dato Mínimo']
    # )

    # memory_usage = pd.DataFrame(
    #     dataF.memory_usage(),
    #     columns=['Tamaño(Memoria)']
    # )

    # change_name = pd.DataFrame(
    #     columns=['Sugerencia Cambio Nombre Columna']
    # )
    # for row in list(dataF.columns.values):
    #     change_name.loc[row] = self.getNewNameColum(row, dSet)

    # description = pd.DataFrame(
    #     columns=['Descripcion de Columna']
    # )
    # for row in list(dataF.columns.values):
    #     description.loc[row] = self.getDescriptionColum(row, dSet)

    ## ================================================================================

    # nro_outliers = pd.DataFrame(
    #     columns=['Nro Outliers']
    # )
    # for row in list(data.columns.values):
    #     if (data[row].dtypes in ['int64','float64'] ):
    #         nro_outliers.loc[row]= len(find_outliers_IQR(data[row]))
    #         if (len(find_outliers_IQR(data[row])) == 0): nro_outliers.loc[row]= 'None'
    #     else:
    #         nro_outliers.loc[row]= 'None'

    # max_outliers = pd.DataFrame(
    #     columns=['Max Outliers']
    # )
    # for row in list(data.columns.values):
    #     if (data[row].dtypes in ['int64','float64'] ):
    #         max_outliers.loc[row]= find_outliers_IQR(data[row]).max()
    #     else:
    #         max_outliers.loc[row]= 'None'

    # min_outliers = pd.DataFrame(
    #     columns=['Min Outliers']
    # )
    # for row in list(data.columns.values):
    #     if (data[row].dtypes in ['int64','float64'] ):
    #         min_outliers.loc[row]= find_outliers_IQR(data[row]).min()
    #     else:
    #         min_outliers.loc[row]= 'None'

    ## ================================================================================

    # dq_report = data_types.join(present_Data).join(missing_data).join(unique_values).join(maximum_values).join(minimum_values).join(memory_usage).join(change_name).join(description)
    # dq_report

    # return dq_report

    # def getDescriptionColum(self, columna, dataSet):
    #     f = open('modules/deaultDataDictionary.json')
    #     dataJSON = json.load(f)
    #     try:
    #         df_column = dataJSON[dataSet][columna].get("descripcionColumna")
    #     except:
    #         df_column = "--"
    #     return df_column

    # def getNewNameColum(self, columna, dataSet):
    #     f = open('modules/deaultDataDictionary.json')
    #     dataJSON = json.load(f)
    #     try:
    #         df_column = dataJSON[dataSet][columna].get("cambioColumna")
    #     except:
    #         df_column = "--"
    #     return df_column
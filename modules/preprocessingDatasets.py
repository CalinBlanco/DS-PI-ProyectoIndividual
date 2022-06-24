import pandas as pd
import modules.utils as util
import modules.createDataDictionary as dic_datos
import shutil

from fuzzywuzzy import process

## Declaramos PATHS de los directorios
originalDatasets = "datasets/1_original_datasets/"
prepDatasets = "datasets/2_preprocessing_datasets/"
normalizeDatasets = "datasets/3_normalize_datasets/"
logDatasets = "datasets/4_log_datasets/"
reporteDicDatosPrepro = "datasets/2_preprocessing_datasets/reportes/"

## Función que inicial el pre procesamiento de los datasets
def iniciarPreprocesado():
    print("Iniciando el Pre procesado de los datasets...")
    filesList = util.getArchivosDatasets(originalDatasets)
    if (filesList==[]):
        print("No hay Datasets en el directorio original")
    else:
        preProcesandoDatasets(originalDatasets)


## Función que permite comparar si esta vacío o no el directorio original de los datasets
def preProcesandoDatasets(originalDirectory):
    ori = util.getArchivosDatasets(originalDirectory)
    prep = util.getArchivosDatasets(prepDatasets)

    if(ori == []):
        print("No hay dataset nuevos...")
    else:
        preProcesandoDirectorios(ori, prep)
        print("Dataset preprocesados...")

## Función que permite pre procesar los datasets
def preProcesandoDirectorios(listaDirectorioOri, listaDirectorioPre):
    if (listaDirectorioPre == []):
        preProcesarSoloDirectorioOriginal(listaDirectorioOri)
    else:
        preProcesarConDirectorioPreProcessing(listaDirectorioOri, listaDirectorioPre)

## Función que permite pre procesar datasets en el directorio original
def preProcesarSoloDirectorioOriginal(lista):
    lista = lista
    nuevaLista = lista.copy()

    # print(nuevaLista)
    # print("="*100)
    
    for i, e in enumerate(lista):
        subList=[]
        if (nuevaLista == []): return ""
        strPrimeraOpcion = util.convertirNombreSingular(nuevaLista[0].split('.')[0]).lower()
        matches = process.extract(strPrimeraOpcion,nuevaLista)
        # print("Primera Opcion:",strPrimeraOpcion)
        # print("Matches:",matches)
        for match in matches:
            if match[1] >= 90:
                subList.append(match[0])
        
        # print("SubList Primera Instanacia:",subList)
        
        for i in range(len(subList)):
            x = " ".join(subList[i].lower().split('.'))
            resultado = x.find(strPrimeraOpcion)
            if(resultado != 0):
                subList.pop(i)

        # print("SubList Segunda Instanacia:",subList)
        
        generandoDatasetPreprocesado(strPrimeraOpcion, subList, True)

        for i, e in enumerate(subList):
            try:
                # print("Removiendo",e)
                nuevaLista.remove(e)
            except:
                continue
        
        # print("Nueva Lista:",nuevaLista)
        # print("\n")

## Función que permite pre procesar datasets en el directorio pre procesado
def preProcesarConDirectorioPreProcessing(listaOriginal, listaPre):  
    listaPre = listaPre
    nuevaListaPrep = listaPre.copy()
    listaOri = listaOriginal
    nuevaListaOri = listaOri.copy()
    
    try:
        for i, e in enumerate(listaOri):
            subList=[]
            if (nuevaListaPrep == []): return ""
            strPrimeraOpcion = nuevaListaPrep[0].split('.')[0].lower()
            matches = process.extract(strPrimeraOpcion,listaOri)
            for match in matches:
                if match[1] >= 90:
                    subList.append(match[0])
            
            for i in range(len(subList)):
                x = " ".join(subList[i].lower().split('.'))
                resultado = x.find(strPrimeraOpcion)
                if(resultado != 0):
                    subList.pop(i)
            
            generandoDatasetPreprocesado(strPrimeraOpcion, subList, False)

            for i, e in enumerate(subList):
                nuevaListaOri.remove(e)
    except:
        preProcesarSoloDirectorioOriginal(nuevaListaOri)

## Función que permite generar el dataset en el directorio de pre procesamiento
def generandoDatasetPreprocesado(nombreDataset, datset, inicial = False):
    varList=[]
    for i in range(len(datset)):
        varList.insert(i,'df'+str(i))
    
    for i in range(len(varList)):
        pathOriginal = originalDatasets
        pathPreProcesado = prepDatasets+nombreDataset+'.csv'
        # pathLog = logDatasets+datset[i].split('.')[0]
        sep = util.detectarSeparadorDataset(pathOriginal+datset[i])
        enc = util.detectarEncodingDataset(pathOriginal+datset[i])
        try:
            varList[i] = pd.read_csv(pathOriginal+datset[i], encoding="utf-8", sep=sep, engine='python', decimal=",")
        except:
            varList[i] = pd.read_csv(pathOriginal+datset[i], encoding=enc, sep=sep, engine='python', decimal=",")
    
    if (inicial):
        dfFinal = pd.concat(varList)
        dfFinal.to_csv(prepDatasets+nombreDataset.lower()+'.csv',encoding = "UTF-8",index=False)
    else:
        prepDataset = pd.read_csv(pathPreProcesado, encoding="utf-8", sep=',', engine='python')
        varList.insert(0,prepDataset)
        dfFinal = pd.concat(varList)
        dfFinal.to_csv(prepDatasets+nombreDataset.lower()+'.csv',encoding = "UTF-8",index=False)
    
    ## Generando el diccionario de datos
    dd = dic_datos.create_data_dictionary()
    df_dd = dd.make_my_data_dictionary(dfFinal,nombreDataset)
    util.generate_html(df_dd,nombreDataset,reporteDicDatosPrepro,"prepro")
        
    # Muevo el archivo dataset original al directorio LOG
    for item in datset:
        timestamp = util.getTimeStamp()
        shutil.move(pathOriginal+item, logDatasets+item.split('.')[0]+"-ori"+timestamp+".csv")

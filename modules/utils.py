import unicodedata
import os
import csv
import modules.singularWord as s
import chardet
import pandas as pd
import shutil

# from numpy import unicode_
# from pyparsing import unicodeString
from pathlib import Path
from datetime import datetime



def unicodify(st):
    '''
    Convert the given string to normalized Unicode (i.e. combining characters such as accents are combined)
    If given arg is not a string, it's returned as is, and origType is 'noConversion'.
    @return a tuple with the unicodified string and the original string encoding.
    '''

    # Convert 'st' to Unicode
    if isinstance(st, str):
        origType = 'unicode'
    elif isinstance(st, str):
        try:
            st = st.decode('utf8')
            origType = 'utf8'
        except UnicodeDecodeError:
            try:
                st = st.decode('latin1')
                origType = 'latin1'
            except:
                raise UnicodeEncodeError('Given string %s must be either Unicode, UTF-8 or Latin-1' % repr(st))
    else:
        origType = 'noConversion'

    # Normalize the Unicode (to combine any combining characters, e.g. accents, into the previous letter)
    if origType != 'noConversion':
        st = unicodedata.normalize('NFKC', st)

    return st, origType


def deunicodify(unicodifiedStr, origType):
    '''
    Convert the given unicodified string back to its original type and encoding
    '''

    if origType == 'unicode':
        return unicodifiedStr

    return unicodifiedStr.encode(origType)


## Función que permite obtener los elementos CSV's de un directorio
def getArchivosDatasets(filesPath):
    files = []
    with os.scandir(filesPath) as directorio:
        for archivo in directorio:
            try:
                if (archivo.name.split('.')[1] in ['xls','xlsx']):
                    excelFile = convertExcelCSV(archivo.name, filesPath)
                    files.append(excelFile)

                if (archivo.name.split('.')[1] == 'csv'):
                    files.append(archivo.name)
            except:
                continue
    files.sort()
    # print(files)
    return(files)

## Función que detecta y retorna el separador de un dataset
def detectarSeparadorDataset(path):
    enc = detectarEncodingDataset(path)
    # print("Path: ",path," || ", "Encoding:", enc )
    with open(path, 'r', encoding = enc, errors='ignore') as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.readline(), [',',';','|','/'])
        csvfile.seek(0)  
        data = csv.reader(csvfile, dialect)
    return data.dialect.delimiter

## Función que retorna el encoding de un dataset
def detectarEncodingDataset(path):
    filepath = Path(path)
    blob = filepath.read_bytes()

    detection = chardet.detect(blob)
    encoding = detection["encoding"]
    # confidence = detection["confidence"]
    # text = blob.decode(encoding)
    # print(encoding)
    return encoding

## Función que retorna un nombre en singular
def convertirNombreSingular(nombre):
    singular = s.Spanish()
    humanizado = s.Base()
    # nombreArchivo = nombreArchivo.split('.')[0]
    # palabraHumanizada = humanizado.humanize(nombreArchivo).split(' ')[0]
    palabraHumanizada = humanizado.humanize(nombre).split(' ')[0]
    nombreArchivoSingular = singular.singularize(palabraHumanizada)
    return nombreArchivoSingular

## Función que retorna el timestamp
def getTimeStamp():
    now = datetime.now()
    timestampCorto = str(now.timestamp())
    parteEntera = timestampCorto.split(".")[0]
    parteDecimal = timestampCorto.split(".")[1]
    retornarTimeStamp = "-"+parteEntera+"-"+parteDecimal
    return retornarTimeStamp

## Función que retorna la conversión de un archivo excel a csv
def convertExcelCSV(file, path):
    dfFinal = pd.read_excel(path+file, decimal=',', engine='openpyxl')
    dfFinal = dfFinal.to_csv(path+file.split('.')[0]+'.csv',encoding = "UTF-8",index=False, sep=',')
    os.remove(path+file)
    return file.split('.')[0]+'.csv'

## Función que me permite generar un archivo HTML
def generate_html(dataframe: pd.DataFrame, dset, path, pseudo):
    # get the table HTML from the dataframe
    dataframe = dataframe.rename_axis( columns="Columna")
    table_html = dataframe.to_html(table_id="table")
    # construct the complete HTML with jQuery Data tables
    # You can disable paging or enable y scrolling on lines 20 and 21 respectively
    html = f"""
    <html>
    <header>
        <link href="https://cdn.datatables.net/1.11.5/css/jquery.dataTables.min.css" rel="stylesheet">
    </header>
    <body>
    <h1>Diccionario de Datos del Dataset:</h1>
    <h2>{dset}</h2>
    {table_html}
    <script src="https://code.jquery.com/jquery-3.6.0.slim.min.js" integrity="sha256-u7e5khyithlIdTpu22PHhENmPcRdFiHRjhAuHcs05RI=" crossorigin="anonymous"></script>
    <script type="text/javascript" src="https://cdn.datatables.net/1.11.5/js/jquery.dataTables.min.js"></script>
    <script>
        $(document).ready( function () {{
            $('#table').DataTable({{
                // paging: false,    
                // scrollY: 400,
            }});
        }});
    </script>
    </body>
    </html>
    """
    saveDataframeHTML(html,dset,path,pseudo)

def saveDataframeHTML(html,dset,path, pseudo):
    timeStamp = getTimeStamp()
    open(path+dset+'-dicdatos-'+pseudo+timeStamp+".html", "w").write(html)
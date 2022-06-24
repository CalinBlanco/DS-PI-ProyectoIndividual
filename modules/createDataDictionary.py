import json
import pandas as pd
# Opening JSON file
f = open('modules/defaultDataDictionary.json')
datameaning = json.load(f)

class create_data_dictionary:

    def __init__(self):
        return None

    def make_my_data_dictionary(self, data, dSet):

        data_types = pd.DataFrame(
            data.dtypes,
            columns=['Tipo de Datos']
        )
        present_Data = pd.DataFrame(
            data.notnull().sum(),
            columns=['Datos NO Vacíos']
        )

        missing_data = pd.DataFrame(
            data.isnull().sum(),
            columns=['Datos Vacíos']
        )

        unique_values = pd.DataFrame(
            columns=['Valores Únicos']
        )
        for row in list(data.columns.values):
            unique_values.loc[row] = [data[row].nunique()]

        maximum_values = pd.DataFrame(
            data.max(numeric_only=True),
            columns=['Dato Máximo']
        )

        minimum_values = pd.DataFrame(
            data.min(numeric_only=True),
            columns=['Dato Mínimo']
        )

        memory_usage = pd.DataFrame(
            data.memory_usage(),
            columns=['Tamaño(Memoria)']
        )

        change_name = pd.DataFrame(
            columns=['Sugerencia Cambio Nombre Columna']
        )
        for row in list(data.columns.values):
            change_name.loc[row] = self.getNewNameColum(row, dSet)

        description = pd.DataFrame(
            columns=['Descripcion de Columna']
        )
        for row in list(data.columns.values):
            description.loc[row] = self.getDescriptionColum(row, dSet)

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

        dq_report = data_types.join(present_Data).join(missing_data).join(unique_values).join(maximum_values).join(minimum_values).join(memory_usage).join(change_name).join(description)
        dq_report

        return dq_report

    def getDescriptionColum(self, columna, dataSet):
        f = open('modules/defaultDataDictionary.json')
        dataJSON = json.load(f)
        try:
            df_column = dataJSON[dataSet][columna].get("descripcionColumna")
        except:
            df_column = "--"
        return df_column

    def getNewNameColum(self, columna, dataSet):
        f = open('modules/defaultDataDictionary.json')
        dataJSON = json.load(f)
        try:
            df_column = dataJSON[dataSet][columna].get("cambioColumna")
        except:
            df_column = "--"
        return df_column
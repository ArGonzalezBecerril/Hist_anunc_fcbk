import os
import sys
import dao.AdministradorDao as AdminDao
import dao.facebook.DaoExtraccionProp as DaoProp
from pyspark.sql.types import *
import datetime as fec
import pandas as pd
import random

reload(sys)
sys.setdefaultencoding('utf-8')


def obt_ruta(nom_directorio, nom_archivo):
    if sys.platform != 'win32':
        if nom_directorio == '':
            directorio = nom_archivo
        else:
            directorio = nom_directorio + "/" + nom_archivo
    else:
        if nom_directorio == '':
            directorio = nom_archivo
        else:
            directorio = nom_directorio + "\\" + nom_archivo
    return directorio


def lee_fichero_campanias():
    nom_archivo_autentic = 'campanias.properties'
    ruta = (os.path.dirname(os.path.abspath(__file__))).replace('/util', '/configuracion')
    path_txt_campania = obt_ruta(ruta, nom_archivo_autentic)
    return path_txt_campania


def lee_fichero_de_desglose():
    nom_archivo_autentic = 'desglose.properties'
    ruta = (os.path.dirname(os.path.abspath(__file__))).replace('/util', '/configuracion')
    path_txt_desglose = obt_ruta(ruta, nom_archivo_autentic)
    return path_txt_desglose


def obt_datos_conx(nom_seccion):
    ruta = (os.path.dirname(os.path.abspath(__file__))).replace('/util', '/configuracion')
    nom_archivo = obt_ruta(ruta, 'conexion.properties')
    dao_conf = AdminDao.AdministradorDao(DaoProp.DaoExtraccionProp, nom_archivo, nom_seccion).dao
    return dao_conf.obten()


def obt_prop_driver(nom_driver):
    properties = {
        "driver": nom_driver
    }
    return properties


def tipo_equivalente(tipo_de_formato):
    if tipo_de_formato == 'datetime64[ns]':
        return DateType()
    elif tipo_de_formato == 'int64':
        return LongType()
    elif tipo_de_formato == 'int32':
        return IntegerType()
    elif tipo_de_formato == 'float64':
        return FloatType()
    else:
        return StringType()


def define_estructura(cadena, tipo_formato):
    try:
        tipo = tipo_equivalente(tipo_formato)
    except:
        tipo = StringType()
    return StructField(cadena, tipo)


def pandas_a_spark(sql_context, pandas_df):
    try:
        columnas = list()
        [columnas.append(columna.upper().strip()) for columna in pandas_df.columns]

        tipos = list(pandas_df.dtypes)
        estructura_del_esquema = []
        for columna, tipo in zip(columnas, tipos):
            estructura_del_esquema.append(define_estructura(columna, tipo))
        esquema = StructType(estructura_del_esquema)
        return sql_context.createDataFrame(remueve_carac_especiales(pandas_df), esquema)
    except AttributeError as e:
        print("El dataframe pandas que intentas convertir a df_spark esta vacio:" + str(e))


def remueve_carac_especiales(dataframe, caracteres=',|\\t|\\n|\\r|\\|\\"|\\/|\"'):
    df_sin_caracteres_esp = dataframe.replace(caracteres, "", regex=True)
    return df_sin_caracteres_esp


def obt_fecha_actual():
    fecha_hoy = fec.date.today()
    id_dia = str(random.randint(1, 1000000000))
    fecha_con_formato = fecha_hoy.strftime("%d_%m_%Y" + id_dia)
    return str(fecha_con_formato)


def extrea_anio(fecha):
    return int(fecha.split('-')[0])


def extrae_mes(fecha):
    # Formato YYYY-MM-DD
    return int(fecha.split('-')[1])


def extrae_dia(fecha):
    # Formato YYYY-MM-DD
    return int(fecha.split('-')[2])


def union_de_drfms_pandas(grupo_de_dfrm_pandas):
    try:
        concentrado_dfrms = pd.concat(grupo_de_dfrm_pandas, ignore_index=True, sort=True)
        concentrado_dfrms['data_date_part'] = '27/10/2019'  # obt_fecha_actual()
        return concentrado_dfrms
    except ValueError as e:
        print("El grupo de dataframes esta vacio:" + str(e))


def carga(data_frm_spark):
    datos_conx = obt_datos_conx('conx_oracle')
    prop_conx = obt_prop_driver(datos_conx['driver'])
    url_jdbc = datos_conx['url_jdbc']
    data_frm_spark.write.jdbc(url=url_jdbc, table="ADS_INFO_ANUNCIO", mode='append', properties=prop_conx)
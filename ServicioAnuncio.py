import util.dto.DtoFacebook as Dto
import sys
import pyspark as pspk
import pyspark.sql as pysql
import modelo.Anuncio as camp
import dao.facebook.DaoConsulta as daoCon
import dao.AdministradorDao as adminDao
from datetime import date, timedelta
import util.Utilerias as Util
import pandas as pd
import os
import dao.facebook.DaoExtraccionProp as DaoProp

reload(sys)
sys.setdefaultencoding('utf-8')

context = pspk.SparkContext.getOrCreate()
sql_context = pysql.SQLContext(context)

path_raiz = os.getcwd()
grupo_de_anuncios = pd.read_csv('total_anuncios.csv', names=["id_anuncio", "fec_inicio", "fec_final"])


def union_de_drfms_pandas(grupo_campanias):
    df_total_campanias = pd.concat(grupo_campanias, ignore_index=True, sort=True)
    df_total_campanias['data_date_part'] = '27/10/2019'  # Util.obt_fecha_actual()
    return df_total_campanias

gpo_anunc_con_detalle = list()
dto_credenciales = Dto.DtoCredenciales(id_cuenta='act_804059193122922',
                                       token_de_acceso='EAAFqYKPZBGTwBAESZB1MgH3tnZCBt0Ny4LRQ8OhbL'
                                                       'sEgXvW7hDddhlHsHUqnrlu3KDlIII7qPgr501HZCJQQuZBK8z'
                                                       'vMQegVrBiTB1IILpOI1YYMLd8b5dp25ZCvd7yNZAukSioGZCyH'
                                                       'ADl4XE331SRUSZB275Dgav9uXpqfTtMLlbwZDZD',
                                       id_usuario='',
                                       id_app='',
                                       id_pagina='',
                                       app_secreta='')

nom_archivo = Util.lee_fichero_campanias()
dao_conf_campania = adminDao.AdministradorDao(DaoProp.DaoExtraccionProp, nom_archivo, 'campania').dao
dict_columnas = dao_conf_campania.obten()


# Contador
contador = 0

for anuncio in grupo_de_anuncios.values.tolist():

    if contador == 4:
        nom_archivo = 'hist_anuncios_' + Util.obt_fecha_actual() + '.csv'
        print(nom_archivo)
        dfrm_total_anuncios = Util.union_de_drfms_pandas(gpo_anunc_con_detalle)
        df_sp_total_anuncios = Util.pandas_a_spark(sql_context, dfrm_total_anuncios)
        df_sp_total_anuncios.withColumnRenamed("WEBSITE_CTR", "SITIOWEB_CTR")
        df_sp_total_anuncios.coalesce(1).write.format('com.databricks.spark.csv').save(nom_archivo, header='true')

        Util.carga(df_sp_total_anuncios)
        os.system("tail -n +5 total_anuncios.csv >> "
                  "total_anuncios2.csv && rm total_anuncios.csv "
                  "&& mv total_anuncios2.csv total_anuncios.csv")

        print("Finalizo el proceso...")
        sys.exit(0)

    fecha_inicial = date(Util.extrea_anio(anuncio[1]),
                         Util.extrae_mes(anuncio[1]),
                         Util.extrae_dia(anuncio[1]))

    fecha_final = date(Util.extrea_anio(anuncio[2]),
                       Util.extrae_mes(anuncio[2]),
                       Util.extrae_dia(anuncio[2]))

    print(" ##################################### Inicio ########################################### ")
    cve_anuncio = anuncio[0]
    incremento_en_dia = timedelta(days=1)

    while fecha_inicial <= fecha_final:
        obj_anuncio = camp.Anuncio(cve_anuncio=cve_anuncio,
                                   fec_inicial=fecha_inicial.strftime("%Y-%m-%d"),
                                   fec_final=fecha_final.strftime("%Y-%m-%d"))

        dao_anuncio = adminDao.AdministradorDao(daoCon.DaoAnuncio,
                                                dict_columnas,
                                                dto_credenciales,
                                                obj_anuncio).dao
        df_info_anuncio = dao_anuncio.obten_info_anuncio()
        gpo_anunc_con_detalle.append(df_info_anuncio)
        fecha_inicial += incremento_en_dia

    contador = contador + 1

nom_archivo = 'hist_anuncios_' + Util.obt_fecha_actual() + '.csv'
print(nom_archivo)
dfrm_total_anuncios = Util.union_de_drfms_pandas(gpo_anunc_con_detalle)
df_sp_total_anuncios = Util.pandas_a_spark(sql_context, dfrm_total_anuncios)

df_sp_total_anuncios.withColumnRenamed("WEBSITE_CTR", "SITIOWEB_CTR")

df_sp_total_anuncios.coalesce(1).write.format('com.databricks.spark.csv').save(nom_archivo, header='true')
Util.carga(df_sp_total_anuncios)

print("Finalizo el proceso final...")



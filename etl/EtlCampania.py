from abc import ABCMeta, abstractmethod
import dao.facebook.DaoConsulta as daoCon
import dao.AdministradorDao as adminDao
import util.Utilerias as Util
import util.Asserciones as Asercion
import modelo.Campania as camp
import pandas as pd


class EtlCampaniaABC(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def extrae(self):
        pass

    @abstractmethod
    def transforma(self):
        pass

    @abstractmethod
    def carga(self):
        pass


# Funcion UDF
def trunca_cadena(cadena):
    long_max = 4000
    return (cadena[:long_max]) if len(cadena) > long_max else cadena


class EtlCampania(EtlCampaniaABC):
    def __init__(self, dto_credenciales, sql_context):
        self.sql_context = sql_context
        self.dto_credenciales = dto_credenciales
        self.listado_de_campanias = None
        self.df_sp_campanias = None

    def valida_atributo(self, dato):
        Asercion.no_es_nulo(
            dato,
            '\n*Causa: El atributo' + str(dato) + ' esta vacio'
            '\n*Accion: Revise en el flujo etl si esta devolviendo un resultado la consulta')

    def concentra_todos_anuncios(self, grupo_campanias):
        df_total_campanias = pd.concat(grupo_campanias, ignore_index=True, sort=True)
        df_total_campanias['data_date_part'] = Util.obt_fecha_actual()
        return df_total_campanias

    def dfrm_a_lista_de_objetos(self, df_info_campanias):
        df_campanias = df_info_campanias[['id', 'insights', 'name', 'status']]

        coleccion_campanias = list()
        [coleccion_campanias.append(camp.Campania(campania[0],
                                                  str(campania[1]),
                                                  str(campania[2]),
                                                  campania[3])) for campania in
         df_campanias.values.tolist()]
        return coleccion_campanias

    def extrae(self):
        dao_http_campanias = adminDao.AdministradorDao(daoCon.DaoHttpCampania, self.dto_credenciales).dao
        campanias_en_crudo = dao_http_campanias.obten_campanias()
        self.listado_de_campanias = self.dfrm_a_lista_de_objetos(campanias_en_crudo)
        for i in self.listado_de_campanias:
            print (i)

    def transforma(self):
        grupo_campanias = list()

        for campania in self.listado_de_campanias:
            dao_campania = adminDao.AdministradorDao(daoCon.DaoCampania, campania, self.dto_credenciales).dao
            info_campania = dao_campania.obten_info_campania()
            grupo_campanias.append(info_campania)

        df_total_campanias = self.concentra_todos_anuncios(grupo_campanias)
        self.df_sp_campanias = Util.pandas_a_spark(self.sql_context, df_total_campanias)
        self.df_sp_campanias.show()

    def carga(self):
        datos_conx = Util.obt_datos_conx('conx_oracle')
        prop_conx = Util.obt_prop_driver(datos_conx['driver'])
        url_jdbc = datos_conx['url_jdbc']
        self.df_sp_campanias.write.jdbc(url=url_jdbc, table="ADS_INFO_CAMPANIA", mode='append', properties=prop_conx)



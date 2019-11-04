import dao.AdministradorDao as AdminDao
import pandas as pd
import requests
import util.Asserciones as Asercion
import os
import json
import util.Utilerias as Util
import dao.AdministradorDao as adminDao
import dao.facebook.DaoExtraccionProp as DaoProp


class DaoHttpCampania(AdminDao.HttpCampaniaABS):
    def __init__(self, dto_credenciales):
        self.dto_credenciales = dto_credenciales
        self.uri = "https://graph.facebook.com/v3.3/"

    def valida_parametros(self):
            Asercion.no_es_nulo(
                self.dto_credenciales,
                '\n*Causa: El objeto dto_credenciales esta vacio'
                '\n*Accion: Cree una instancia de dto_credenciales y vuelva a intentarlo')

    def construye_url(self):
        id_cuenta = self.dto_credenciales.id_cuenta + "/"
        atributos_campania = "campaigns?fields=id,name,status,insights{reach,impressions,clicks},can_use_spend_cap," \
                             "configured_status,created_time,daily_budget,effective_status,last_budget_toggling_time," \
                             "lifetime_budget,objective,pacing_type,promoted_object,recommendations,source_campaign," \
                             "source_campaign_id,special_ad_category,spend_cap,start_time,topline_id,updated_time"
        token_de_acceso = "&access_token=" + self.dto_credenciales.token_de_acceso

        nom_url = self.uri + id_cuenta + atributos_campania + token_de_acceso
        print(nom_url)
        return nom_url

    def escanea_campanias(self, nom_pagina):
        respuesta = requests.get(nom_pagina)
        txt_campanias = respuesta.json()

        if 'paging' not in txt_campanias:
            return pd.DataFrame()
        else:
            if 'next' in txt_campanias['paging']:
                pagina = txt_campanias['paging']['next']
                campania = pd.DataFrame(txt_campanias['data'])
                return campania.append(self.escanea_campanias(str(pagina)), ignore_index=True, sort=True)
            else:
                return pd.DataFrame(txt_campanias['data'])

    def obten_campanias(self):
        nom_url = self.construye_url()
        total_campanias = self.escanea_campanias(nom_url)
        return total_campanias


class DaoCampania(AdminDao.CampaniaABS):
    def __init__(self, obj_campania, dto_credenciales):
        self.dto_credenciales = dto_credenciales
        self.obj_campania = obj_campania

    def ejecuta_curl(self, comando):
        salida_comando = os.popen(comando).read()
        return salida_comando

    def construye_consulta_curl(self):
        commando_curl = "curl -G "
        rango_historia = "-d \"time_range={'since':'" + self.obj_campania.fec_inicial + "','until':'" + self.obj_campania.fec_final + "'}\" "
        token_de_acceso = "-d \"access_token=" + self.dto_credenciales.token_de_acceso + "\" "
        cve_campania = "\"https://graph.facebook.com/v3.3/" + self.obj_campania.cve_campania + "/insights\""
        comm_consulta_curl = commando_curl + rango_historia + token_de_acceso + cve_campania
        print(comm_consulta_curl)
        return comm_consulta_curl

    def valida_informacion(self, dict_campania):
        if 'data' in dict_campania:
            df_info_campania = pd.DataFrame(dict_campania['data'])
        else:
            df_info_campania = pd.DataFrame()
        return df_info_campania

    def obten_info_campania(self):
        comando = self.construye_consulta_curl()
        txt_info_campania = self.ejecuta_curl(comando)
        dict_campania = json.loads(txt_info_campania)
        df_info_campania = self.valida_informacion(dict_campania)
        return df_info_campania


class DaoAnuncio(AdminDao.AnuncioABS):
    def __init__(self, dict_col_campania, dto_credenciales, obj_anuncio):
        self.dict_col_campania = dict_col_campania
        self.dto_credenciales = dto_credenciales
        self.obj_anuncio = obj_anuncio
        self.valida_parametros(dto_credenciales, obj_anuncio)

    def valida_parametros(self, dto_credenciales, obj_campania):
        Asercion.no_es_nulo(
            dto_credenciales,
            '\n*Causa: El objeto dto_credenciales esta vacio'
            '\n*Accion: Cree una instancia de DtoCredenciales y vuelva a intentarlo')
        Asercion.no_es_nulo(
            obj_campania,
            '\n*Causa: El objeto obj_campania esta vacio'
            '\n*Accion: Cree una instancia de Campania y vuelva a intentarlo')

    def ejecuta_curl(self, comando):
        salida_comando = os.popen(comando).read()
        return salida_comando

    def construye_consulta_curl(self):
        commando_curl = "curl -G "
        rango_historia = "-d \"time_range={'since':'" + self.obj_anuncio.fec_inicial + "','until':'" + self.obj_anuncio.fec_inicial + "'}\" "
        atributos_de_campania = "-d \"fields=" + self.dict_col_campania['nom_column_origen'] + "\" "
        token_de_acceso = "-d \"access_token=" + self.dto_credenciales.token_de_acceso + "\" "
        cve_campania = "\"https://graph.facebook.com/v3.3/" + str(self.obj_anuncio.cve_anuncio) + "/insights\""
        comm_consulta_curl = commando_curl + rango_historia + atributos_de_campania + token_de_acceso + cve_campania
        print(comm_consulta_curl)
        return comm_consulta_curl

    def valida_informacion(self, dict_campania):
        if 'data' in dict_campania:
            df_info_campania = pd.DataFrame(dict_campania['data'])

        else:
            df_info_campania = pd.DataFrame()
        return df_info_campania

    def obten_info_anuncio(self):
        comando = self.construye_consulta_curl()
        #txt_info_anuncio = self.ejecuta_curl(comando)
        #dict_anuncio = json.loads(txt_info_anuncio)
        #df_info_anuncio = self.valida_informacion(dict_anuncio)
        #return df_info_anuncio


class DaoDesgloseGenerico(AdminDao.DaoDesgloseAbs):

    def __init__(self, dto_credenciales, gpo_de_claves_d_campanias):
        self.dto_credenciales = dto_credenciales
        self.gpo_de_claves_d_campanias = gpo_de_claves_d_campanias
        self.valida_parametros()

    def valida_parametros(self):
            Asercion.no_es_nulo(
                self.dto_credenciales,
                '\n*Causa: El objeto dto_credenciales esta vacio'
                '\n*Accion: Cree una instancia de dto_credenciales y vuelva a intentarlo')

            Asercion.esta_vacio_el_grupo(
                self.gpo_de_claves_d_campanias,
                '\n*Causa: El atributo \"gpo_claves_campania\" esta vacio, valor actuar:'
                + str(len(self.gpo_de_claves_d_campanias)) +
                '\n*Accion: Revise que el campo no este vacio e intente nuevamente ejecutar el script')

    def ejecuta_curl(self, comando):
        salida_comando = os.popen(comando).read()
        return salida_comando

    def obten_prop_de_desglose(self):
        txt_desglose = Util.lee_fichero_de_desglose()
        dao_conf_desglose = adminDao.AdministradorDao(DaoProp.DaoExtraccionProp, txt_desglose, 'desglose_generico').dao
        return dao_conf_desglose

    def obten_grupo_de_filtros(self):
        nom_desgloses = self.obten_prop_de_desglose().obten()
        grupo_de_filtros_de_desglose = nom_desgloses['grupo_de_desgloses'].split(",")

        return grupo_de_filtros_de_desglose

    def construye_commnd_curl(self, nom_filtro_desglose, cve_campania):
        filtro_desglose = "curl -G -d \"breakdowns=" + nom_filtro_desglose + "\" "
        filtro_nom_token = "-d \"access_token=" + self.dto_credenciales.token_de_acceso + "\" "
        nom_url_facebook = "\"https://graph.facebook.com/v3.3/" + cve_campania + "/insights\""
        curl_desglose = filtro_desglose + filtro_nom_token + nom_url_facebook
        return curl_desglose

    def obten_grupo_de_consultas_curl(self):
        nombres_de_desgloses = list()
        grupo_de_consultas_curl = list()
        grupo_de_filtros = self.obten_grupo_de_filtros()
        for nom_filtro_desglose in grupo_de_filtros:
            for cve_campania in self.gpo_de_claves_d_campanias:
                comando_curl = self.construye_commnd_curl(nom_filtro_desglose, cve_campania)
                grupo_de_consultas_curl.append(comando_curl)
                nombres_de_desgloses.append(nom_filtro_desglose)
        return tuple(zip(nombres_de_desgloses, grupo_de_consultas_curl))

    def obten(self):
        fragmentos_desgloses = list()
        grupo_de_consultas = self.obten_grupo_de_consultas_curl()

        for nom_desglose, consulta_desglose in grupo_de_consultas:
            json_desglose = self.ejecuta_curl(consulta_desglose)
            dict_desglose = json.loads(json_desglose)
            if 'data' in dict_desglose:
                df_desglose = pd.DataFrame(dict_desglose['data'])
                df_desglose['nom_desglose'] = nom_desglose
                df_desglose['data_date_part'] = Util.obt_fecha_actual()
                fragmentos_desgloses.append(df_desglose)

        total_de_desgloses = pd.concat(fragmentos_desgloses, axis=0, ignore_index=True, sort=False)
        return total_de_desgloses



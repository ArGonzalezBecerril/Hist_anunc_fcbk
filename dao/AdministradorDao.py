from abc import ABCMeta, abstractmethod
import util.Excepciones as Exc


class DaoAbc(object):
    __metaclass__ = ABCMeta


class DaoExtraccionPropAbc:
    __metaclass__ = ABCMeta

    @abstractmethod
    def obten(self):
        pass


class HttpCampaniaABS(DaoAbc):
    __metaclass__ = ABCMeta

    @abstractmethod
    def obten_campanias(self):
        pass


class CampaniaABS(DaoAbc):
    __metaclass__ = ABCMeta

    @abstractmethod
    def obten_info_campania(self):
        pass


class AnuncioABS(DaoAbc):
    __metaclass__ = ABCMeta

    @abstractmethod
    def obten_info_anuncio(self):
        pass


class DaoDesgloseAbs(DaoAbc):
    __metaclass__ = ABCMeta

    @abstractmethod
    def obten(self):
        pass


class AdministradorDao:
    def __init__(self, dao, *args):
        self.es_tipo_dao(
            dao, "Causa: Se ha utilizado un objeto que no es el del tipo DAO.\n"
                 " Accion: Utilice un objeto DAO en el AdministradorDao.")
        self.dao = dao(*args)

    def es_tipo_dao(self, dao, txt_mensaje):
        try:
            assert type(dao) == type(DaoAbc), txt_mensaje
        except AssertionError:
            raise Exc.InstanciaInvalida(txt_mensaje)

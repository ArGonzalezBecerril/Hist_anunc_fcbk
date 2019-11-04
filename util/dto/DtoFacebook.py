import util.Asserciones as Asercion


class DtoCredenciales:
        def __init__(self, id_cuenta, token_de_acceso, id_usuario, id_app, id_pagina, app_secreta):
            self.id_cuenta = id_cuenta
            self.token_de_acceso = token_de_acceso
            self.id_usuario = id_usuario
            self.id_app = id_app
            self.id_pagina = id_pagina
            self.app_secreta = app_secreta

        def valida_parametros(self):
            Asercion.no_es_cadena_vacia(
                self.id_cuenta,
                '\n*Causa: El atributo id_cuenta esta vacio'
                '\n*Accion: Agrege un valor por defecto al instanciar la clase')
            Asercion.no_es_cadena_vacia(
                self.token_de_acceso,
                '\n*Causa: El atributo toke_de_acceso esta vacio'
                '\n*Accion: Agrege un valor por defecto al instanciar la clase')

        def __str__(self):
            return ', '.join(['{key}={value}'.
                             format(key=key, value=self.__dict__.get(key))
                              for key in self.__dict__])

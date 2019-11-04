
class InstanciaInvalida(Exception):
    def __init__(self, mensaje):
        Exception.__init__(self, mensaje)


class EntradaSalidaIO(Exception):
    def __init__(self, mensaje):
        Exception.__init__(self, mensaje)


class CadenaVacia(Exception):
    def __init__(self, mensaje):
        Exception.__init__(self, mensaje)


class ObjetoNoNulo(Exception):
    def __init__(self, mensaje):
        Exception.__init__(self, mensaje)


class ObjetoNoValido(Exception):
    def __init__(self, mensaje):
        Exception.__init__(self, mensaje)

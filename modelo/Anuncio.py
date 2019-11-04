
class Anuncio:
    def __init__(self, cve_anuncio, fec_inicial, fec_final):
        self.cve_anuncio = cve_anuncio
        self.fec_inicial = fec_inicial
        self.fec_final = fec_final

    def __str__(self):
        return ', '.join(['{key}={value}'.
                         format(key=key, value=self.__dict__.get(key))
                          for key in self.__dict__])

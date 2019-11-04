
class Campania:
    def __init__(self, cve_campania, info_campania, nom_campania, estatus, fec_inicial, fec_final):
        self.cve_campania = cve_campania
        self.info_campania = info_campania
        self.nom_campania = nom_campania
        self.estatus = estatus
        self.fec_inicial = fec_inicial
        self.fec_final = fec_final

    def __str__(self):
        return ', '.join(['{key}={value}'.
                         format(key=key, value=self.__dict__.get(key))
                          for key in self.__dict__])

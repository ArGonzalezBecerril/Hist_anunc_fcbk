import os
import util.Excepciones as Exc


def no_existe_directorio(objeto, txt_mensaje):
    try:
        assert os.path.exists(objeto), txt_mensaje
    except AssertionError:
        Exc.EntradaSalidaIO(txt_mensaje)


def no_es_cadena_vacia(cadena, txt_mensaje):
    try:
        assert cadena != "" and cadena is not None, txt_mensaje
    except AssertionError:
        raise Exc.CadenaVacia(txt_mensaje)


def no_es_nulo(objeto, txt_mensaje):
    try:
        assert objeto is not None and objeto is not "", txt_mensaje
    except AssertionError:
        raise Exc.ObjetoNoNulo(txt_mensaje)


def existe_colum_en_df(objeto, columna, txt_mensaje):
    try:
        assert columna in objeto.columns, txt_mensaje
    except AssertionError:
        raise Exc.ObjetoNoValido(txt_mensaje)


def esta_vacio_el_grupo(objeto, txt_mensaje):
    try:
        assert not len(objeto) == 0, txt_mensaje
    except AssertionError:
        raise Exc.ObjetoNoValido(txt_mensaje)

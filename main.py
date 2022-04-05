# Se importan las funciones 
import functions as f
#m Se importan las librerias necesarias
from datetime import datetime
import pandas as pd
import numpy as np
import datetime
import collections
import pandas_datareader.data as web
# Se importa la librería para importar los historiales de metatraderfrom datetime import datetime
import MetaTrader5 as mt5

# Descargar los archivos de operaciones y de pips
jorge = r'files\historico_jorge.csv'
archivo_pip = f_leer_pip()
posiciones = f_leer_archivo(jorge)

# Modulo 1

f_columnas_pips(posiciones)
f_columnas_tiempos(posiciones)
f_estadisticas_ba(posiciones)
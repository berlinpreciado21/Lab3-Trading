#m Se importan las librerias necesarias
from datetime import datetime
import pandas as pd
import numpy as np
import datetime
import collections
import pandas_datareader.data as web
# Se importa la librería para importar los historiales de metatraderfrom datetime import datetime
import MetaTrader5 as mt5

def f_leer_archivo(nombre):
    # Numero de posiciones que tuviste debe de ser cambiado manualmente por cada usuario
    posiciones = pd.read_csv(nombre,skiprows = 6,nrows = 15)
    for i in range(len(posiciones)):
        posiciones.loc[i,"Price"] = (posiciones.loc[i,"Price"]).replace(" ","")
        posiciones.loc[i,"Price.1"] = (posiciones.loc[i,"Price.1"]).replace(" ","")
        posiciones.loc[i,"Profit"] = (posiciones.loc[i,"Profit"]).replace(" ","")
    posiciones = posiciones.astype({'Price':'float'})
    posiciones = posiciones.astype({'Price.1':'float'})
    posiciones = posiciones.astype({'Profit':'float'})
    #Cambiar nombre de las commodities
    posiciones = posiciones.replace("WTI","WTICOUSD")
    posiciones = posiciones.replace("PALLADIUM","XPDUSD")
    posiciones = posiciones.replace("PLATINUM","XPTUSD")
    return posiciones

def f_columnas_tiempos(param_data):
    #Convertir las columnas de tiempo
    param_data['Time'] =  pd.to_datetime(param_data['Time'])
    param_data['Time.1'] =  pd.to_datetime(param_data['Time.1'])
    param_data.rename(columns = {'Time':'Opentime', 'Time.1':'Closetime'}, inplace = True)
    #Calcular la duración de  en segundos 
    param_data["Tiempo"] = (param_data["Closetime"] - param_data["Opentime"]).dt.total_seconds()
    return param_data

def f_leer_pip():
    #Función para descargar una vez los pips
    doc_pips= pd.read_csv("files\instruments_pips.csv")
    for i in range(len(doc_pips)):
        doc_pips.loc[i,"Instrument"] = (doc_pips.loc[i,"Instrument"]).replace("_","")
    doc_pips = doc_pips.set_index("Instrument")
    new_doc_pips = doc_pips.filter(["TickSize"])
    # Añadir instrumentos que no estan en el excel
    new_doc_pips.loc["BITCOIN"] = .01
    new_doc_pips.loc["ETHEREUM"] = .01
    return new_doc_pips

def f_pip_size(instrumento: str):
    #Función que devuelve el pip del instrumento solicitado 
    return 1/archivo_pip.loc[instrumento,"TickSize"]

def f_columnas_pips(posiciones):
    # Añadir el pipsize de cada instrumento
    pip_size = []
    pip = []
    for i in range(len(posiciones)):
        pip_size.append(f_pip_size(posiciones["Symbol"][i]))
    posiciones["Pip_size"] = pip_size
    # Cantidad de pips resultantes de cada operación 
    for i in range(len(posiciones)):
        if posiciones["Type"][i] == "sell":
            pip.append((posiciones["Price"][i] - posiciones["Price.1"][i])*posiciones["Pip_size"][i])
        else:
            pip.append((posiciones["Price.1"][i] - posiciones["Price"][i])*posiciones["Pip_size"][i])
    posiciones["Pip"] = pip
    posiciones["Pips_acm"] = posiciones["Pip"].cumsum()
    posiciones["Profit_acm"] = posiciones["Profit"].cumsum()
    return posiciones

def f_estadisticas_ba(posiciones):
    # Listas de los nombres
    lista_medida = ["Ops totales","Ganadoras","Ganadoras_c","Ganadoras_v","Perdedoras","Perdedoras_c","Perdedoras_v","Mediana(Profit)",
                   "Mediana(Pips)","r_efectividad","r_proporcion","r_efectividad_c","r_efectividad_v"]
    lista_descripcion = ["Operaciones totales","Operaciones ganadoras","Operaciones ganadoras de compra","Operaciones ganadoras de venta","Operaciones perdedoras",
                         "Operaciones perdedoras de compra","Operaciones perdedoras de venta","Mediana de profit de operaciones","Mediana de pips de operaciones",
                         "Ganadoras Totales/Operaciones Totales","Ganadoras Totales/Perdedoras Totales","Ganadoras compras/Operaciones Totales",
                         "Ganadoras ventas/Operaciones Totales"]
    # Calcular los valores solicitados
    data = {"Valor":[len(posiciones),
                     len(posiciones.loc[posiciones['Profit'] > 0]),
                     len(posiciones.loc[(posiciones['Profit'] > 0) & (posiciones["Type"] == "buy")]),
                    len(posiciones.loc[(posiciones['Profit'] > 0) & (posiciones["Type"] == "sell")]),
                     len(posiciones.loc[posiciones['Profit'] < 0]),
                     len(posiciones.loc[(posiciones['Profit'] < 0) & (posiciones["Type"] == "buy")]),
                     len(posiciones.loc[(posiciones['Profit'] < 0) & (posiciones["Type"] == "sell")]),
                     posiciones['Profit'].median(),
                     posiciones['Pip'].median(),
                     len(posiciones.loc[posiciones['Profit'] > 0])/len(posiciones),
                     len(posiciones.loc[posiciones['Profit'] > 0])/len(posiciones.loc[posiciones['Profit'] < 0]),
                     len(posiciones.loc[(posiciones['Profit'] > 0) & (posiciones["Type"] == "buy")])/len(posiciones),
                     len(posiciones.loc[(posiciones['Profit'] > 0) & (posiciones["Type"] == "sell")])/len(posiciones)
                    ],
           "Descripcion": lista_descripcion,
           "Medida": lista_medida}
    estadisticas_ba = pd.DataFrame(data)
    estadisticas_ba = estadisticas_ba.set_index("Medida")
    
    #Segunda parte de la función 
    
    # Obtener todos los instrumentos operados
    symbols = list(posiciones["Symbol"].unique())
    symbol_ratio = []
    for i in symbols:
        symbol_ratio.append(len(posiciones.loc[(posiciones['Profit'] > 0) & (posiciones["Symbol"] == i)])/len(posiciones.loc[posiciones['Symbol'] == i]))
    ranking = {"Symbol":symbols,"Rank": symbol_ratio}
    ranking = pd.DataFrame(ranking)
    ranking = ranking.set_index("Symbol")
    ranking = ranking.sort_values(by = ["Rank"],ascending = False)
    
    dict_estadisticas = {"df_1_tabla": estadisticas_ba,"df_2_ranking":ranking} 
    
    return dict_estadisticas

def f_evolucion_capital(posiciones):
    tabla=posiciones[['Time', "Profit"]]
    tabla['Time'] = pd.to_datetime(tabla['Time']) 
    tabla['Profit'] = pd.to_datetime(tabla['Profit']) 
    tabla.set_index('Time', inplace=True) #index
    posiciones3=tabla.resample('D').sum()
    tabla["Profit_acm_d"] = posiciones["Profit"].cumsum()
    return posiciones3

# Función para descargar precios de cierre ajustados:
def get_adj_closes(tickers, start_date=None, end_date=None):
    # Fecha inicio por defecto (start_date='2010-01-01') y fecha fin por defecto (end_date=today)
    closes = web.DataReader(name=tickers, data_source='yahoo', start=start_date, end=end_date)
    closes = closes['Adj Close']
    closes.sort_index(inplace=True)
    return closes

def f_estadisticas_mad(tabla):
          
        #Sharpe original
        retlog = np.log(tabla.Profit_acm_d / tabla.Profit_acm_d.shift()).dropna()
        rp = retlog.mean()
        sigma = rp.std()
        #tasa libre de riesgo
        rf = 0.05
        shor= (rp - rf) / sigma

        #Sharpe Actualizado
        # Descarga de datos, se sustituyó el SPY (ETF S&P 500), por DXY (U&S Dollar Index)
        benchmark = get_adj_closes('SPY', start_date=tabla.index[0], end_date=tabla.index[-1])
        retlogben = np.log(benchmark / benchmark.shift()).dropna()
        rpbenchmark = retlogben.mean()
        dif_rets = retlog - retlogben
        # Desviación de la diferencia
        sigma_benchmark = dif_rets.std()
        # Formula sharpe actualizado
        shact = (rp - rpbenchmark) / sigma_benchmark

      

        #Drawdown y drawup

        data_min = tabla['profit_acm_d'].min()
        data_max = tabla['profit_acm_d'].max()

        posicion_max = tabla['profit_acm_d'].idxmax()
        posicion_min = tabla['profit_acm_d'].idxmin()

        fecha_max = tabla['time'][posicion_max]
        fecha_min = tabla['time'][posicion_min]

        if fecha_max > fecha_min:
            fecha_inicial_dd = tabla.loc[0, 'time']
            fecha_final_dd = fecha_min
            fecha_inicial_du = tabla['time'][posicion_min]
            fecha_final_du = fecha_max
        else:
            fecha_inicial_du = tabla.loc[0, 'time']
            fecha_final_du = fecha_max
            fecha_inicial_dd = tabla['time'][posicion_max]
            fecha_final_dd = fecha_min

        estadisticas = pd.DataFrame()
        estadisticas['metrica'] = ['sharpe_original', 'sharpe_actualizado', 'drawdown_capi', 'drawdown_capi',
                                   'drawdown_capi', 'drawup_capi', 'drawup_capi', 'drawup_capi']
        estadisticas[''] = ['Cantidad', 'Cantidad', 'Fecha Inicial', 'Fecha Final', 'DrawDown $ (capital)',
                            'Fecha Inicial', 'Fecha Final', 'DrawDown $ (capital)']
        estadisticas['Valor'] = [shor, shact, fecha_inicial_dd, fecha_final_dd, data_min, fecha_inicial_du,
                                 fecha_final_du, data_max]
        
        estadisticas['descripcion'] = ['Sharpe Ratio Fórmula Original', 'Sharpe Ratio Fórmula Ajustada' , 'Fecha inicial del DrawDown de Capital' , 
        'Fecha final del DrawDown de Capital', 'Máxima pérdida flotante registrada', 'Fecha inicial del DrawUp de Capital', 
        'Fecha final del DrawUp de Capital', 'Máxima ganancia flotante registrada']
        
        tabla.set_index('time', inplace=True)
        
        return estadisticas

def f_be_de(param_data):
    #Cambiar a simbolos
    param_data = param_data.replace("WTICOUSD","WTI")
    param_data = param_data.replace("XPDUSD","PALLADIUM")
    param_data = param_data.replace("XPTUSD","PLATINUM")
    # Dataframe con eventos ancla 
    operaciones_ancla = param_data.loc[param_data["Profit"] > 0]
    # Dataframe con operaciones restantes 
    operaciones_otras = param_data.loc[param_data["Profit"] <= 0]
    # Inicialización de diccionario y contador
    ocurrencias = 0
    dictionario = {"Ocurrencias": {}}
    status_quo = 0
    aversion = 0
    for i in range(len(operaciones_ancla)):
        for j in range(len(param_data)): 
            if operaciones_ancla.iloc[i]["Position"] == param_data.iloc[j]['Position']:
                continue
            else:
                if operaciones_ancla.iloc[i]["Closetime"] >= param_data.iloc[j]['Opentime'] and operaciones_ancla.iloc[i]["Closetime"] <= param_data.iloc[j]['Closetime']:
                    precio = mt5.copy_ticks_from(param_data.loc[j, 'Symbol'],operaciones_ancla.iloc[i]['Closetime'].to_pydatetime(),1,mt5.COPY_TICKS_ALL)
                    # Calcular los nuevos precios de compra y venta
                    if param_data.iloc[j]["Type"] == "buy":
                        nuevo_precio = precio[0][1]
                        # Si hay perdida se suma una ocurrencia
                        if (nuevo_precio - param_data.iloc[j]['Price']) < 0 :
                            ocurrencia = True
                            perdida_flotante = (nuevo_precio - param_data.iloc[j]['Price']) * param_data.iloc[j]['Volume'] * param_data.iloc[j]['Pip_size']
                        else:
                            ocurrencia = False

                    if param_data.iloc[j]["Type"] == "sell":
                        nuevo_precio = precio[0][2] 
                        ## Si hay perdida se suma una ocurrencia
                        if (param_data.iloc[j]['Price'] - nuevo_precio) < 0:
                            ocurrencia = True
                            perdida_flotante = (param_data.iloc[j]['Price'] - nuevo_precio) * param_data.iloc[j]['Volume'] * param_data.iloc[j]['Pip_size']
                        else:
                            ocurrencia = False

                    # Sumar las ocurrencias y guardar los datos
                    if ocurrencia == True:
                        ocurrencias += 1
                        timestamp = operaciones_ancla.iloc[i]["Closetime"]
                        # Datos de la ganadora
                        instrumento_ganadora = operaciones_ancla.iloc[i]["Symbol"]
                        volumen_ganadora =  operaciones_ancla.iloc[i]["Volume"]
                        sentido_ganadora = operaciones_ancla.iloc[i]["Type"]
                        profit_ganadora = operaciones_ancla.iloc[i]["Profit"]

                        #Datos de la perdedora
                        instrumento_perdedora = param_data.iloc[j]['Symbol']
                        volumen_perdedora = param_data.iloc[j]['Volume']
                        sentido_perdedora = param_data.iloc[j]['Type']
                        profit_perdedora = perdida_flotante

                        ocurrencia_num = "ocurrencia" + str(ocurrencias)
                        ratio_cp = profit_perdedora / operaciones_ancla.iloc[i]["Profit_acm"]
                        ratio_cg = profit_ganadora / operaciones_ancla.iloc[i]["Profit_acm"]
                        ratio_cp_cg = profit_perdedora/profit_ganadora

                        # Añadir los datos de las operaciones al diccionario 
                        dictionario["Ocurrencias"][ocurrencia_num] = {"Timestamp" : timestamp,
                             "Operaciones":
                             {"Ganadora":
                              {"instrumento":instrumento_ganadora,
                               "volumen": volumen_ganadora,
                               "sentido":sentido_ganadora,
                               "profit": profit_ganadora    
                              },
                             "Perdedora":
                              {"instrumento":instrumento_perdedora,
                               "volumen": volumen_perdedora,
                               "sentido":sentido_perdedora,
                               "profit":profit_perdedora    
                              }},
                             "ratio_cp_profit_acm": ratio_cp,
                             "ratio_cg_profit_acm": ratio_cg,
                             "ratio_cp_cg":ratio_cp_cg 

                             }
                        # Contar las veces que afecó el status quo
                        if abs(ratio_cp) < abs(ratio_cg):
                            status_quo += 1

                        # Contar las veces que afectó la aversión a la perdida
                        if abs(ratio_cp_cg) > 2:
                            aversion += 1
    #Checar sesgo de sensibilidad decreciente
    resultados = pd.DataFrame([[ocurrencias,status_quo,aversion,0]],columns=['ocurrencias','status_quo','aversion_perdida','sensibilidad_decreciente'])
    dictionario["Resultados"] = {"dataframe" : resultados}        
    return dictionario
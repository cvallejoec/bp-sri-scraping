#To server requests
import requests
#To databse connections
import pandas as pd
import psycopg2
import sqlalchemy
import matplotlib.pyplot as plt
%matplotlib inline
from sqlalchemy import create_engine
#To json file
import json
#To dates
import datetime
from datetime import datetime
#To free caché
import gc


# Nombre de tabla para guardar
nombre_tabla = "persona" # Tabla de la base de datos donde se irán guardando los registros
nombre_archivo = "lista_cedulas.csv"
# Listas de URL para enviar las peticiones
urls = [
  ['consulta_ruc', 'https://srienlinea.sri.gob.ec/sri-catastro-sujeto-servicio-internet/rest/ConsolidadoContribuyente/obtenerPorNumerosRuc?&ruc='],
  ['impuesto_causado', 'https://srienlinea.sri.gob.ec/sri-declaraciones-impuesto-renta-servicio-internet/rest/RentaJuridicos/obtenerConsultaRentaJuridicos?identificacion=']
]
# Link de prueba para verificar la validez del Token
url_for_request = 'https://srienlinea.sri.gob.ec/sri-registro-civil-servicio-internet/rest/DatosRegistroCivil/obtenerPorNumeroIdentificacionConToken?numeroIdentificacion='


# Conexión a base de datos
# Cambiar las credenciales por las de su base de datos propia
POSTGRES_ADDRESS = 'db-wsc-postgre.ccjyfyhf8wbi.us-east-1.rds.amazonaws.com' 
POSTGRES_PORT = '5432'
POSTGRES_USERNAME = 'postgres' 
POSTGRES_PASSWORD = 'c1av3aw5' 
POSTGRES_DBNAME = 'db_wsc_postgre' 

postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'
                .format(username=POSTGRES_USERNAME, 
                password=POSTGRES_PASSWORD, 
                ipaddress=POSTGRES_ADDRESS, 
                port=POSTGRES_PORT, 
                dbname=POSTGRES_DBNAME))

# Create the connection
cnx = create_engine(postgres_str)

# Obtener lista de identificaciones desde el archivo
df = pd.read_csv(nombre_archivo)
lista_cedulas = df.values.tolist()






#
#
# CÓDIGO ÚTIL, NO CAMBIAR NADA DESDE AQUÍ:
#
#

def make_identifications(identificacion):
  if(len(identificacion) == 9):
    cedula = '0'+identificacion
    ruc = cedula + '001'
    return cedula, cedula, ruc
  else:
    if(len(identificacion) == 12):
      ruc = '0'+identificacion
      cedula = ruc[0] + ruc[1] + ruc[2] + ruc[3] + ruc[4] + ruc[5] + ruc[6] + ruc[7] + ruc[8] + ruc[9]
      return ruc, cedula, ruc
    if(len(identificacion) == 13):
      ruc = identificacion
      cedula = ruc[0] + ruc[1] + ruc[2] + ruc[3] + ruc[4] + ruc[5] + ruc[6] + ruc[7] + ruc[8] + ruc[9]
      return ruc, cedula, ruc
    if(len(identificacion) == 10):
      ruc = identificacion + '001'
      return identificacion, identificacion, ruc

# Verifica la validez de una cédula
def cedula_valida(cedula):
    l = len(cedula)
    if l == 10 or l == 13: # verificar la longitud correcta
        cp = int(cedula[0:2])
        if cp >= 1 and cp <= 22: # verificar codigo de provincia
            tercer_dig = int(cedula[2])
            if tercer_dig >= 0 and tercer_dig < 6 : # numeros enter 0 y 6
                if l == 10:
                    return __validar_ced_ruc(cedula,0)                       
                elif l == 13:
                    return __validar_ced_ruc(cedula,0) and cedula[10:13] != '000' # se verifica q los ultimos numeros no sean 000
            elif tercer_dig == 6:
                    return __validar_ced_ruc(cedula,1) # sociedades publicas
            elif tercer_dig == 9: # si es ruc
                    return __validar_ced_ruc(cedula,2) # sociedades privadas
            else:
              #raise Exception(u'Tercer digito invalido') 
              return False
        else:
          #raise Exception(u'Codigo de provincia incorrecto') 
          return False
    else:
      #raise Exception(u'Longitud incorrecta del numero ingresado')
      return False

def __validar_ced_ruc(nro,tipo):
    total = 0
    if tipo == 0: # cedula y r.u.c persona natural
        base = 10
        d_ver = int(nro[9])# digito verificador
        multip = (2, 1, 2, 1, 2, 1, 2, 1, 2)
    elif tipo == 1: # r.u.c. publicos
        base = 11
        d_ver = int(nro[8])
        multip = (3, 2, 7, 6, 5, 4, 3, 2 )
    elif tipo == 2: # r.u.c. juridicos y extranjeros sin cedula
        base = 11
        d_ver = int(nro[9])
        multip = (4, 3, 2, 7, 6, 5, 4, 3, 2)
    for i in range(0,len(multip)):
        p = int(nro[i]) * multip[i]
        if tipo == 0:
            total+=p if p < 10 else int(str(p)[0])+int(str(p)[1])
        else:
            total+=p
    mod = total % base
    val = base - mod if mod != 0 else 0
    return val == d_ver
    
# Crea variable Datetime
def create_datetime(date_str):
  my_date = str(date_str)
  date_str = my_date + " 00:00:00"
  format_str = '%d/%m/%Y %H:%M:%S' # The format
  datetime_obj = datetime.strptime(date_str, format_str)
  return datetime_obj

def get_token():
    token = input('Ingrese el Token: ')
    return token

def get_data(url, token):
    try:
        request = requests.get(url, headers={'Authorization':token})
        if(request.status_code == 401 or request.status_code == 403):
            request_list = ['token_caducado']
        else:
            request_list = request.json()
    except:
        request_list = {}

    return request_list
  
def impuesto_causado_natural(request_list, identificacion):
    anio_uno_persona = 'NULL'
    impuestocausado_uno_persona = 'NULL'
    impuestodivisas_uno_persona = 'NULL'
    anio_dos_persona = 'NULL'
    impuestocausado_dos_persona = 'NULL'
    impuestodivisas_dos_persona = 'NULL'
    anio_tres_persona = 'NULL'
    impuestocausado_tres_persona = 'NULL'
    impuestodivisas_tres_persona = 'NULL'

    try:
        anio_uno_persona = request_list[0]['anioFiscal']
        if(str(request_list[0]['rentaCausadoRetenidoRelacionDependencia']) == 'null' or str(request_list[0]['rentaCausadoRetenidoRelacionDependencia']) == 'None'): 
            impuestocausado_uno_persona = request_list[0]['rentaCausadoRetenido']
        else:
            impuestocausado_uno_persona = request_list[0]['rentaCausadoRetenidoRelacionDependencia']
        impuestodivisas_uno_persona = request_list[0]['salidaDivisas']
    except:
        print("WARNING: No tiene información registrada de Impuestos")

    try:
        anio_dos_persona = request_list[1]['anioFiscal']
        if(str(request_list[1]['rentaCausadoRetenidoRelacionDependencia']) == 'null' or str(request_list[1]['rentaCausadoRetenidoRelacionDependencia']) == 'None'): 
            impuestocausado_dos_persona = request_list[1]['rentaCausadoRetenido']
        else:
            impuestocausado_dos_persona = request_list[1]['rentaCausadoRetenidoRelacionDependencia']
        impuestodivisas_dos_persona = request_list[1]['salidaDivisas']
    except:
        print("WARNING: Al parecer solo tiene un anio de declaracion")
        
    try:
        anio_tres_persona = request_list[2]['anioFiscal']
        if(str(request_list[2]['rentaCausadoRetenidoRelacionDependencia']) == 'null' or str(request_list[2]['rentaCausadoRetenidoRelacionDependencia']) == 'None'): 
            impuestocausado_tres_persona = request_list[2]['rentaCausadoRetenido']
        else:
            impuestocausado_tres_persona = request_list[2]['rentaCausadoRetenidoRelacionDependencia']
        impuestodivisas_tres_persona = request_list[2]['salidaDivisas']
    except:
        print("WARNING: Al parecer solo tiene dos anios de declaracion")
        
    if(str(impuestocausado_uno_persona) == 'None'):
        impuestocausado_uno_persona = 'NULL'
    if(str(impuestocausado_dos_persona) == 'None'):
        impuestocausado_dos_persona = 'NULL'
    if(str(impuestocausado_tres_persona) == 'None'):
        impuestocausado_tres_persona = 'NULL'

    query = ('UPDATE '+nombre_tabla+' '
            'SET anio_uno_persona = '+str(anio_uno_persona)+','
            'impuestocausado_uno_persona ='+str(impuestocausado_uno_persona)+','
            'impuestodivisas_uno_persona ='+str(impuestodivisas_uno_persona)+','
            'anio_dos_persona ='+str(anio_dos_persona)+','
            'impuestocausado_dos_persona ='+str(impuestocausado_dos_persona)+','
            'impuestodivisas_dos_persona ='+str(impuestodivisas_dos_persona)+','
            'anio_tres_persona ='+str(anio_tres_persona)+','
            'impuestocausado_tres_persona ='+str(impuestocausado_tres_persona)+','
            'impuestodivisas_tres_persona ='+str(impuestodivisas_tres_persona)+' '
            "WHERE cedula_persona = '%s' OR ruc_persona = '%s'" % (identificacion, identificacion))
    update_persona(query)
    
    print("SUCCESS: Consulta Impuesto Natural")


def impuesto_causado_juridico(request_list, identificacion):
    anio_uno_persona = 'NULL'
    impuestocausado_uno_persona = 'NULL'
    impuestodivisas_uno_persona = 'NULL'
    anio_dos_persona = 'NULL'
    impuestocausado_dos_persona = 'NULL'
    impuestodivisas_dos_persona = 'NULL'
    anio_tres_persona = 'NULL'
    impuestocausado_tres_persona = 'NULL'
    impuestodivisas_tres_persona = 'NULL'

    try:
        anio_uno_persona = request_list[0]['anioFiscal']
        if(str(request_list[0]['impuestoCausado']) != 'null' or str(request_list[0]['impuestoCausado']) != 'None'): 
            impuestocausado_uno_persona = request_list[0]['impuestoCausado']
        impuestodivisas_uno_persona = request_list[0]['salidaDivisas']
    except:
        print("WARNING: No tiene información registrada de Impuestos")

    try:
        anio_dos_persona = request_list[1]['anioFiscal']
        if(str(request_list[1]['impuestoCausado']) != 'null' or str(request_list[1]['impuestoCausado']) != 'None'): 
            impuestocausado_dos_persona = request_list[1]['impuestoCausado']
        impuestodivisas_dos_persona = request_list[1]['salidaDivisas']
    except:
        print("WARNING: Al parecer solo tiene un anio de declaracion")
        
    try:
        anio_tres_persona = request_list[2]['anioFiscal']
        if(str(request_list[2]['impuestoCausado']) != 'null' or str(request_list[2]['impuestoCausado']) != 'null'): 
            impuestocausado_tres_persona = request_list[2]['impuestoCausado']
        impuestodivisas_tres_persona = request_list[2]['salidaDivisas']
    except:
        print("WARNING: Al parecer solo tiene dos anios de declaracion")
        
    if(str(impuestocausado_uno_persona) == 'None'):
        impuestocausado_uno_persona = 'NULL'
    if(str(impuestocausado_dos_persona) == 'None'):
        impuestocausado_dos_persona = 'NULL'
    if(str(impuestocausado_tres_persona) == 'None'):
        impuestocausado_tres_persona = 'NULL'
    
    query = ('UPDATE '+nombre_tabla+' '
            'SET anio_uno_persona = '+str(anio_uno_persona)+','
            'impuestocausado_uno_persona ='+str(impuestocausado_uno_persona)+','
            'impuestodivisas_uno_persona ='+str(impuestodivisas_uno_persona)+','
            'anio_dos_persona ='+str(anio_dos_persona)+','
            'impuestocausado_dos_persona ='+str(impuestocausado_dos_persona)+','
            'impuestodivisas_dos_persona ='+str(impuestodivisas_dos_persona)+','
            'anio_tres_persona ='+str(anio_tres_persona)+','
            'impuestocausado_tres_persona ='+str(impuestocausado_tres_persona)+','
            'impuestodivisas_tres_persona ='+str(impuestodivisas_tres_persona)+' '
            "WHERE cedula_persona = '%s' OR ruc_persona = '%s'" % (identificacion, identificacion))
    update_persona(query)
    
    print("SUCCESS: Consulta Impuesto Jurídico")


def consulta_ruc(request_list, identificacion):
    fechaceseactividades_persona = 'NULL'
    
    obligadocontabilidad_persona = request_list[0]['obligado']
    if(obligadocontabilidad_persona == 'N'):
        obligadocontabilidad_persona = 'NO'
    if(obligadocontabilidad_persona == 'S'):
        obligadocontabilidad_persona = 'SI'
    razonsocial_persona = request_list[0]['razonSocial']
    fechainicioactividades_persona = create_datetime(request_list[0]['informacionFechasContribuyente']['fechaInicioActividades'])
    actividadeconomica_persona = request_list[0]['actividadContribuyente']
    try:
        fechaceseactividades_persona = create_datetime(request_list[0]['informacionFechasContribuyente']['fechaCese'])
    except:
        fechaceseactividades_persona = 'NULL'
    
    if(fechaceseactividades_persona == 'NULL'):
        query = "UPDATE %s SET obligadocontabilidad_persona = '%s', razonsocial_persona = '%s', fechainicioactividades_persona = '%s', actividadeconomica_persona = '%s', fechaceseactividades_persona = %s WHERE cedula_persona = '%s' OR ruc_persona = '%s'" % (nombre_tabla, obligadocontabilidad_persona, razonsocial_persona, fechainicioactividades_persona, actividadeconomica_persona, fechaceseactividades_persona, identificacion, identificacion)
    else:
        query = "UPDATE %s SET obligadocontabilidad_persona = '%s', razonsocial_persona = '%s', fechainicioactividades_persona = '%s', actividadeconomica_persona = '%s', fechaceseactividades_persona = '%s' WHERE cedula_persona = '%s' OR ruc_persona = '%s'" % (nombre_tabla, obligadocontabilidad_persona, razonsocial_persona, fechainicioactividades_persona, actividadeconomica_persona, fechaceseactividades_persona, identificacion, identificacion)
    update_persona(query)
    
    print("SUCCESS: Consulta RUC")

    
def consulta_registro_civil(request_list, identificacion):
    nombreCompleto = request_list['nombreCompleto']
    nombreSeparado = nombreCompleto.split()
    nombre_persona = ""
    apellido_persona = ""
    try:
        apellido_persona = nombreSeparado[0] + " " + nombreSeparado[1]
        nombre_persona = nombreSeparado[2]
    except:
        print("Solo tiene un nombre y un apellido")
    try:
        nombre_persona = nombre_persona + " " + nombreSeparado[3]
    except:
        print("Solo tiene un nombre")
    
    query = "UPDATE %s SET nombre_persona = '%s', apellido_persona = '%s' WHERE cedula_persona = '%s' OR ruc_persona = '%s'" % (nombre_tabla, nombre_persona, apellido_persona, identificacion, identificacion)
    update_persona(query)
    
    print("SUCCESS: Consulta Registro Civil")

def is_natural_juridico(request_list):
    acronimo_persona = request_list['tipoPersona']
    if(acronimo_persona == 'PNL'):
        tipoPersona = "natural"
    if(acronimo_persona == 'JUR'):
        tipoPersona = "juridico"
    return tipoPersona


# Función principal
def main_():
  i = 0
is_first_time = True
while i < len(lista_cedulas):
    identificacion, cedula, ruc = make_identifications(str(lista_cedulas[i][0]))
    print("Ciclo: %i" % (i+1))
    print("Identificacion: %s" % identificacion)
    
    if(cedula_valida(cedula) or cedula_valida(ruc)):
        insert_persona(cedula, ruc)
        if(is_first_time):
            token = get_token()
            is_first_time = False
        
        if(len(identificacion) == 10):
            #Extraigo datos del Registro Civil
            print("PROCESO: Extrayendo datos Registro Civil")
            url_complete = "https://srienlinea.sri.gob.ec/sri-registro-civil-servicio-internet/rest/DatosRegistroCivil/obtenerPorNumeroIdentificacionConToken?numeroIdentificacion=" + identificacion
            request_list = get_data(url_complete, token)
            while(request_list == ['token_caducado']):
                token = get_token()
                request_list = get_data(url_complete, token)
            if(request_list):
                consulta_registro_civil(request_list, identificacion)
            else:
                print("ERROR: Consulta Registro Civil - Puede ser una empresa")
        j = 0
        while j < len(urls):
            try:
                if(urls[j][0] == 'consulta_ruc'):
                    print("PROCESO: Extrayendo datos Consulta RUC")
                    if(cedula_valida(ruc)):
                        url_complete = urls[j][1] + ruc
                        request_list = get_data(url_complete, token)
                        while(request_list == ['token_caducado']):
                            token = get_token()
                            request_list = get_data(url_complete, token)
                        if(request_list):
                            consulta_ruc(request_list, identificacion)
                        else:
                            print("ERROR: Consulta de RUC")
                    else:
                        print("ERROR: %s no es un RUC válido" % ruc)
                if(urls[j][0] == 'impuesto_causado'):
                    print("PROCESO: Extrayendo datos Impuestos")
                    if(len(identificacion) == 10):
                        url_complete = "https://srienlinea.sri.gob.ec/sri-declaraciones-impuesto-renta-servicio-internet/rest/RentaNaturalesAnexos/obtenerConsultaRentaNaturales?identificacion=" + identificacion
                        request_list = get_data(url_complete, token)
                        while(request_list == ['token_caducado']):
                            token = get_token()
                            request_list = get_data(url_complete, token)
                        if(request_list):
                            impuesto_causado_natural(request_list, identificacion)
                        else:
                            # Si no consigue datos con la cédula, lo hace con el RUC
                            print("ERROR: Consulta Impuesto Causado")
                            print("Intentado buscar Impuestos con el RUC...")
                            url_complete = "https://srienlinea.sri.gob.ec/sri-catastro-sujeto-servicio-internet/rest/Persona/obtenerPersonaDesdeRucPorIdentificacionConToken?numeroRuc=" + ruc
                            request_list = get_data(url_complete, token)
                            while(request_list == ['token_caducado']):
                                token = get_token()
                                request_list = get_data(url_complete, token)
                            try:
                                tipoPersona = is_natural_juridico(request_list)
                            except:
                                tipoPersona = "Desconocido"
                            if(tipoPersona == "natural"):
                                url_complete = "https://srienlinea.sri.gob.ec/sri-declaraciones-impuesto-renta-servicio-internet/rest/RentaNaturalesAnexos/obtenerConsultaRentaNaturales?identificacion=" + ruc
                                request_list = get_data(url_complete, token)
                                if(request_list):
                                    impuesto_causado_natural(request_list, identificacion)
                                else:
                                    print("ERROR: Consulta Impuesto Causado")
                            if(tipoPersona == "juridico"):
                                url_complete = "https://srienlinea.sri.gob.ec/sri-declaraciones-impuesto-renta-servicio-internet/rest/RentaJuridicos/obtenerConsultaRentaJuridicos?identificacion=" + ruc
                                request_list = get_data(url_complete, token)
                                if(request_list):
                                    impuesto_causado_juridico(request_list, identificacion)
                                else:
                                    print("ERROR: Consulta Impuesto Causado")
                            if(tipoPersona == "Desconocido"):
                                print("ERROR: No se ha podido comprobar si es Natural/Jurídico")
                                print("ERROR: Consulta Impuesto Causado")
                    if(len(identificacion) == 13):
                        url_complete = "https://srienlinea.sri.gob.ec/sri-catastro-sujeto-servicio-internet/rest/Persona/obtenerPersonaDesdeRucPorIdentificacionConToken?numeroRuc" + identificacion
                        request_list = get_data(url_complete, token)
                        while(request_list == ['token_caducado']):
                            token = get_token()
                            request_list = get_data(url_complete, token)
                        try:
                            tipoPersona = is_natural_juridico(request_list)
                        except:
                            tipoPersona = "Desconocido"
                        if(tipoPersona == "natural"):
                            url_complete = "https://srienlinea.sri.gob.ec/sri-declaraciones-impuesto-renta-servicio-internet/rest/RentaNaturalesAnexos/obtenerConsultaRentaNaturales?identificacion=" + identificacion
                            request_list = get_data(url_complete, token)
                            impuesto_causado_natural(request_list, id_persona)
                        if(tipoPersona == "juridico"):
                            url_complete = "https://srienlinea.sri.gob.ec/sri-declaraciones-impuesto-renta-servicio-internet/rest/RentaJuridicos/obtenerConsultaRentaJuridicos?identificacion=" + identificacion
                            request_list = get_data(url_complete, token)
                            impuesto_causado_juridico(request_list, id_persona)
                        if(tipoPersona == "Desconocido"):
                            print("ERROR: No se ha podido comprobar si es Natura/Jurídico")
                            print("ERROR: Consulta Impuesto Causado")
            except:
                print("ERROR: Desconocido")
            j += 1
    else:
        print("ERROR: %s no es una identificacion válida" % identificacion)
        
    print("")
    gc.collect()
    i += 1
  
#
#
# HASTA AQUÍ
#
#
 



# Funciones de insersión de filas en Base de Datos
def insert_persona(cedula_persona, ruc_persona):
  query = "INSERT INTO %s (cedula_persona, ruc_persona) VALUES ('%s', '%s') ON CONFLICT DO NOTHING" % (nombre_tabla, cedula_persona, ruc_persona)
  cnx.execute(query)
    
def update_persona(query):
  cnx.execute(query)


if __name__ == '__main__' :
  main()
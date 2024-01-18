"""
Función cargar_dataframe: Esta función tomará un path y retornará un DataFrame de pandas cargado con los datos del archivo CSV.

Función procesar_dataframe: Esta función tomará un DataFrame como entrada y realizará las siguientes tareas:

1.-Extraer y separar los puertos de origen y destino.
2.-Convertir las direcciones IP (IPv4 e IPv6) a un formato entero.
3.-Convertir los protocolos a números, utilizando un diccionario predefinido.

Dentro de procesar_dataframe, crearemos funciones auxiliares específicas para cada tarea:

Función extraer_puertos: Para separar los puertos de origen y destino.

Función ip_a_entero: Para convertir direcciones IP a enteros.

Función protocolo_a_numero: Para convertir nombres de protocolos a números basados en un mapeo predefinido.
"""
import pandas as pd
import re
import protocol
from sklearn.preprocessing import MinMaxScaler


# Función para convertir IPv4 a entero
def ipv4_to_int(ipv4_str):
    parts = ipv4_str.split(".")
    return sum(int(part) << (8 * i) for i, part in enumerate(reversed(parts)))

# Función para aplicar el hash FNV a una cadena
def fnv_hash(ip_str):
    # FNV-1a hash
    hash = 0x811c9dc5
    for byte in ip_str.encode():
        hash ^= byte
        hash *= 0x01000193
    return hash & 0xffffffff  # Limitando a 32-bit para mantener el tamaño manejable

# Función para serializar la dirección IP
def ip_a_entero(ip_address):
    try:
        # Intentar parsear y convertir como IPv4
        return ipv4_to_int(ip_address)
    except ValueError:
        # Si falla, asumir que es IPv6 y aplicar FNV hash
        return fnv_hash(ip_address)

# Función para cargar el DataFrame desde un path
def cargar_dataframe(path):
    return pd.read_csv(path)

# Función para convertir protocolo a número
def protocolo_a_numero(protocolo, protocolo_numero_mapa):
  if protocolo in protocolo_numero_mapa:
      return protocolo_numero_mapa[protocolo]
  else:
      # Asignar un nuevo número al protocolo desconocido
      max_value = max(protocolo_numero_mapa.values())
      protocolo_numero_mapa[protocolo] = max_value + 1
      return max_value + 1

# Función para procesar el DataFrame
def procesar_dataframe(df, protocolo_numero_mapa):
    # Extraer puertos de origen y destino
    # Convertir IPs a enteros
    df['Source Num'] = df['Source'].apply(ip_a_entero)
    df['Destination Num'] = df['Destination'].apply(ip_a_entero)

    # Convertir protocolo a número
    df['Protocol Num'] = df['Protocol'].apply(lambda x: protocolo_a_numero(x, protocolo_numero_mapa))
    df = df.drop(columns=[ 'Source','Destination', 'Protocol'])

    return df
def guardar_dataframe_txt(df, path):
  # Guardar el DataFrame en un archivo TXT
  with open(path, 'w') as file:
      file.write(df.to_string())
def guardar_dataframe_csv(df, path):
# Guardar el DataFrame en un archivo CSV
  df.to_csv(path, index=False)
# Mapeo de protocolos a números


# Ejemplo de uso
path = 'prueba2.csv'
df = cargar_dataframe(path)
df_procesado = procesar_dataframe(df, protocol.protocolo_numero_mapa)
  
df_procesado.head()

scaler = MinMaxScaler()
df_normalized = pd.DataFrame(scaler.fit_transform(df_procesado), columns=df_procesado.columns)

guardar_dataframe_csv(df_normalized, 'output.csv')

print (df_procesado)
# Mostrar las primeras filas del DataFrame procesado

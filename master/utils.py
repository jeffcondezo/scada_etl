import pandas as pd
from master.models import Homologacion, Nivel, Central, ScadaTemporal
import pyodbc
from django.conf import settings
from datetime import datetime
from django.utils import timezone
from django.db.models import Min, Max
from datetime import timedelta
import logging
import time
from collections import defaultdict


def importar_tag_sro_a_homologacion(ruta_archivo):
    """
    Lee un archivo Excel de tags SRO y almacena la información en la tabla Homologacion.
    La primera columna del archivo es id_scada, la segunda es cabecera.
    El campo nivel se asigna por defecto con id 1 y estado se asigna como True.
    """
    df = pd.read_excel(ruta_archivo)
    for _, fila in df.iterrows():
        id_scada = str(fila[0]).strip()
        cabecera = str(fila[1]).strip()
        try:
            nivel = Nivel.objects.get(descripcion=str(fila[3]).strip(), central__descripcion=str(fila[2]).strip())
        except Nivel.DoesNotExist:
            try:
                central = Central.objects.get(descripcion=str(fila[2]).strip())
                nivel = Nivel.objects.create(
                    descripcion=str(fila[3]).strip(),
                    central=central,
                    codigo=str(fila[3]).strip()
                )
            except Central.DoesNotExist:
                # Si no existe la central, se crea una nueva central por defecto
                central = Central.objects.create(
                    descripcion=str(fila[2]).strip(),
                    codigo=str(fila[2]).strip()
                )
                nivel = Nivel.objects.create(
                    descripcion=str(fila[3]).strip(),
                    central=central,
                    codigo=str(fila[3]).strip()
                )  
            
        if str(fila[6]).strip() == '1':
            estado = True
        else:     
            estado = False

        Homologacion.objects.create(
            id_scada=id_scada,
            cabecera_cmd=cabecera,
            nivel=nivel,
            estado=estado
        )


def crear_tabla_sqlserver_con_cabeceras():
    """
    Crea una tabla en SQL Server con una columna por cada valor único de cabecera_cmd en Homologacion,
    y siempre agrega una columna adicional llamada 'timestamp'.
    Obtiene los datos de conexión desde settings.py.
    """
    db_settings = settings.DATABASES['default']
    server = 'DESKTOP-0SI1RPI'
    database = 'scada'
    username = 'root'
    password = 'wolf_4030'

    central = Central.objects.all()

    for c in central:
        nombre_tabla = 'CMD' + c.descripcion.replace(' ', '_')
        columnas = Homologacion.objects.filter(nivel__central=c).values_list('cabecera_cmd', flat=True).distinct()
        columnas = [col.replace(' ', '_') for col in columnas]

        # Agrega la columna timestamp al inicio
        columnas_sql = '[timestamp] DATETIME, ' + ', '.join([f'[{col}] NVARCHAR(MAX)' for col in columnas])
        sql = f"CREATE TABLE [{nombre_tabla}] ({columnas_sql});"

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print("Error al crear la tabla:", sql)




def importar_valores_scada_desde_sqlserver():
    """
    Extrae los id_scada activos de Homologacion, consulta en SQL Server por esos IDs
    con Quality=192 y TimeStamp en el rango dado, y guarda los resultados en ScadaTemporal.
    """
    fecha_inicio = datetime(2025, 6, 1, 7, 27, 0)
    fecha_fin = datetime(2025, 6, 1, 7, 45, 59)

    # 1. Obtener los id_scada activos
    homologaciones = Homologacion.objects.filter(estado=True)
    ids_scada = list(homologaciones.values_list('id_scada', flat=True))
    niveles = {h.id_scada: h.nivel for h in homologaciones}

    if not ids_scada:
        print("No hay id_scada activos.")
        return

    # 2. Conexión a SQL Server
    db_settings = settings.DATABASES['default']
    server = 'DESKTOP-0SI1RPI'
    database = 'OPCUAs60Mini'
    username = 'root'
    password = 'wolf_4030'

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # 3. Consulta por cada id_scada
    for id_scada in ids_scada:
        query = """
            SELECT ID, Value, TimeStamp
            FROM dbo.HistoricalData
            WHERE ID = ?
              AND Quality = 192
              AND TimeStamp BETWEEN ? AND ?
            ORDER BY TimeStamp ASC
        """
        cursor.execute(query, id_scada, fecha_inicio, fecha_fin)
        rows = cursor.fetchall()
        nivel = niveles[id_scada]
        minutos_vistos = set()
        for row in rows:
            minuto = row.TimeStamp.replace(second=0, microsecond=0)
            if minuto in minutos_vistos:
                continue
            minutos_vistos.add(minuto)
            ScadaTemporal.objects.create(
                id_scada=row.ID,
                cabecera_cmd=Homologacion.objects.get(id_scada=row.ID).cabecera_cmd,
                timestamp=timezone.make_aware(minuto),
                valor=float(str(row.Value).replace(',', '.')),
                nivel=nivel,

            )
    cursor.close()
    conn.close()


def completar_minutos_faltantes_scadatemporal():
    """
    Para cada id_scada en ScadaTemporal, verifica si hay un registro por minuto en el intervalo dado.
    Si faltan minutos, interpola linealmente el valor y crea el registro faltante.
    """

    fecha_inicio = datetime(2025, 6, 1, 7, 27, 0)
    fecha_fin = datetime(2025, 6, 1, 7, 45, 59)

    ids = ScadaTemporal.objects.filter(timestamp__range=(fecha_inicio, fecha_fin)).values_list('id_scada', flat=True).distinct()
    for id_scada in ids:
        # Traer todos los registros ordenados por timestamp
        registros = list(
            ScadaTemporal.objects.filter(
                id_scada=id_scada,
                timestamp__range=(fecha_inicio, fecha_fin)
            ).order_by('timestamp')
        )
        if not registros:
            continue

        # Crear un dict {timestamp: registro}
        registros_por_minuto = {r.timestamp.replace(second=0, microsecond=0): r for r in registros}

        # Definir el rango de minutos a revisar
        t_actual = timezone.make_aware(fecha_inicio.replace(second=0, microsecond=0))
        t_final = timezone.make_aware(fecha_fin.replace(second=0, microsecond=0))

        while t_actual <= t_final:
            if t_actual not in registros_por_minuto:
                # Buscar los registros anterior y posterior para interpolar
                prev = next((r for r in reversed(registros) if r.timestamp.replace(second=0, microsecond=0) < t_actual), None)
                next_ = next((r for r in registros if r.timestamp.replace(second=0, microsecond=0) > t_actual), None)
                if prev and next_:
                    # Interpolación lineal
                    total_secs = (next_.timestamp - prev.timestamp).total_seconds()
                    if total_secs == 0:
                        valor_interp = prev.valor
                    else:
                        secs_to_t = (t_actual - prev.timestamp).total_seconds()
                        valor_interp = prev.valor + (next_.valor - prev.valor) * (secs_to_t / total_secs)
                    ScadaTemporal.objects.create(
                        id_scada=id_scada,
                        cabecera_cmd=prev.cabecera_cmd,
                        valor=valor_interp,
                        timestamp=t_actual,
                        nivel=prev.nivel
                    )
            t_actual += timedelta(minutes=1)


def exportar_scadatemporal_a_sqlserver():
    """
    Exporta los datos de ScadaTemporal a las tablas correspondientes en la base de datos SCADA en SQL Server.
    Cada tabla tiene una columna 'timestamp' y columnas por cada cabecera_cmd.
    """
    # Conexión a SQL Server
    db_settings = settings.DATABASES['default']
    server = 'DESKTOP-0SI1RPI'
    database = 'scada'
    username = 'root'
    password = 'wolf_4030'

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Obtener todas las centrales
    centrales = Central.objects.filter(estado=True)

    for central in centrales:
        nombre_tabla = 'CMD' + central.descripcion.replace(' ', '_')
        # Obtener todos los registros de ScadaTemporal para esta central
        niveles = Nivel.objects.filter(central=central)
        registros = ScadaTemporal.objects.filter(nivel__in=niveles).order_by('timestamp')

        # Agrupar por timestamp
        datos_por_minuto = {}
        for reg in registros:
            minuto = reg.timestamp.replace(second=0, microsecond=0)
            if minuto not in datos_por_minuto:
                datos_por_minuto[minuto] = {}
            datos_por_minuto[minuto][reg.cabecera_cmd.replace(' ', '_')] = reg.valor

        # Obtener todas las cabeceras para las columnas
        cabeceras = Homologacion.objects.filter(nivel__central=central, estado=True).values_list('cabecera_cmd', flat=True)
        cabeceras = [c.replace(' ', '_') for c in cabeceras]

        for minuto, valores in datos_por_minuto.items():
            columnas = ['timestamp'] + cabeceras
            valores_insert = [minuto] + [valores.get(c, None) for c in cabeceras]
            placeholders = ','.join(['?'] * len(columnas))
            sql = f"INSERT INTO [{nombre_tabla}] ({','.join('['+c+']' for c in columnas)}) VALUES ({placeholders})"
            try:
                cursor.execute(sql, *valores_insert)
            except Exception as e:
                print(f"Error insertando en {nombre_tabla} para {minuto}: {e}")

    conn.commit()
    cursor.close()
    conn.close()


def comparar_scadatemporal_con_sqlserver():
    """
    Compara los datos de ScadaTemporal con las tablas de SQL Server.
    Si encuentra diferencias, las registra en un archivo log.
    """
    # Configurar logging
    logging.basicConfig(filename='comparacion_scada.log', level=logging.INFO, 
                        format='%(asctime)s %(levelname)s:%(message)s')

    db_settings = settings.DATABASES['default']
    server = 'DESKTOP-0SI1RPI'
    database = 'scada'
    username = 'root'
    password = 'wolf_4030'

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    centrales = Central.objects.filter(estado=True)

    for central in centrales:
        nombre_tabla = 'CMD' + central.descripcion.replace(' ', '_')
        niveles = Nivel.objects.filter(central=central)
        registros = ScadaTemporal.objects.filter(nivel__in=niveles).order_by('timestamp')

        cabeceras = Homologacion.objects.filter(nivel__central=central, estado=True).values_list('cabecera_cmd', flat=True)
        cabeceras = [c.replace(' ', '_') for c in cabeceras]

        for reg in registros:
            minuto = reg.timestamp.replace(second=0, microsecond=0)
            columna = reg.cabecera_cmd.replace(' ', '_')
            if columna not in cabeceras:
                continue  # Solo compara columnas válidas

            # Consulta el valor en SQL Server
            sql = f"SELECT [{columna}] FROM [{nombre_tabla}] WHERE [timestamp]=?"
            cursor.execute(sql, minuto)
            row = cursor.fetchone()
            valor_sql = row[0] if row else None

            # Compara valores (considera None y float)
            valor_django = reg.valor
            try:
                valor_sql_float = float(str(valor_sql).replace(',', '.')) if valor_sql is not None else None
            except Exception:
                valor_sql_float = None

            if valor_sql_float != valor_django:
                logging.info(
                    f"Diferencia en {nombre_tabla} - timestamp: {minuto}, columna: {columna}, "
                    f"Django: {valor_django}, SQLServer: {valor_sql_float}"
                )

    cursor.close()
    conn.close()



def ejecutar_proceso_etl_completo():
    """
    Ejecuta el proceso ETL completo y mide el tiempo de ejecución de cada función,
    registrando los tiempos en un archivo de log.
    """
    logging.basicConfig(filename='etl_scada.log', level=logging.INFO, 
                        format='%(asctime)s %(levelname)s:%(message)s')

    funciones = [
        ('importar_valores_scada_desde_sqlserver', importar_valores_scada_desde_sqlserver2),
        ('completar_minutos_faltantes_scadatemporal', completar_minutos_faltantes_scadatemporal),
        ('exportar_scadatemporal_a_sqlserver', exportar_scadatemporal_a_sqlserver),
        ('comparar_scadatemporal_con_sqlserver', comparar_scadatemporal_con_sqlserver),
    ]

    for nombre, funcion in funciones:
        inicio = time.time()
        try:
            funcion()
            duracion = time.time() - inicio
            logging.info(f"Función '{nombre}' ejecutada en {duracion:.2f} segundos.")
        except Exception as e:
            logging.error(f"Error ejecutando '{nombre}': {e}")



def importar_valores_scada_desde_sqlserver2():
    fecha_inicio = datetime(2025, 6, 1, 7, 27, 0)
    fecha_fin = datetime(2025, 6, 1, 7, 45, 59)

    homologaciones = Homologacion.objects.filter(estado=True)
    ids_scada = list(homologaciones.values_list('id_scada', flat=True))
    if not ids_scada:
        print("No hay id_scada activos.")
        return

    niveles = {h.id_scada: h.nivel for h in homologaciones}
    cabeceras = {h.id_scada: h.cabecera_cmd for h in homologaciones}

    db_settings = settings.DATABASES['default']
    server = 'DESKTOP-0SI1RPI'
    database = 'OPCUAs60Mini'
    username = 'root'
    password = 'wolf_4030'

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    placeholders = ','.join(['?'] * len(ids_scada))
    query = f"""
        SELECT ID, Value, TimeStamp
        FROM dbo.HistoricalData
        WHERE ID IN ({placeholders})
          AND Quality = 192
          AND TimeStamp BETWEEN ? AND ?
        ORDER BY ID, TimeStamp ASC
    """
    params = ids_scada + [fecha_inicio, fecha_fin]
    cursor.execute(query, *params)
    rows = cursor.fetchall()

    minutos_vistos = defaultdict(set)
    objetos = []
    for row in rows:
        id_scada = row.ID
        minuto = row.TimeStamp.replace(second=0, microsecond=0)
        if minuto in minutos_vistos[id_scada]:
            continue
        minutos_vistos[id_scada].add(minuto)
        objetos.append(
            ScadaTemporal(
                id_scada=id_scada,
                cabecera_cmd=cabeceras[id_scada],
                timestamp=timezone.make_aware(minuto),
                valor=float(str(row.Value).replace(',', '.')),
                nivel=niveles[id_scada],
            )
        )
    if objetos:
        ScadaTemporal.objects.bulk_create(objetos, batch_size=1000)

    cursor.close()
    conn.close()
import pandas as pd
from master.models import Homologacion, Nivel, Central, ScadaTemporal, ETLProcessState, ETLProcessLog, ETLProcessStateCron, ETLProcessLogCron, Parametro
import pyodbc
from django.conf import settings
from datetime import datetime, timedelta
from django.utils import timezone
from django.db.models import Min, Max
from datetime import timedelta
import logging
import time
from collections import defaultdict
from zoneinfo import ZoneInfo
from bisect import bisect_left
from django.db import transaction
from functools import wraps
from django.http import HttpResponseForbidden
from django.utils.dateparse import parse_datetime


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
    server = settings.DB_SQL_SERVER
    database = settings.DB_SQL_DATABASE
    username = settings.DB_SQL_USERNAME
    password = settings.DB_SQL_PASSWORD

    central = Central.objects.all()

    for c in central:
        nombre_tabla = 'CMD' + c.descripcion.replace(' ', '_')
        columnas = Homologacion.objects.filter(nivel__central=c).values_list('cabecera_cmd', flat=True).distinct()
        columnas = [col.replace(' ', '_') for col in columnas]

        # Agrega la columna timestamp al inicio
        columnas_sql = '[timestamp] DATETIME, ' + ', '.join([f'[{col}] DECIMAL(10, 3)' for col in columnas])
        sql = f"CREATE TABLE [{nombre_tabla}] ({columnas_sql});"

        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "TrustServerCertificate=Yes;"
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




def importar_valores_scada_desde_sqlserver(fecha_inicio, fecha_fin):
    """
    Extrae los id_scada activos de Homologacion, consulta en SQL Server por esos IDs
    con Quality=192 y TimeStamp en el rango dado, y guarda los resultados en ScadaTemporal.
    """
    # 1. Obtener los id_scada activos
    homologaciones = Homologacion.objects.filter(estado=True)
    ids_scada = list(homologaciones.values_list('id_scada', flat=True))
    niveles = {h.id_scada: h.nivel for h in homologaciones}

    if not ids_scada:
        print("No hay id_scada activos.")
        return

    # 2. Conexión a SQL Server
    server = settings.DB_SQL_SERVER
    database = settings.DB_SQL_DATABASE_SCADA
    username = settings.DB_SQL_USERNAME
    password = settings.DB_SQL_PASSWORD

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=Yes;"
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
                timestamp_utc=timezone.make_aware(minuto.replace(tzinfo=ZoneInfo('America/Lima'))),
                valor=float(str(row.Value).replace(',', '.')),
                nivel=nivel,

            )
    cursor.close()
    conn.close()


def completar_minutos_faltantes_scadatemporal(fecha_inicio, fecha_fin):
    """
    Para cada id_scada en ScadaTemporal, verifica si hay un registro por minuto en el intervalo dado.
    Si faltan minutos, interpola linealmente el valor y crea el registro faltante.
    """

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
                        timestamp_utc=t_actual - timedelta(hours=5),
                        nivel=prev.nivel,
                        tipo='2'
                    )
            t_actual += timedelta(minutes=1)
    # Eliminar los booleanos que se hayan interpolado
    ScadaTemporal.objects.filter(tipo='2', cabecera_cmd__in=Homologacion.objects.filter(tipo='2').values_list('cabecera_cmd', flat=True)).delete()
            

def exportar_scadatemporal_a_sqlserver(fecha_inicio, fecha_fin):
    """
    Exporta solo los datos de ScadaTemporal del día proporcionado por los parámetros fecha_inicio y fecha_fin
    a las tablas correspondientes en la base de datos SCADA en SQL Server.
    Si el registro con ese timestamp existe, actualiza los campos; si no existe, lo crea.
    """
    server = settings.DB_SQL_SERVER
    database = settings.DB_SQL_DATABASE
    username = settings.DB_SQL_USERNAME
    password = settings.DB_SQL_PASSWORD

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=Yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    centrales = Central.objects.filter(estado=True)

    for central in centrales:
        nombre_tabla = 'CMD' + central.descripcion.replace(' ', '_')
        niveles = Nivel.objects.filter(central=central)
        registros = ScadaTemporal.objects.filter(
            nivel__in=niveles,
            timestamp_utc__range=(fecha_inicio, fecha_fin)
        ).order_by('timestamp_utc')

        datos_por_minuto = {}
        for reg in registros:
            minuto = reg.timestamp_utc.replace(second=0, microsecond=0)
            if minuto not in datos_por_minuto:
                datos_por_minuto[minuto] = {}
            datos_por_minuto[minuto][reg.cabecera_cmd.replace(' ', '_')] = reg.valor

        cabeceras = Homologacion.objects.filter(nivel__central=central, estado=True, nivel__estado=True, nivel__central__estado=True).values_list('cabecera_cmd', flat=True)
        cabeceras = [c.replace(' ', '_') for c in cabeceras]

        for minuto, valores in datos_por_minuto.items():
            # Verifica si el registro existe
            cursor.execute(f"SELECT COUNT(*) FROM [{nombre_tabla}] WHERE [timestamp]=?", minuto)
            existe = cursor.fetchone()[0] > 0

            columnas = cabeceras
            valores_update = [valores.get(c, None) for c in columnas]

            if existe:
                # Actualiza solo los campos correspondientes
                set_clause = ', '.join([f"[{col}]=?" for col in columnas])
                sql = f"UPDATE [{nombre_tabla}] SET {set_clause} WHERE [timestamp]=?"
                try:
                    cursor.execute(sql, *valores_update, minuto)
                except Exception as e:
                    print(f"Error actualizando en {nombre_tabla} para {minuto}: {e}")
            else:
                # Inserta el registro nuevo
                columnas_insert = ['timestamp'] + columnas
                valores_insert = [minuto] + valores_update
                placeholders = ','.join(['?'] * len(columnas_insert))
                sql = f"INSERT INTO [{nombre_tabla}] ({','.join('['+c+']' for c in columnas_insert)}) VALUES ({placeholders})"
                try:
                    cursor.execute(sql, *valores_insert)
                except Exception as e:
                    print(f"Error insertando en {nombre_tabla} para {minuto}: {e}")

    conn.commit()
    cursor.close()
    conn.close()


def comparar_scadatemporal_con_sqlserver(fecha_inicio, fecha_fin):
    """
    Compara los datos de ScadaTemporal con las tablas de SQL Server.
    Si encuentra diferencias (considerando solo hasta 3 decimales), las registra en un archivo log.
    """
    logging.basicConfig(filename='comparacion_scada.log', level=logging.INFO, 
                        format='%(asctime)s %(levelname)s:%(message)s')

    server = settings.DB_SQL_SERVER
    database = settings.DB_SQL_DATABASE
    username = settings.DB_SQL_USERNAME
    password = settings.DB_SQL_PASSWORD

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=Yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    centrales = Central.objects.filter(estado=True)

    for central in centrales:
        nombre_tabla = 'CMD' + central.descripcion.replace(' ', '_')
        niveles = Nivel.objects.filter(central=central)
        registros = ScadaTemporal.objects.filter(nivel__in=niveles).order_by('timestamp_utc')

        cabeceras = Homologacion.objects.filter(nivel__central=central, estado=True).values_list('cabecera_cmd', flat=True)
        cabeceras = [c.replace(' ', '_') for c in cabeceras]

        for reg in registros:
            minuto = reg.timestamp_utc.replace(second=0, microsecond=0)
            columna = reg.cabecera_cmd.replace(' ', '_')
            if columna not in cabeceras:
                continue  # Solo compara columnas válidas

            # Consulta el valor en SQL Server
            sql = f"SELECT [{columna}] FROM [{nombre_tabla}] WHERE [timestamp]=?"
            cursor.execute(sql, minuto)
            row = cursor.fetchone()
            valor_sql = row[0] if row else None

            # Compara valores hasta 3 decimales
            valor_django = reg.valor
            try:
                valor_sql_float = float(str(valor_sql).replace(',', '.')) if valor_sql is not None else None
            except Exception:
                valor_sql_float = None

            iguales = False
            if valor_sql_float is None and valor_django is None:
                iguales = True
            elif valor_sql_float is not None and valor_django is not None:
                iguales = round(valor_sql_float, 3) == round(valor_django, 3)

            if not iguales:
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
    
    fecha_inicio = datetime(2024, 7, 1, 5, 0, 0)
    fecha_fin = datetime(2024, 7, 2, 4, 59, 59)

    logging.basicConfig(filename='etl_scada.log', level=logging.INFO, 
                        format='%(asctime)s %(levelname)s:%(message)s')

    funciones = [
        ('importar_valores_scada_desde_sqlserver', importar_valores_scada_desde_sqlserver2),
        ('completar_minutos_faltantes_scadatemporal', completar_minutos_faltantes_scadatemporal2),
        ('exportar_scadatemporal_a_sqlserver', exportar_scadatemporal_a_sqlserver),
        ('comparar_scadatemporal_con_sqlserver', comparar_scadatemporal_con_sqlserver2),
    ]

    for nombre, funcion in funciones:
        inicio = time.time()
        try:
            funcion(fecha_inicio, fecha_fin)
            duracion = time.time() - inicio
            logging.info(f"Función '{nombre}' ejecutada en {duracion:.2f} segundos.")
        except Exception as e:
            logging.error(f"Error ejecutando '{nombre}': {e}")



def importar_valores_scada_desde_sqlserver2(fecha_inicio, fecha_fin):

    homologaciones = Homologacion.objects.filter(estado=True, nivel__estado=True, nivel__central__estado=True)
    ids_scada = list(homologaciones.values_list('id_scada', flat=True))
    if not ids_scada:
        print("No hay id_scada activos.")
        return

    niveles = {h.id_scada: h.nivel for h in homologaciones}
    cabeceras = {h.id_scada: h.cabecera_cmd for h in homologaciones}

    server = settings.DB_SQL_SERVER
    database = settings.DB_SQL_DATABASE_SCADA
    username = settings.DB_SQL_USERNAME
    password = settings.DB_SQL_PASSWORD

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=Yes;"
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
                timestamp_utc=minuto - timedelta(hours=5),
            )
        )
    if objetos:
        ScadaTemporal.objects.bulk_create(objetos, batch_size=1000)

    cursor.close()
    conn.close()


def limpiar_scadatemporal_y_sqlserver():
    """
    Elimina todos los registros de ScadaTemporal y restablece su secuencia a 1.
    Hace lo mismo para todas las tablas CMD* en SQL Server.
    """
    from django.db import connection

    # Limpiar ScadaTemporal y resetear secuencia (para PostgreSQL y MySQL)
    ScadaTemporal.objects.all().delete()
    with connection.cursor() as cursor:
        # Para PostgreSQL
        try:
            cursor.execute("ALTER SEQUENCE master_scadatemporal_id_seq RESTART WITH 1;")
        except Exception:
            pass
        # Para MySQL
        try:
            cursor.execute("ALTER TABLE master_scadatemporal AUTO_INCREMENT = 1;")
        except Exception:
            pass

    # Limpiar tablas CMD* en SQL Server
    server = settings.DB_SQL_SERVER
    database = settings.DB_SQL_DATABASE
    username = settings.DB_SQL_USERNAME
    password = settings.DB_SQL_PASSWORD

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=Yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Buscar todas las tablas que empiezan con CMD
    cursor.execute("""
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE' AND TABLE_NAME LIKE 'CMD%'
    """)
    tablas = [row[0] for row in cursor.fetchall()]

    for tabla in tablas:
        try:
            cursor.execute(f"TRUNCATE TABLE [{tabla}]")
            # Si hay un campo IDENTITY, reiniciar el contador
            cursor.execute(f"DBCC CHECKIDENT ('{tabla}', RESEED, 0)")
        except Exception as e:
            print(f"Error limpiando {tabla}: {e}")

    conn.commit()
    cursor.close()
    conn.close()



def limpiar_historicaldata_ids_no_homologados():
    """
    Elimina los registros de dbo.HistoricalData en SQL Server donde ID no está en la tabla Homologacion con estado=True.
    """
    # Obtener los id_scada activos de Homologacion
    ids_validos = list(Homologacion.objects.filter(estado=True).values_list('id_scada', flat=True))
    if not ids_validos:
        print("No hay id_scada activos.")
        return

    server = settings.DB_SQL_SERVER
    database = settings.DB_SQL_DATABASE_SCADA
    username = settings.DB_SQL_USERNAME
    password = settings.DB_SQL_PASSWORD

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=Yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Construir la lista de IDs para la consulta SQL
    # Si hay muchos IDs, considera hacer la operación en bloques
    ids_validos_str = ','.join(f"'{id_}'" for id_ in ids_validos)
    sql = f"DELETE FROM dbo.HistoricalData WHERE ID NOT IN ({ids_validos_str})"

    try:
        cursor.execute(sql)
        conn.commit()
        print("Registros eliminados correctamente de dbo.HistoricalData.")
    except Exception as e:
        print(f"Error eliminando registros: {e}")

    cursor.close()
    conn.close()


def completar_minutos_faltantes_scadatemporal2(fecha_inicio, fecha_fin):
    """
    Interpola minutos faltantes en memoria y usa bulk_create.
    Busca hasta 2 días previos y posteriores para interpolar los extremos.
    Si no encuentra, deja el valor en blanco.
    """
    ids = ScadaTemporal.objects.filter(
        timestamp__range=(fecha_inicio, fecha_fin)
    ).values_list('id_scada', flat=True).distinct()

    for id_scada in ids:
        # Buscar registros en el rango extendido para los extremos
        rango_extendido_inicio = fecha_inicio - timedelta(days=2)
        rango_extendido_fin = fecha_fin + timedelta(days=2)
        registros_ext = list(
            ScadaTemporal.objects.filter(
                id_scada=id_scada,
                timestamp__range=(rango_extendido_inicio, rango_extendido_fin)
            ).order_by('timestamp')
        )
        if not registros_ext:
            continue

        # Lista de minutos existentes y sus valores
        minutos_existentes = [r.timestamp.replace(second=0, microsecond=0) for r in registros_ext]
        registros_dict = {r.timestamp.replace(second=0, microsecond=0): r for r in registros_ext}

        # Rango de minutos a revisar (solo el rango solicitado)
        t_actual = timezone.make_aware(fecha_inicio.replace(second=0, microsecond=0))
        t_final = timezone.make_aware(fecha_fin.replace(second=0, microsecond=0))

        nuevos = []
        while t_actual <= t_final:
            if t_actual not in registros_dict:
                # Buscar posición para interpolar usando bisect
                idx = bisect_left(minutos_existentes, t_actual)
                prev = None
                next_ = None
                if 0 < idx < len(minutos_existentes):
                    prev = registros_dict[minutos_existentes[idx - 1]]
                    next_ = registros_dict[minutos_existentes[idx]]
                elif idx == 0 and len(minutos_existentes) > 1:
                    # Solo hay siguiente, buscar hasta 2 días después
                    next_ = registros_dict[minutos_existentes[0]]
                    # Buscar previo hasta 2 días antes
                    prevs = ScadaTemporal.objects.filter(
                        id_scada=id_scada,
                        timestamp__lt=minutos_existentes[0],
                        timestamp__gte=rango_extendido_inicio
                    ).order_by('-timestamp')
                    if prevs.exists():
                        prev = prevs.first()
                elif idx == len(minutos_existentes):
                    # Solo hay previo, buscar hasta 2 días después
                    prev = registros_dict[minutos_existentes[-1]]
                    nexts = ScadaTemporal.objects.filter(
                        id_scada=id_scada,
                        timestamp__gt=minutos_existentes[-1],
                        timestamp__lte=rango_extendido_fin
                    ).order_by('timestamp')
                    if nexts.exists():
                        next_ = nexts.first()

                if prev and next_:
                    total_secs = (next_.timestamp - prev.timestamp).total_seconds()
                    if total_secs == 0:
                        valor_interp = prev.valor
                    else:
                        secs_to_t = (t_actual - prev.timestamp).total_seconds()
                        valor_interp = prev.valor + (next_.valor - prev.valor) * (secs_to_t / total_secs)
                    nuevos.append(
                        ScadaTemporal(
                            id_scada=id_scada,
                            cabecera_cmd=prev.cabecera_cmd,
                            valor=valor_interp,
                            timestamp=t_actual,
                            timestamp_utc=t_actual - timedelta(hours=5),
                            nivel=prev.nivel,
                            tipo='2'
                        )
                    )
                # Si no hay ambos extremos, no interpola (deja en blanco)
            t_actual += timedelta(minutes=1)

        # Bulk create para eficiencia
        if nuevos:
            with transaction.atomic():
                ScadaTemporal.objects.bulk_create(nuevos, batch_size=1000)


def comparar_scadatemporal_con_sqlserver2(fecha_inicio, fecha_fin):
    """
    Compara los datos de ScadaTemporal con las tablas de SQL Server.
    Si encuentra diferencias mayores a 0.005, las registra en un archivo log.
    """
    logging.basicConfig(filename='comparacion_scada.log', level=logging.INFO, 
                        format='%(asctime)s %(levelname)s:%(message)s')

    server = settings.DB_SQL_SERVER
    database = settings.DB_SQL_DATABASE
    username = settings.DB_SQL_USERNAME
    password = settings.DB_SQL_PASSWORD

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=Yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    centrales = Central.objects.filter(estado=True)

    for central in centrales:
        nombre_tabla = 'CMD' + central.descripcion.replace(' ', '_')
        niveles = Nivel.objects.filter(central=central)
        registros = ScadaTemporal.objects.filter(nivel__in=niveles).order_by('timestamp_utc')

        cabeceras = Homologacion.objects.filter(nivel__central=central, estado=True, nivel__estado=True, nivel__central__estado=True).values_list('cabecera_cmd', flat=True)
        cabeceras = [c.replace(' ', '_') for c in cabeceras]

        for reg in registros:
            minuto = reg.timestamp_utc.replace(second=0, microsecond=0)
            columna = reg.cabecera_cmd.replace(' ', '_')
            if columna not in cabeceras:
                continue  # Solo compara columnas válidas

            # Consulta el valor en SQL Server
            sql = f"SELECT [{columna}] FROM [{nombre_tabla}] WHERE [timestamp]=?"
            cursor.execute(sql, minuto)
            row = cursor.fetchone()
            valor_sql = row[0] if row else None

            valor_django = reg.valor
            try:
                valor_sql_float = float(str(valor_sql).replace(',', '.')) if valor_sql is not None else None
            except Exception:
                valor_sql_float = None

            diferencia = None
            iguales = False
            if valor_sql_float is None and valor_django is None:
                iguales = True
            elif valor_sql_float is not None and valor_django is not None:
                diferencia = abs(valor_sql_float - valor_django)
                iguales = diferencia <= 0.005

            if not iguales:
                logging.info(
                    f"Diferencia en {nombre_tabla} - timestamp: {minuto}, columna: {columna}, "
                    f"Django: {valor_django}, SQLServer: {valor_sql_float}, Diferencia: {diferencia}"
                )

    cursor.close()
    conn.close()


def ejecutar_etl_secuencial():
    """
    Ejecuta secuencialmente las etapas del ETL por día.
    Solo se ejecuta si existe un registro activo en ETLProcessState.
    Si ya se está ejecutando, no hace nada.
    Registra logs de inicio y fin de cada ejecución diaria.
    """
    with transaction.atomic():
        try:
            estado = ETLProcessState.objects.select_for_update().get(completado=False)
        except ETLProcessState.DoesNotExist:
            # No hay proceso activo, no ejecutar nada
            return
        if estado.en_ejecucion or estado.completado:
            # Ya se está ejecutando o ya terminó
            return
        estado.en_ejecucion = True
        estado.save()

    log = ETLProcessLog.objects.create(
        fecha=estado.dia_actual,
        etapa=estado.etapa,
        mensaje="Inicio de ejecución"
    )

    try:
        etapas = [
            ('importar', importar_valores_scada_desde_sqlserver2),
            ('completar', completar_minutos_faltantes_scadatemporal2),
            ('exportar', exportar_scadatemporal_a_sqlserver),
        ]
        etapa_idx = [e[0] for e in etapas].index(estado.etapa)
        funcion = etapas[etapa_idx][1]

        fecha_inicio = datetime.combine(estado.dia_actual, datetime.min.time())
        fecha_fin = fecha_inicio + timedelta(days=1) - timedelta(seconds=1)
        funcion(fecha_inicio, fecha_fin)

        # Avanzar al siguiente día o etapa
        if estado.dia_actual < estado.fecha_fin:
            estado.dia_actual += timedelta(days=1)
        else:
            if etapa_idx < len(etapas) - 1:
                estado.etapa = etapas[etapa_idx + 1][0]
                estado.dia_actual = estado.fecha_inicio
            else:
                estado.completado = True  # Proceso terminado

        log.exito = True
        log.mensaje = "Ejecución finalizada correctamente"
    except Exception as e:
        log.exito = False
        log.mensaje = f"Error: {str(e)}"
        raise
    finally:
        log.fin = datetime.now()
        log.save()
        # Liberar el flag de ejecución
        estado.en_ejecucion = False
        estado.save()


def acceso_modulo_requerido(nombre_modulo):
    """
    Decorador para validar acceso a un módulo según el campo booleano en el profile del usuario.
    Ejemplo de uso:
        @acceso_modulo_requerido('acceso_usuarios')
        def usuarios_list(request):
            ...
    """
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped_view(request, *args, **kwargs):
            if hasattr(request.user, 'profile') and getattr(request.user.profile, nombre_modulo, False):
                return view_func(request, *args, **kwargs)
            from django.shortcuts import redirect
            return redirect('acceso_denegado')
        return _wrapped_view
    return decorator


def importar_excel_a_cmd(ruta_archivo):
    """
    Recibe un archivo Excel con columnas: ID_scada, valor, timestamp.
    Guarda cada registro en la tabla CMD correspondiente según el parámetro ID_scada,
    en la columna de cabecera adecuada y con el timestamp.
    """
    df = pd.read_excel(ruta_archivo)
    if not all(col in df.columns for col in ['ID_scada', 'valor', 'timestamp']):
        raise ValueError("El archivo debe tener las columnas: ID_scada, valor, timestamp.")

    server = settings.DB_SQL_SERVER
    database = settings.DB_SQL_DATABASE
    username = settings.DB_SQL_USERNAME
    password = settings.DB_SQL_PASSWORD
    
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=Yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        id_scada = row['ID_scada']
        valor = row['valor']
        timestamp = row['timestamp']
        homologacion = Homologacion.objects.filter(id_scada=id_scada).first()
        if not homologacion or not hasattr(homologacion, 'tabla_cmd'):
            continue
        tabla_cmd = homologacion.tabla_cmd
        cabecera = homologacion.cabecera_cmd.replace(' ', '_')
        # Inserta el valor en la columna de cabecera adecuada, con el timestamp
        # Si ya existe ese timestamp, actualiza; si no, inserta nuevo
        cursor.execute(f"SELECT COUNT(*) FROM [{tabla_cmd}] WHERE [timestamp]=?", timestamp)
        existe = cursor.fetchone()[0] > 0
        if existe:
            sql = f"UPDATE [{tabla_cmd}] SET [{cabecera}]=? WHERE [timestamp]=?"
            try:
                cursor.execute(sql, valor, timestamp)
            except Exception as e:
                print(f"Error actualizando en {tabla_cmd}: {e}")
        else:
            sql = f"INSERT INTO [{tabla_cmd}] ([timestamp], [{cabecera}]) VALUES (?, ?)"
            try:
                cursor.execute(sql, timestamp, valor)
            except Exception as e:
                print(f"Error insertando en {tabla_cmd}: {e}")

    conn.commit()
    cursor.close()
    conn.close()


def ejecutar_etl_secuencial_cron():
    """
    Ejecuta secuencialmente las etapas del ETL usando las tablas ETLProcessStateCron y ETLProcessLogCron.
    La fecha/hora de inicio se obtiene del parámetro menos 15 minutos,
    la fecha/hora de fin es el mismo valor más 15 minutos.
    Si concluye correctamente, actualiza la tabla parámetro con la nueva fecha_base.
    Solo ejecuta si la fecha_base es menor que la fecha/hora actual por al menos 15 minutos.
    Almacena en ETLProcessStateCron la cantidad de registros exportados por exportar_scadatemporal_a_sqlserver.
    Guarda un registro en ETLProcessLogCron por cada etapa.
    """
    try:
        fecha_base = Parametro.objects.get(pk=2).valor
        if isinstance(fecha_base, str):
            from django.utils.dateparse import parse_datetime
            fecha_base = parse_datetime(fecha_base)
    except (Parametro.DoesNotExist, ValueError, TypeError):
        print("No se pudo obtener la fecha/hora base del parámetro.")
        return
    if timezone.is_naive(fecha_base):
        fecha_base = timezone.make_aware(fecha_base, timezone.get_current_timezone())
        
    ahora = timezone.now()
    if fecha_base > ahora - timedelta(minutes=15):
        print("No se ejecuta porque la fecha_base no es suficientemente antigua.")
        return

    fecha_inicio = fecha_base - timedelta(minutes=15)
    fecha_fin = fecha_base + timedelta(minutes=15)

    with transaction.atomic():
        estado = ETLProcessStateCron.objects.create(
            fecha_hora_inicio=fecha_inicio,
            fecha_hora_fin=fecha_fin,
            dia=fecha_base.date(),
            en_ejecucion=True,
            completado=False
        )

    # ETAPA 1: importar
    log_importar = ETLProcessLogCron.objects.create(
        fecha_hora=fecha_inicio,
        etapa='importar',
        mensaje="Inicio de etapa importar",
        proceso=estado
    )
    try:
        importar_valores_scada_desde_sqlserver2(fecha_inicio, fecha_fin)
        log_importar.exito = True
        log_importar.mensaje = "Etapa importar finalizada correctamente"
    except Exception as e:
        log_importar.exito = False
        log_importar.mensaje = f"Error en importar: {str(e)}"
        estado.en_ejecucion = False
        log_importar.fin = datetime.now()
        log_importar.save()
        estado.save()
        return
    log_importar.fin = datetime.now()
    log_importar.save()

    # ETAPA 2: completar
    log_completar = ETLProcessLogCron.objects.create(
        fecha_hora=fecha_inicio,
        etapa='completar',
        mensaje="Inicio de etapa completar",
        proceso=estado
    )
    try:
        completar_minutos_faltantes_scadatemporal3(fecha_inicio, fecha_fin)
        log_completar.exito = True
        log_completar.mensaje = "Etapa completar finalizada correctamente"
    except Exception as e:
        log_completar.exito = False
        log_completar.mensaje = f"Error en completar: {str(e)}"
        estado.en_ejecucion = False
        log_completar.fin = datetime.now()
        log_completar.save()
        estado.save()
        return
    log_completar.fin = datetime.now()
    log_completar.save()

    # ETAPA 3: exportar
    log_exportar = ETLProcessLogCron.objects.create(
        fecha_hora=fecha_inicio,
        etapa='exportar',
        mensaje="Inicio de etapa exportar",
        proceso=estado
    )
    try:
        registros_exportados = exportar_scadatemporal_a_sqlserver(fecha_inicio, fecha_fin)
        estado.registros = registros_exportados
        estado.completado = True
        log_exportar.exito = True
        log_exportar.mensaje = f"Etapa exportar finalizada correctamente. Registros exportados: {registros_exportados}"
        # Actualiza la tabla parámetro con la nueva fecha_base (fecha_base + 15 minutos)
        nuevo_valor = fecha_base + timedelta(minutes=15)
        Parametro.objects.filter(pk=2).update(valor=nuevo_valor)
    except Exception as e:
        log_exportar.exito = False
        log_exportar.mensaje = f"Error en exportar: {str(e)}"
        estado.en_ejecucion = False
        log_exportar.fin = datetime.now()
        log_exportar.save()
        estado.save()
        return
    log_exportar.fin = datetime.now()
    log_exportar.save()

    estado.en_ejecucion


def completar_minutos_faltantes_scadatemporal3(fecha_inicio, fecha_fin):
    """
    Interpola minutos faltantes en memoria y usa bulk_create.
    Solo interpola si la diferencia entre el dato previo y el siguiente es de 15 minutos o menos.
    Si no encuentra ambos extremos o la diferencia es mayor, no interpola.
    """
    ids = ScadaTemporal.objects.filter(
        timestamp__range=(fecha_inicio, fecha_fin)
    ).values_list('id_scada', flat=True).distinct()

    for id_scada in ids:
        # Buscar registros en el rango extendido para los extremos
        rango_extendido_inicio = fecha_inicio - timedelta(days=2)
        rango_extendido_fin = fecha_fin + timedelta(days=2)
        registros_ext = list(
            ScadaTemporal.objects.filter(
                id_scada=id_scada,
                timestamp__range=(rango_extendido_inicio, rango_extendido_fin)
            ).order_by('timestamp')
        )
        if not registros_ext:
            continue

        minutos_existentes = [r.timestamp.replace(second=0, microsecond=0) for r in registros_ext]
        registros_dict = {r.timestamp.replace(second=0, microsecond=0): r for r in registros_ext}

        t_actual = fecha_inicio.replace(second=0, microsecond=0)
        t_final = fecha_fin.replace(second=0, microsecond=0)
        from django.utils import timezone
        if timezone.is_naive(t_actual):
            t_actual = timezone.make_aware(t_actual, timezone.get_current_timezone())
        if timezone.is_naive(t_final):
            t_final = timezone.make_aware(t_final, timezone.get_current_timezone())

        nuevos = []
        while t_actual <= t_final:
            if t_actual not in registros_dict:
                idx = bisect_left(minutos_existentes, t_actual)
                prev = None
                next_ = None
                if 0 < idx < len(minutos_existentes):
                    prev = registros_dict[minutos_existentes[idx - 1]]
                    next_ = registros_dict[minutos_existentes[idx]]
                elif idx == 0 and len(minutos_existentes) > 1:
                    next_ = registros_dict[minutos_existentes[0]]
                    prevs = ScadaTemporal.objects.filter(
                        id_scada=id_scada,
                        timestamp__lt=minutos_existentes[0],
                        timestamp__gte=rango_extendido_inicio
                    ).order_by('-timestamp')
                    if prevs.exists():
                        prev = prevs.first()
                elif idx == len(minutos_existentes):
                    prev = registros_dict[minutos_existentes[-1]]
                    nexts = ScadaTemporal.objects.filter(
                        id_scada=id_scada,
                        timestamp__gt=minutos_existentes[-1],
                        timestamp__lte=rango_extendido_fin
                    ).order_by('timestamp')
                    if nexts.exists():
                        next_ = nexts.first()

                # Solo interpola si ambos extremos existen y la diferencia es <= 15 minutos
                if prev and next_:
                    diferencia_minutos = abs(int((next_.timestamp - prev.timestamp).total_seconds() // 60))
                    if diferencia_minutos <= 15:
                        total_secs = (next_.timestamp - prev.timestamp).total_seconds()
                        if total_secs == 0:
                            valor_interp = prev.valor
                        else:
                            secs_to_t = (t_actual - prev.timestamp).total_seconds()
                            valor_interp = prev.valor + (next_.valor - prev.valor) * (secs_to_t / total_secs)
                        nuevos.append(
                            ScadaTemporal(
                                id_scada=id_scada,
                                cabecera_cmd=prev.cabecera_cmd,
                                valor=valor_interp,
                                timestamp=t_actual,
                                timestamp_utc=t_actual - timedelta(hours=5),
                                nivel=prev.nivel,
                                tipo='2'
                            )
                        )
                # Si no hay ambos extremos o la diferencia es mayor, no interpola
            t_actual += timedelta(minutes=1)

        if nuevos:
            with transaction.atomic():
                ScadaTemporal.objects.bulk_create(nuevos,
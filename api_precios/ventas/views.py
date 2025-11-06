import json
import cx_Oracle
import mysql.connector
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from datetime import datetime, timedelta
from decouple import config

def get_db_connection():
    """Crea y retorna una conexión a la base de datos Oracle"""
    return cx_Oracle.connect(
        user=config('ORACLE_USER'),
        password=config('ORACLE_PASSWORD'),
        dsn=config('ORACLE_DSN'),
    )

def add_cors_headers(response):
    """Añade headers CORS a la respuesta"""
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response["Access-Control-Allow-Headers"] = "Content-Type"
    return response

@require_GET
def get_ventas(request, store_no):
    """
    Endpoint para obtener ventas detalladas
    GET /api/ventas/<store_no>?fecha_inicio=YYYY-MM-DD&fecha_fin=YYYY-MM-DD
    """
    fecha_inicio = request.GET.get('fecha_inicio')
    fecha_fin = request.GET.get('fecha_fin')

    if not fecha_inicio or not fecha_fin:
        return add_cors_headers(JsonResponse(
            {'error': 'Parámetros fecha_inicio y fecha_fin son requeridos'}, 
            status=400
        ))

    try:
        datetime.strptime(fecha_inicio, '%Y-%m-%d')
        datetime.strptime(fecha_fin, '%Y-%m-%d')
    except ValueError:
        return add_cors_headers(JsonResponse(
            {'error': 'Formato de fecha inválido, debe ser YYYY-MM-DD'}, 
            status=400
        ))

    query = """
SELECT
    TO_CHAR(i.INVC_SID) AS INVC_SID,
    TO_CHAR(i.TRACKING_NO) AS TRACKING_NO,
    iv.INVC_TOTAL.AMT AS TOTAL_AMOUNT,
    (SELECT e.EMPL_NAME FROM EMPLOYEE_v e WHERE i.CLERK_ID = e.EMPL_ID) AS EMPL_NAME,
    (SELECT e.RPRO_FULL_NAME FROM EMPLOYEE_v e WHERE i.CLERK_ID = e.EMPL_ID) AS EMPL_FULL_NAME,
    TO_CHAR(st.STORE_NAME) AS TIENDA,
    TO_CHAR(i.CREATED_DATE, 'YYYY-MM-DD') AS CREATED_DATE
FROM INVOICE i
JOIN INVOICE_OV iv ON i.INVC_SID = iv.INVC_SID
JOIN STORE st ON i.STORE_NO = st.STORE_NO AND i.SBS_NO = st.SBS_NO
WHERE i.INVC_TYPE IN (0, 2)
  AND i.PROC_STATUS IN (0, 1)
  AND i.SBS_NO = 1
  AND i.STORE_NO = :store_no
  AND TRUNC(i.CREATED_DATE) >= TO_DATE(:fecha_inicio, 'YYYY-MM-DD')
  AND TRUNC(i.CREATED_DATE) <= TO_DATE(:fecha_fin, 'YYYY-MM-DD')
ORDER BY i.CREATED_DATE
    """

    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute(query, {
                'store_no': int(store_no),
                'fecha_inicio': fecha_inicio,
                'fecha_fin': fecha_fin
            })
            rows = cursor.fetchall()

            data = [{
                'invc_sid': row[0],
                'tracking_no': row[1],
                'total_amount': float(row[2]) if row[2] else 0,
                'empleado': f"{row[3]} ({row[4]})" if row[3] and row[4] else None,
                'tienda': row[5],
                'fecha': row[6]
            } for row in rows]

            return add_cors_headers(JsonResponse(data, safe=False))

    except Exception as e:
        return add_cors_headers(JsonResponse(
            {'error': f'Error en el servidor: {str(e)}'}, 
            status=500
        ))
    finally:
        if connection:
            connection.close()

@require_GET
def comparativo_mensual(request, store_no):
    """
    Endpoint para comparar meses entre agnos
    GET /api/comparativo/<store_no>?mes=7&agnos=2023,2024
    """
    mes = request.GET.get('mes')
    agnos = request.GET.get('agnos')

    if not mes or not agnos:
        return add_cors_headers(JsonResponse(
            {'error': 'Parámetros "mes" y "agnos" son requeridos'}, 
            status=400
        ))

    try:
        mes = int(mes)
        agnos_lista = [int(a) for a in agnos.split(',')]
        if not all(2020 <= a <= 2100 for a in agnos_lista):
            raise ValueError
    except ValueError:
        return add_cors_headers(JsonResponse(
            {'error': 'Formato inválido para mes o agnos'}, 
            status=400
        ))

    query = f"""
SELECT
    EXTRACT(YEAR FROM i.CREATED_DATE) AS agno,
    SUM(iv.INVC_TOTAL.AMT) AS total,
    COUNT(*) AS transacciones,
    TO_CHAR(st.STORE_NAME) AS tienda
FROM INVOICE i
JOIN INVOICE_OV iv ON i.INVC_SID = iv.INVC_SID
JOIN STORE st ON i.STORE_NO = st.STORE_NO AND i.SBS_NO = st.SBS_NO
WHERE i.INVC_TYPE IN (0, 2)
  AND i.PROC_STATUS IN (0, 1)
  AND i.SBS_NO = 1
  AND i.STORE_NO = :store_no
  AND EXTRACT(MONTH FROM i.CREATED_DATE) = :mes
  AND EXTRACT(YEAR FROM i.CREATED_DATE) IN ({','.join(map(str, agnos_lista))})
GROUP BY EXTRACT(YEAR FROM i.CREATED_DATE), st.STORE_NAME
ORDER BY agno
    """

    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute(query, {
                'store_no': int(store_no),
                'mes': mes
            })
            resultados = cursor.fetchall()

            if not resultados:
                return add_cors_headers(JsonResponse(
                    {'error': 'No hay datos para los criterios'}, 
                    status=404
                ))

            response_data = {
                'tienda': resultados[0][3],
                'mes': mes,
                'agnos': {},
                'variacion': None
            }

            for agno, total, transacciones, _ in resultados:
                response_data['agnos'][str(agno)] = {
                    'total': float(total),
                    'transacciones': transacciones
                }

            # Calcular variación si hay exactamente 2 agnos
            if len(agnos_lista) == 2:
                agno1, agno2 = sorted(agnos_lista)
                if str(agno1) in response_data['agnos'] and str(agno2) in response_data['agnos']:
                    total1 = response_data['agnos'][str(agno1)]['total']
                    total2 = response_data['agnos'][str(agno2)]['total']
                    if total1 > 0:
                        response_data['variacion'] = round(((total2 - total1) / total1 * 100), 2)

            return add_cors_headers(JsonResponse(response_data))

    except Exception as e:
        return add_cors_headers(JsonResponse(
            {'error': f'Error en el servidor: {str(e)}'}, 
            status=500
        ))
    finally:
        if connection:
            connection.close()

@require_GET
def comparativo_mensual_hasta_fecha(request, store_no):
    """
    Endpoint para comparar meses entre agnos hasta la fecha actual
    GET /api/comparativo-dias/<store_no>?mes=7&agnos=2023,2024
    """
    mes = request.GET.get('mes')
    agnos = request.GET.get('agnos')

    if not mes or not agnos:
        return add_cors_headers(JsonResponse(
            {'error': 'Parámetros "mes" y "agnos" son requeridos'}, 
            status=400
        ))

    try:
        mes = int(mes)
        agnos_lista = [int(a) for a in agnos.split(',')]
        if not all(2020 <= a <= 2100 for a in agnos_lista):
            raise ValueError
    except ValueError:
        return add_cors_headers(JsonResponse(
            {'error': 'Formato inválido para mes o agnos'}, 
            status=400
        ))

    # Obtener fecha actual
    hoy = datetime.now()
    dia_actual = hoy.day if hoy.month == mes else 31

    # Función para obtener último día del mes
    def ultimo_dia_mes(year, month):
        if month == 2:
            return 29 if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0) else 28
        return 30 if month in [4, 6, 9, 11] else 31

    query = f"""
SELECT
    EXTRACT(YEAR FROM i.CREATED_DATE) AS agno,
    SUM(iv.INVC_TOTAL.AMT) AS total,
    COUNT(*) AS transacciones,
    TO_CHAR(st.STORE_NAME) AS tienda,
    TO_CHAR(MAX(i.CREATED_DATE), 'YYYY-MM-DD') AS ultima_fecha
FROM INVOICE i
JOIN INVOICE_OV iv ON i.INVC_SID = iv.INVC_SID
JOIN STORE st ON i.STORE_NO = st.STORE_NO AND i.SBS_NO = st.SBS_NO
WHERE i.INVC_TYPE IN (0, 2)
  AND i.PROC_STATUS IN (0, 1)
  AND i.SBS_NO = 1
  AND i.STORE_NO = :store_no
  AND EXTRACT(MONTH FROM i.CREATED_DATE) = :mes
  AND EXTRACT(YEAR FROM i.CREATED_DATE) = :agno
  AND EXTRACT(DAY FROM i.CREATED_DATE) <= :dia_maximo
GROUP BY EXTRACT(YEAR FROM i.CREATED_DATE), st.STORE_NAME
ORDER BY agno
    """

    connection = None
    try:
        connection = get_db_connection()
        resultados_totales = []
        
        for agno in agnos_lista:
            dia_max = min(dia_actual, ultimo_dia_mes(agno, mes))
            with connection.cursor() as cursor:
                cursor.execute(query, {
                    'store_no': int(store_no),
                    'mes': mes,
                    'agno': agno,
                    'dia_maximo': dia_max
                })
                resultados = cursor.fetchall()
                resultados_totales.extend(resultados)

        if not resultados_totales:
            return add_cors_headers(JsonResponse(
                {'error': 'No hay datos para los criterios'}, 
                status=404
            ))

        response_data = {
            'tienda': resultados_totales[0][3],
            'mes': mes,
            'dia_comparado': dia_actual,
            'agnos': {},
            'variacion': None,
            'detalle_dias': {}
        }

        for agno, total, transacciones, tienda, ultima_fecha in resultados_totales:
            response_data['agnos'][str(agno)] = {
                'total': float(total),
                'transacciones': transacciones,
                'ultima_fecha': ultima_fecha
            }
            response_data['detalle_dias'][str(agno)] = min(dia_actual, ultimo_dia_mes(agno, mes))

        # Calcular variación si hay exactamente 2 agnos
        if len(agnos_lista) == 2:
            agno1, agno2 = sorted(agnos_lista)
            if str(agno1) in response_data['agnos'] and str(agno2) in response_data['agnos']:
                total1 = response_data['agnos'][str(agno1)]['total']
                total2 = response_data['agnos'][str(agno2)]['total']
                if total1 > 0:
                    response_data['variacion'] = round(((total2 - total1) / total1 * 100), 2)

        return add_cors_headers(JsonResponse(response_data))

    except Exception as e:
        return add_cors_headers(JsonResponse(
            {'error': f'Error en el servidor: {str(e)}'}, 
            status=500
        ))
    finally:
        if connection:
            connection.close()

@require_GET
def ventas_diarias_hasta_hoy(request, store_no):
    """
    Endpoint para obtener ventas diarias acumuladas hasta la fecha actual
    GET /api/ventas-diarias/<store_no>
    """
    # Obtener fecha actual
    hoy = datetime.now().date()
    fecha_inicio = hoy.replace(day=1)  # Primer día del mes actual
    
    query = """
SELECT
    TRUNC(i.CREATED_DATE) AS fecha,
    SUM(iv.INVC_TOTAL.AMT) AS total_dia,
    COUNT(i.INVC_SID) AS cantidad_ventas,
    TO_CHAR(st.STORE_NAME) AS tienda
FROM INVOICE i
JOIN INVOICE_OV iv ON i.INVC_SID = iv.INVC_SID
JOIN STORE st ON i.STORE_NO = st.STORE_NO AND i.SBS_NO = st.SBS_NO
WHERE i.INVC_TYPE IN (0, 2)
  AND i.PROC_STATUS IN (0, 1)
  AND i.SBS_NO = 1
  AND i.STORE_NO = :store_no
  AND TRUNC(i.CREATED_DATE) BETWEEN TO_DATE(:fecha_inicio, 'YYYY-MM-DD') AND TO_DATE(:fecha_fin, 'YYYY-MM-DD')
GROUP BY TRUNC(i.CREATED_DATE), st.STORE_NAME
ORDER BY fecha
    """

    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute(query, {
                'store_no': int(store_no),
                'fecha_inicio': fecha_inicio.strftime('%Y-%m-%d'),
                'fecha_fin': hoy.strftime('%Y-%m-%d')
            })
            rows = cursor.fetchall()

            if not rows:
                return JsonResponse({'error': 'No hay ventas registradas este mes'}, status=404)

            # Procesar datos para acumulado diario
            ventas_acumuladas = 0
            datos_diarios = []
            
            for row in rows:
                fecha = row[0].strftime('%Y-%m-%d')
                ventas_dia = float(row[1]) if row[1] else 0
                ventas_acumuladas += ventas_dia
                
                datos_diarios.append({
                    'fecha': fecha,
                    'ventas_dia': ventas_dia,
                    'ventas_acumuladas': ventas_acumuladas,
                    'cantidad_ventas': row[2],
                    'tienda': row[3]
                })

            response_data = {
                'tienda': rows[0][3],
                'mes_actual': hoy.strftime('%Y-%m'),
                'dia_actual': hoy.day,
                'total_mes_actual': ventas_acumuladas,
                'datos_diarios': datos_diarios,
                'ultima_actualizacion': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            return JsonResponse(response_data)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
    finally:
        if connection:
            connection.close()

@require_GET
def ventas_comparativo_anual_dia(request, store_no):
    """
    Endpoint para obtener ventas diarias del mes actual vs mismo periodo año anterior
    GET /api/ventas-comparativo/<store_no>
    """
    # Obtener fechas actuales
    hoy = datetime.now().date()
    fecha_inicio_actual = hoy.replace(day=1)  # Primer día del mes actual
    
    # Calcular fechas del año pasado (mismo periodo)
    fecha_inicio_pasado = fecha_inicio_actual.replace(year=fecha_inicio_actual.year-1)
    hoy_pasado = min(hoy.replace(year=hoy.year-1), fecha_inicio_pasado.replace(month=fecha_inicio_pasado.month+1, day=1) - timedelta(days=1))

    query = """
SELECT
    TRUNC(i.CREATED_DATE) AS fecha,
    SUM(iv.INVC_TOTAL.AMT) AS total_dia,
    COUNT(i.INVC_SID) AS cantidad_ventas,
    TO_CHAR(st.STORE_NAME) AS tienda,
    EXTRACT(YEAR FROM i.CREATED_DATE) AS año
FROM INVOICE i
JOIN INVOICE_OV iv ON i.INVC_SID = iv.INVC_SID
JOIN STORE st ON i.STORE_NO = st.STORE_NO AND i.SBS_NO = st.SBS_NO
WHERE i.INVC_TYPE IN (0, 2)
  AND i.PROC_STATUS IN (0, 1)
  AND i.SBS_NO = 1
  AND i.STORE_NO = :store_no
  AND (
      (TRUNC(i.CREATED_DATE) BETWEEN TO_DATE(:fecha_inicio_actual, 'YYYY-MM-DD') AND TO_DATE(:fecha_fin_actual, 'YYYY-MM-DD'))
      OR
      (TRUNC(i.CREATED_DATE) BETWEEN TO_DATE(:fecha_inicio_pasado, 'YYYY-MM-DD') AND TO_DATE(:fecha_fin_pasado, 'YYYY-MM-DD'))
  )
GROUP BY TRUNC(i.CREATED_DATE), st.STORE_NAME, EXTRACT(YEAR FROM i.CREATED_DATE)
ORDER BY año, fecha
    """

    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute(query, {
                'store_no': int(store_no),
                'fecha_inicio_actual': fecha_inicio_actual.strftime('%Y-%m-%d'),
                'fecha_fin_actual': hoy.strftime('%Y-%m-%d'),
                'fecha_inicio_pasado': fecha_inicio_pasado.strftime('%Y-%m-%d'),
                'fecha_fin_pasado': hoy_pasado.strftime('%Y-%m-%d')
            })
            rows = cursor.fetchall()

            if not rows:
                return JsonResponse({'error': 'No hay datos para comparar'}, status=404)

            # Procesar datos para ambos años
            datos_actual = []
            datos_pasado = []
            ventas_acum_actual = 0
            ventas_acum_pasado = 0
            tienda_nombre = None

            for row in rows:
                fecha = row[0].strftime('%Y-%m-%d')
                ventas_dia = float(row[1]) if row[1] else 0
                año = int(row[4])
                tienda_nombre = row[3]
                
                if año == hoy.year:
                    ventas_acum_actual += ventas_dia
                    datos_actual.append({
                        'fecha': fecha,
                        'ventas_dia': ventas_dia,
                        'ventas_acumuladas': ventas_acum_actual,
                        'cantidad_ventas': row[2],
                        'dia': row[0].day
                    })
                else:
                    ventas_acum_pasado += ventas_dia
                    datos_pasado.append({
                        'fecha': fecha,
                        'ventas_dia': ventas_dia,
                        'ventas_acumuladas': ventas_acum_pasado,
                        'cantidad_ventas': row[2],
                        'dia': row[0].day
                    })

            # Asegurarse que ambos períodos tengan los mismos días (rellenar con 0 si falta algún día)
            dias_mes_actual = hoy.day
            datos_pasado_completos = []
            
            for dia in range(1, dias_mes_actual + 1):
                # Buscar datos para este día en año pasado
                dato_dia = next((d for d in datos_pasado if d['dia'] == dia), None)
                if dato_dia:
                    datos_pasado_completos.append(dato_dia)
                else:
                    # Rellenar con 0 si no hay datos
                    fecha_pasada = fecha_inicio_pasado.replace(day=dia).strftime('%Y-%m-%d')
                    datos_pasado_completos.append({
                        'fecha': fecha_pasada,
                        'ventas_dia': 0,
                        'ventas_acumuladas': 0,
                        'cantidad_ventas': 0,
                        'dia': dia
                    })

            response_data = {
                'tienda': tienda_nombre,
                'mes_actual': hoy.strftime('%Y-%m'),
                'mes_pasado': fecha_inicio_pasado.strftime('%Y-%m'),
                'dia_actual': hoy.day,
                'total_actual': ventas_acum_actual,
                'total_pasado': ventas_acum_pasado,
                'variacion_porcentual': ((ventas_acum_actual - ventas_acum_pasado) / ventas_acum_pasado * 100) if ventas_acum_pasado != 0 else 0,
                'datos_actual': datos_actual,
                'datos_pasado': datos_pasado_completos,
                'ultima_actualizacion': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            return JsonResponse(response_data)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
    finally:
        if connection:
            connection.close()

@require_GET
def get_detalle_ventas(request, store_no):
    """
    Endpoint para obtener el detalle completo de ventas por SKU agrupado por folio
    GET /api/ventas-detalle/<store_no>?fecha_inicio=YYYY-MM-DD&fecha_fin=YYYY-MM-DD&marca=Marca&limit=100
    """
    # Validación de parámetros (igual que antes)
    fecha_inicio = request.GET.get('fecha_inicio')
    fecha_fin = request.GET.get('fecha_fin') or fecha_inicio
    marca = request.GET.get('marca')
    limit = int(request.GET.get('limit', 100))
    
    if not fecha_inicio:
        return JsonResponse(
            {'success': False, 'error': 'El parámetro fecha_inicio es requerido (YYYY-MM-DD)'}, 
            status=400
        )

    try:
        fecha_inicio_dt = datetime.strptime(fecha_inicio, '%Y-%m-%d')
        fecha_fin_dt = datetime.strptime(fecha_fin, '%Y-%m-%d')
    except ValueError:
        return JsonResponse(
            {'success': False, 'error': 'Formato de fecha inválido, debe ser YYYY-MM-DD'},
            status=400
        )

    # Construcción de la query corregida
    base_query = """
    SELECT * FROM (
        SELECT a.*, ROWNUM rnum FROM (
            WITH ventas_detalle AS (
                SELECT
                    i.TRACKING_NO as folio,
                    TO_CHAR(i.CREATED_DATE, 'DD-MM-YYYY') as fecha,
                    TO_CHAR(st.STORE_CODE) || ' ' || st.store_name AS tienda,
                    SUBSTR(st.store_name,1,3) as marca,
                    TO_CHAR(ud.UDF_VALUE) as tipo_doc,
                    TO_CHAR(i.INVC_SID) as invc_sid,
                    SUM(CASE WHEN i.INVC_TYPE = 2 THEN (det.price * det.qty * -1) ELSE (det.price * det.qty) END) AS total_venta,
                    COUNT(*) as items_count,
                    LISTAGG(
                        '{' ||
                        '"sku":"' || pt.ALU || '",' ||
                        '"marca_producto":"' || pta.AUX_10 || '",' ||
                        '"temperatura":"' || pta.AUX_7 || '",' ||
                        '"costo":' || pt.cost || ',' ||
                        '"cantidad":' || (CASE WHEN i.INVC_TYPE = 2 THEN det.qty * -1 ELSE det.qty END) || ',' ||
                        '"precio_unitario":' || det.price || ',' ||
                        '"total_linea":' || (CASE WHEN i.INVC_TYPE = 2 THEN (det.price * det.qty * -1) ELSE (det.price * det.qty) END) || ',' ||
                        '"notas":{' ||
                            '"item_note5":"' || NVL(det.item_note5, '') || '",' ||
                            '"item_note8":"' || NVL(det.ITEM_NOTE8, '') || '",' ||
                            '"item_note9":"' || NVL(det.ITEM_NOTE9, '') || '",' ||
                            '"item_note10":"' || NVL(det.ITEM_NOTE10, '') || '"}' ||
                        '}', ','
                    ) WITHIN GROUP (ORDER BY pt.ALU) as items_json
                FROM INVOICE i 
                JOIN invc_item det ON det.INVC_SID = i.INVC_SID
                JOIN invn_sbs pt ON pt.ITEM_SID = det.ITEM_SID  
                JOIN V_PROM_FITEM pta ON pta.SID_ARTICULO = pt.ITEM_SID      
                JOIN INVC_SUPPL_V s ON i.INVC_SID = s.INVC_SID 
                JOIN UDF_V u ON u.UDF_ID = s.UDF_ID 
                JOIN UDF_VAL_V ud ON u.UDF_ID = ud.UDF_ID AND s.UDF_VAL_ID = ud.UDF_VAL_ID 
                JOIN STORE st ON i.STORE_NO = st.STORE_NO AND i.SBS_NO = st.SBS_NO 
                WHERE i.SBS_NO IN (1) 
                AND u.UDF_NAME = 'Documento' 
                AND i.INVC_TYPE IN (0,2) 
                AND i.PROC_STATUS IN (0,1) 
                AND TRUNC(i.CREATED_DATE) BETWEEN TO_DATE(:fecha_inicio, 'YYYY-MM-DD') AND TO_DATE(:fecha_fin, 'YYYY-MM-DD')
                AND st.STORE_NO = :store_no
    """

    # Filtros adicionales
    params = {
        'fecha_inicio': fecha_inicio,
        'fecha_fin': fecha_fin,
        'store_no': int(store_no)
    }
    
    if marca:
        base_query += " AND SUBSTR(st.store_name,1,3) = :marca"
        params['marca'] = marca

    # Continuación de la query
    base_query += """
                GROUP BY i.TRACKING_NO, i.CREATED_DATE, st.STORE_CODE, st.store_name, ud.UDF_VALUE, i.INVC_SID, SUBSTR(st.store_name,1,3)
            )
            SELECT * FROM ventas_detalle ORDER BY fecha, folio
        ) a WHERE ROWNUM <= :limit
    ) WHERE rnum > 0
    """

    # Query para contar el total de folios
    count_query = """
    SELECT COUNT(DISTINCT i.TRACKING_NO)
    FROM INVOICE i 
    JOIN STORE st ON i.STORE_NO = st.STORE_NO AND i.SBS_NO = st.SBS_NO 
    JOIN INVC_SUPPL_V s ON i.INVC_SID = s.INVC_SID 
    JOIN UDF_V u ON u.UDF_ID = s.UDF_ID 
    WHERE i.SBS_NO IN (1) 
    AND u.UDF_NAME = 'Documento' 
    AND i.INVC_TYPE IN (0,2) 
    AND i.PROC_STATUS IN (0,1) 
    AND TRUNC(i.CREATED_DATE) BETWEEN TO_DATE(:fecha_inicio, 'YYYY-MM-DD') AND TO_DATE(:fecha_fin, 'YYYY-MM-DD')
    AND st.STORE_NO = :store_no
    """
    
    if marca:
        count_query += " AND SUBSTR(st.store_name,1,3) = :marca"

    connection = None
    try:
        connection = get_db_connection()
        
        # Obtener total de registros (folios distintos)
        with connection.cursor() as cursor:
            cursor.execute(count_query, params)
            total = cursor.fetchone()[0]

        # Obtener datos paginados
        params['limit'] = limit
        with connection.cursor() as cursor:
            cursor.execute(base_query, params)
            columns = [col[0].lower() for col in cursor.description]
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                row_data = dict(zip(columns, row))
                # Convertir el JSON de items a lista de diccionarios
                try:
                    row_data['items'] = json.loads(f'[{row_data["items_json"]}]')
                except:
                    row_data['items'] = []
                del row_data['items_json']
                data.append(row_data)

        return JsonResponse({
            'success': True,
            'data': data,
            'metadata': {
                'total_registros': total,
                'limite': limit,
                'pagina': 1,
                'has_more': total > limit,
                'fecha_inicio': fecha_inicio,
                'fecha_fin': fecha_fin
            }
        })

    except Exception as e:
        return JsonResponse(
            {'success': False, 'error': str(e)},
            status=500
        )
    finally:
        if connection:
            connection.close()

@require_GET
def obtener_metas(request, store_no, year, month):
    """
    Endpoint para obtener las metas de ventas por tienda, año y mes
    GET /api/ventas-metas/<store_no>/<year>/<month>/
    """
    try:
        connection = mysql.connector.connect(
            host=config('MYSQL_HOST'),
            user=config('MYSQL_USER'),
            password=config('MYSQL_PASSWORD'),
            database=config('MYSQL_DATABASE'),
            port=config('MYSQL_PORT', cast=int)
        )  # <-- Aquí faltaba este paréntesis
        
        cursor = connection.cursor(dictionary=True)
        
        # Convertir store_no a string manteniendo los ceros a la izquierda
        store_str = f"{store_no:03d}"  # Esto formatea el número a 3 dígitos con ceros a la izquierda
        
        query = """
            SELECT 
                meta_anio as meta_anio, 
                meta_mes as meta_mes, 
                meta_store as meta_store, 
                SUM(meta_est_civa) AS total_meta_est_civa 
            FROM 
                metas_mensuales 
            WHERE 
                meta_anio = %s 
                AND meta_mes = %s 
                AND meta_store = %s
            GROUP BY meta_anio, meta_mes, meta_store;
        """
        
        cursor.execute(query, (year, month, store_str))
        resultado = cursor.fetchone()
        
        if not resultado:
            return JsonResponse({
                'error': 'No se encontraron metas para los parámetros proporcionados',
                'params': {
                    'year': year,
                    'month': month,
                    'store': store_str
                }
            }, status=404)
            
        # Convertir valores numéricos a enteros
        resultado['meta_anio'] = int(resultado['meta_anio'])
        resultado['meta_mes'] = int(resultado['meta_mes'])
        resultado['total_meta_est_civa'] = float(resultado['total_meta_est_civa'])
        
        return JsonResponse(resultado)
        
    except Exception as e:
        return JsonResponse({
            'error': str(e),
            'type': type(e).__name__
        }, status=500)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

@require_GET
def get_ventas_historicas_proyeccion(request, store_no):
    """
    Endpoint optimizado para proyecciones
    GET /api/proyecciones/<store_no>?fecha_inicio=YYYY-MM-DD&fecha_fin=YYYY-MM-DD
    """
    fecha_inicio = request.GET.get('fecha_inicio')
    fecha_fin = request.GET.get('fecha_fin')

    # Validación de fechas (igual que tu endpoint actual)
    if not fecha_inicio or not fecha_fin:
        return JsonResponse({'error': 'Parámetros fecha_inicio y fecha_fin son requeridos'}, status=400)
    
    try:
        datetime.strptime(fecha_inicio, '%Y-%m-%d')
        datetime.strptime(fecha_fin, '%Y-%m-%d')
    except ValueError:
        return JsonResponse({'error': 'Formato de fecha inválido'}, status=400)

    # Consulta SQL modificada para agrupación diaria
    query = """
SELECT
    TRUNC(i.CREATED_DATE) AS fecha,
    SUM(iv.INVC_TOTAL.AMT) AS venta_total,
    COUNT(DISTINCT i.INVC_SID) AS transacciones,
    COUNT(DISTINCT i.CLERK_ID) AS empleados_activos
FROM INVOICE i
JOIN INVOICE_OV iv ON i.INVC_SID = iv.INVC_SID
WHERE i.INVC_TYPE IN (0, 2)
  AND i.PROC_STATUS IN (0, 1)
  AND i.SBS_NO = 1
  AND i.STORE_NO = :store_no
  AND TRUNC(i.CREATED_DATE) >= TO_DATE(:fecha_inicio, 'YYYY-MM-DD')
  AND TRUNC(i.CREATED_DATE) <= TO_DATE(:fecha_fin, 'YYYY-MM-DD')
GROUP BY TRUNC(i.CREATED_DATE)
ORDER BY fecha
    """

    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute(query, {
                'store_no': int(store_no),
                'fecha_inicio': fecha_inicio,
                'fecha_fin': fecha_fin
            })
            rows = cursor.fetchall()

            # Estructura optimizada para frontend
            data = {
                "tienda": store_no,
                "datos": [{
                    "fecha": row[0].strftime('%Y-%m-%d'),
                    "venta_total": float(row[1]) if row[1] else 0,
                    "transacciones": row[2],
                    "metricas_extra": {
                        "empleados_activos": row[3],
                        "ticket_promedio": round(float(row[1])/row[2], 2) if row[2] > 0 else 0
                    }
                } for row in rows],
                "metadata": {
                    "total_dias": len(rows),
                    "venta_promedio_diaria": round(sum(float(row[1]) for row in rows)/len(rows), 2) if rows else 0
                }
            }

            return add_cors_headers(JsonResponse(data))

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
    finally:
        if connection:
            connection.close()
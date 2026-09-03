# Documento de Evidencias — Proyecto Corte 1

**Materia:** Frameworks y Herramientas para Big Data
**Integrantes:** [Nombres de los integrantes]
**Grupo:** [Número de grupo — pendiente de asignación]
**Fecha:** 01/09/2026

---

## 1. Resumen ejecutivo

Se construyó un data stack completo sobre Docker Compose con ingesta de un
Parquet público de NYC Yellow Taxi (enero 2025), aterrizaje en un data lake
(MinIO), conversión a tabla Apache Iceberg catalogada en Nessie (vía Iceberg
REST) y distribución final hacia Azure ADLS y ClickHouse. Toda la ingesta y
carga se realiza con `dlt`; la lectura de Parquet se hace con PyArrow en lotes.

**Resultado de la validación crítica:** la tabla final en ClickHouse contiene
exactamente **3.475.226 filas**, verificado en tres capas: RAW (MinIO), tabla
Iceberg (vía Nessie) y ClickHouse.

## 2. Arquitectura

| Función | Tecnología | Servicio Docker |
|---|---|---|
| Data Lake / Object Storage | MinIO | `minio` |
| Catálogo | Nessie (Iceberg REST) | `nessie` |
| Data Warehouse | ClickHouse | `clickhouse` |
| Entorno interactivo | Jupyter | `jupyter` |
| File format | Apache Parquet | — |
| Table format | Apache Iceberg | — |
| Ingesta / carga | `dlt` | — |
| Lectura de Parquet | PyArrow | — |

```
  Fuente HTTP (NYC yellow taxi, 3.475.226 filas)
        │  01_http_to_minio.py        · dlt · PyArrow batches
        ▼
  MinIO · bucket nyc-taxi-raw  (Parquet)
        │  02_parquet_to_iceberg.py   · PyArrow + PyIceberg
        ▼
  MinIO · bucket nyc-taxi-iceberg  (Iceberg: data + metadata)
  Nessie · namespace nyc_taxi / tabla yellow_tripdata_2025_01
        │
        ├── 03_minio_to_azure.py      · dlt ──▶ Azure ADLS (carpeta del grupo)
        └── 04_minio_to_clickhouse.py · dlt ──▶ ClickHouse (count = 3.475.226)
```

## 3. Configuración

- **Secretos:** las credenciales de Azure, MinIO y ClickHouse viven en
  `dlt/secrets.toml` (excluido de control de versiones). Ningún script contiene
  credenciales hardcodeadas.
- **Entorno:** `.env` con los parámetros no secretos. `GROUP_FOLDER=GRUPO_2`
  (grupo 2 confirmado).
- **Decisiones técnicas relevantes:**
  - Nessie se usa como catálogo Iceberg REST (`/iceberg`), no como tipo de
    catálogo `nessie` (no existe en PyIceberg).
  - Version store de Nessie en RocksDB con volumen persistente (el catálogo
    sobrevive a reinicios).
  - La fuente nunca se carga entera en memoria: `ParquetFile.iter_batches()`.
  - ClickHouse nativo se publica en el puerto 9002 del host porque MinIO ocupa
    el 9000.
  - Separador de nombres en ClickHouse: `_` (tabla final
    `nyc_taxi_yellow_tripdata_2025_01`).

## 4. Ejecución de los pipelines

Todos los scripts corren dentro del contenedor `jupyter`
(`docker compose exec jupyter python scripts/<script>.py`).

### 4.1. Script 01 — HTTP → MinIO RAW

- Fuente: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet`
- Destino: `s3://nyc-taxi-raw/raw/yellow_tripdata/`
- Resultado: 1 archivo Parquet de 72,9 MB
  (`1788272855.525187.7eef0a56d0.parquet`).

```
Rows landed in MinIO: 3,475,226
[OK] expected rows 3,475,226 == 3,475,226
```

### 4.2. Script 02 — MinIO RAW → Iceberg + Nessie

- Tabla creada: `nyc_taxi.yellow_tripdata_2025_01` (20 columnas)
- Location: `s3://nyc-taxi-iceberg/nyc_taxi/yellow_tripdata_2025_01_25fa2203-bb5d-49f6-8162-bc570ef08446`
- Snapshots: 1 · Data files: 14 · Metadata files: 43
- Carga por lotes de 250.000 filas con `iter_batches` + append.

```
[OK] Iceberg row count matches - 3,475,226 (expected 3,475,226)
```

### 4.3. Script 03 — Iceberg → Azure ADLS ⚠️

- Lectura: los data files se resuelven preguntando al catálogo de Nessie.
- Destino: `abfss://clase-4-dlt@fhbd.dfs.core.windows.net/GRUPO_2`
- **Estado: BLOQUEADO por Azure.** La cuenta de almacenamiento `fhbd`
  responde `ErrorCode: AccountIsDisabled`. La credencial es válida (una key
  inválida devolvería `AuthenticationFailed`); el error es del lado del
  servicio, no del pipeline:

```
<Error>
  <Code>AccountIsDisabled</Code>
  <Message>The specified account is disabled.</Message>
</Error>
```

- **Acción requerida:** el profesor suministrará nuevos secrets de Azure
  (cuenta/key). Cuando estén disponibles, se actualiza `dlt/secrets.toml` y el
  script se ejecuta sin cambios.

### 4.4. Script 04 — Iceberg → ClickHouse

- Tabla final: `default.nyc_taxi_yellow_tripdata_2025_01`
- Tablas de metadatos de dlt creadas: `nyc_taxi__dlt_loads`,
  `nyc_taxi__dlt_pipeline_state`, `nyc_taxi__dlt_version`,
  `nyc_taxi_dlt_sentinel_table`.
- Carga completa en 3,02 s.

```
[OK] ClickHouse row count matches - 3,475,226 (expected 3,475,226)
```

## 5. Verificación integral (`verificar.py`)

Reporte de evidencias en una sola corrida: **11/12 checks pasados**. El único
check fallido es el de alcance de Azure (cuenta deshabilitada, ver §4.3).

```
[OK ] bucket 'nyc-taxi-raw' exists
[OK ] bucket 'nyc-taxi-iceberg' exists
[OK ] RAW bucket has Parquet files - 1 file(s)
[OK ] Iceberg bucket has data files - 14 file(s)
[OK ] Iceberg bucket has metadata files - 43 file(s)
[OK ] namespace 'nyc_taxi' exists
[OK ] table 'nyc_taxi.yellow_tripdata_2025_01' registered
[OK ] Iceberg row count matches - 3,475,226 (expected 3,475,226)
[OK ] table 'nyc_taxi_yellow_tripdata_2025_01' exists
[OK ] dlt metadata tables present
[OK ] ClickHouse row count matches - 3,475,226 (expected 3,475,226)
[FAIL] Azure reachable - The specified account is disabled. (AccountIsDisabled)
```

## 6. Evidencias visuales

Capturas almacenadas en la carpeta `evidencias/` del proyecto:

| # | Archivo | Contenido |
|---|---|---|
| 1 | `01_docker_services.png` | Servicios del stack corriendo (`docker compose ps`) |
| 2 | `02_minio_raw_bucket.png` | Bucket `nyc-taxi-raw` con el Parquet de 72,9 MB |
| 3 | `03_minio_iceberg_bucket.png` | Bucket `nyc-taxi-iceberg` con `data/` y `metadata/` |
| 3b | `03b_minio_iceberg_metadata.png` | Archivos `.metadata.json` de la tabla Iceberg |
| 4 | `04_nessie_namespace.png` | Namespace `nyc_taxi` y tabla en Nessie |
| 5 | `05_clickhouse_count.png` | `SELECT count(*)` = 3475226 |
| 6 | `06_clickhouse_metadata.png` | `SHOW TABLES` con tablas de metadatos de dlt |
| 7 | `07_azure_file.png` | **Pendiente** — requiere cuenta Azure habilitada |

## 7. Incidentes y soluciones

| Incidente | Causa | Solución |
|---|---|---|
| Nessie salía con `Permission denied` en `/nessie/data/LOG` | El volumen nombrado quedó con propietario root; la imagen corre como usuario `nessie` (uid 10000) | `chown -R 10000:10001` sobre el volumen ejecutado con `--user 0` |
| `PermissionError` en `/home/jovyan/state/pipelines` | Volumen `jupyter-state` con propietario root; el contenedor corre como `jovyan` (uid 1000) | `chown -R 1000:1000` sobre el volumen |
| dlt rechazaba `secrets.toml` (`Empty key at line 1 col 0`) | El archivo se generó en UTF-8 con BOM (PowerShell 5.1); el parser TOML no acepta BOM | Regenerar el archivo en UTF-8 sin BOM |
| `docker compose up` fallaba con `logon session does not exist` | El CLI de Docker en sesión SSH no accede al credential store de la sesión interactiva de Windows | Ejecutar el `up` como tarea programada en la sesión interactiva del usuario |
| Consola de MinIO: modal de licencia y navegación | UI nueva (`/browser/<bucket>`); el modal AGPL intercepta el primer clic | Aceptar el modal y navegar por clic en las filas |

## 8. Estado y pendientes

| Ítem | Estado |
|---|---|
| Stack Docker (4 servicios) | ✅ Operativo |
| Scripts 01, 02, 04 | ✅ Ejecutados y verificados |
| Validación de 3.475.226 filas | ✅ Exacta en RAW, Iceberg y ClickHouse |
| Script 03 (Azure ADLS) | ⚠️ Pendiente: el profesor entregará nuevos secrets de Azure |
| Evidencia 07 (Azure) | ⚠️ Pendiente del punto anterior |
| Número de grupo | ✅ Grupo 2 (`GRUPO_2`) |

## 9. Conclusiones

El stack cumple el objetivo extremo a extremo para las capas locales: el dato
público se ingiere con `dlt`, se persiste como Parquet en MinIO, se promueve a
tabla Iceberg con catálogo Nessie y se carga a ClickHouse con la validación
crítica de **3.475.226 filas exactas**. La única pieza pendiente (Azure ADLS)
depende de una acción externa: la habilitación de la cuenta de almacenamiento
del curso.

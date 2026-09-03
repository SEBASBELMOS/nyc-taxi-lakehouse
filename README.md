# Proyecto Corte 1 — Frameworks y Herramientas para Big Data

Data stack completo sobre Docker: ingesta de un Parquet público, aterrizaje en
un data lake, conversión a tabla Iceberg catalogada en Nessie, y distribución
final a Azure ADLS y a ClickHouse. Toda la ingesta y carga se hace con `dlt`.

## Stack

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

## Flujo

```
  Fuente HTTP (NYC yellow taxi, 3.475.226 filas)
        │
        │  01_http_to_minio.py        · dlt · PyArrow batches
        ▼
  MinIO · bucket nyc-taxi-raw  (Parquet)
        │
        │  02_parquet_to_iceberg.py   · PyArrow + PyIceberg
        ▼
  MinIO · bucket nyc-taxi-iceberg  (Iceberg: data + metadata)
  Nessie · namespace nyc_taxi / tabla yellow_tripdata_2025_01
        │
        ├── 03_minio_to_azure.py      · dlt ──▶ Azure ADLS (carpeta del grupo)
        │
        └── 04_minio_to_clickhouse.py · dlt ──▶ ClickHouse (count = 3.475.226)
```

## Requisitos

- Docker Desktop con Compose v2 (`docker compose version`).
- ~8 GB de RAM disponibles para Docker y ~15 GB de disco.
- Acceso de red a la fuente HTTP y a Azure.

## Puesta en marcha

Todo corre dentro de Docker. Los contenedores se hablan por **nombre de
servicio**, así que el stack es portable y no depende de la IP de la máquina.

```bash
# 1. Configuración (valores no secretos)
cp env.example .env

# 2. Credenciales de dlt (secretos reales)
cp dlt/secrets.example.toml dlt/secrets.toml
#    Editar dlt/secrets.toml y completar:
#      - azure_storage_account_key  (la que entrega el profesor)
#      - bucket_url                 (reemplazar GRUPO_X por la carpeta del grupo)

# 3. Levantar el stack (la primera vez construye la imagen de Jupyter)
docker compose up -d --build

# 4. Verificar
docker compose ps
```

En Windows los mismos comandos funcionan en PowerShell; usar `copy` en lugar de
`cp`.

### Puertos

| Servicio | Host | Interno | URL |
|---|---:|---:|---|
| MinIO API (S3) | 9000 | 9000 | `http://minio:9000` |
| MinIO Consola | 9001 | 9001 | `http://<host>:9001` |
| Nessie | 19120 | 19120 | `http://nessie:19120/iceberg` |
| ClickHouse HTTP | 8123 | 8123 | `http://clickhouse:8123` |
| ClickHouse nativo | **9002** | 9000 | — |
| Jupyter | 8888 | 8888 | `http://<host>:8888` |

> **ClickHouse nativo se publica en 9002, no en 9000.** MinIO ya ocupa el 9000
> del host. Dentro de la red Docker ClickHouse sigue escuchando en 9000, que es
> lo que usa `dlt`.

### Acceder desde otra máquina de la LAN

Si Docker corre en una máquina y vos mirás desde otra, reemplazá `<host>` por la
IP LAN de la máquina que corre Docker:

```
http://192.168.1.111:9001   ← consola de MinIO
http://192.168.1.111:8888   ← Jupyter (token: el de JUPYTER_TOKEN)
http://192.168.1.111:19120  ← API de Nessie
```

En Windows, el Firewall bloquea estos puertos por defecto para la red privada.
Hay que permitirlos o no vas a poder entrar desde la otra máquina. La IP puede
cambiar por DHCP: no la escribas en ningún archivo del proyecto.

## Ejecutar los pipelines

Los scripts corren **dentro del contenedor de Jupyter**, que es donde están las
dependencias y desde donde se resuelven los nombres de servicio.

```bash
docker compose exec jupyter python scripts/01_http_to_minio.py
docker compose exec jupyter python scripts/02_parquet_to_iceberg.py
docker compose exec jupyter python scripts/03_minio_to_azure.py
docker compose exec jupyter python scripts/04_minio_to_clickhouse.py

# Reporte de evidencias completo
docker compose exec jupyter python scripts/verificar.py
```

Cada script imprime su propia verificación al final y devuelve código de salida
distinto de cero si algo no cuadra. Los cuatro son **idempotentes**: se pueden
re-ejecutar sin duplicar datos.

### Qué hace cada uno

| Script | Entrada | Salida | Notas |
|---|---|---|---|
| `01_http_to_minio.py` | URL HTTP | `s3://nyc-taxi-raw/raw/yellow_tripdata/` | Descarga a disco y entrega a dlt en lotes de PyArrow |
| `02_parquet_to_iceberg.py` | RAW en MinIO | Tabla Iceberg + Nessie | Lee con PyArrow, escribe con PyIceberg |
| `03_minio_to_azure.py` | Data files de Iceberg | `abfss://.../<grupo>` | Verifica el resultado con `adlfs` |
| `04_minio_to_clickhouse.py` | Data files de Iceberg | Tabla ClickHouse | Valida `count(*) = 3.475.226` |

Los scripts 03 y 04 **preguntan al catálogo de Nessie** dónde están los data
files en vez de asumir una ruta. Si cambia el nombre de la tabla o el layout del
warehouse, siguen funcionando.

## Decisiones técnicas

**Nessie se usa como catálogo Iceberg REST, no como "catálogo Nessie".**
PyIceberg no tiene un tipo de catálogo `nessie`. Nessie expone el protocolo
Iceberg REST en `/iceberg`, así que el cliente se configura con `type="rest"`.
La propiedad `prefix` **no** se debe usar: la documentación de Nessie indica que
no funciona con PyIceberg; la rama se elige por la ruta de la URI.

**El version store de Nessie es RocksDB con volumen, no el default.**
Por defecto Nessie usa `IN_MEMORY`: al reiniciar el contenedor se pierden el
namespace y la tabla. Con `ROCKSDB` sobre un volumen, el catálogo sobrevive.

**El archivo fuente nunca se carga entero en memoria.**
Son ~3,5 millones de filas. Leerlo completo a un DataFrame de pandas se dispara
a varios GB dentro del contenedor. Los scripts usan
`ParquetFile.iter_batches()` y entregan lotes de PyArrow, lo que mantiene la
memoria plana y permite a dlt escribir Parquet directo.

**La imagen de Jupyter es `minimal-notebook`, no la de PySpark.**
No hay Spark en este stack. La variante PySpark suma varios GB sin aportar nada.

**Los nombres de tabla en ClickHouse usan `_` como separador.**
Por defecto dlt genera `<dataset>___<tabla>` (tres guiones bajos). En
`dlt/config.toml` se fija `dataset_table_separator = "_"`, así la tabla final es
`nyc_taxi_yellow_tripdata_2025_01`.

## Evidencias para la entrega

`scripts/verificar.py` produce en una sola corrida todo lo que pide el
enunciado. Capturas a tomar:

| # | Evidencia | Dónde obtenerla |
|---|---|---|
| 1 | Servicios corriendo en Docker | `docker compose ps` |
| 2 | Bucket RAW con su Parquet | Consola MinIO → `nyc-taxi-raw` |
| 3 | Bucket Iceberg con data + metadata | Consola MinIO → `nyc-taxi-iceberg` |
| 4 | Catálogo Nessie con el namespace | Salida de `verificar.py` |
| 5 | `count(*)` = 3475226 en ClickHouse | Salida de `04` o `verificar.py` |
| 6 | Tablas de metadatos de dlt | `SHOW TABLES FROM default;` |
| 7 | Archivo cargado en Azure | Salida de `03` (listado vía `adlfs`) |

Consultas directas a ClickHouse:

```sql
SHOW TABLES FROM default;
SELECT count(*) FROM default.nyc_taxi_yellow_tripdata_2025_01;
SELECT * FROM default.nyc_taxi__dlt_loads;
```

```bash
docker compose exec clickhouse clickhouse-client --password clickhouse123 \
  --query "SELECT count(*) FROM default.nyc_taxi_yellow_tripdata_2025_01"
```

## Problemas frecuentes

**`Invalid pattern: '**' can only be an entire path component`**
Patrón de glob mal formado. En fsspec, `**` debe ser un componente completo:
`**/*.parquet` es válido, `**.parquet` no.

**El namespace de Nessie aparece vacío tras reiniciar**
Revisar que `nessie.version.store.type` sea `ROCKSDB` y que el volumen
`nessie-data` esté montado. Con el default `IN_MEMORY` el catálogo se vacía.

**`403` o `SignatureDoesNotMatch` contra MinIO**
Las credenciales de `.env` y de `dlt/secrets.toml` tienen que coincidir. MinIO
además exige `path-style-access`, ya configurado en Nessie y en los scripts.

**El puerto 9000 está ocupado**
MinIO usa 9000 en el host y ClickHouse nativo se publicó en 9002 justamente por
eso. Si hay otro stack levantado (por ejemplo el de otra semana), bajarlo con
`docker compose down` en su carpeta.

**`03_minio_to_azure.py` falla con "GRUPO_X"**
Falta reemplazar el placeholder por la carpeta real del grupo, en `.env`
(`GROUP_FOLDER`) y en `dlt/secrets.toml` (`bucket_url`).

**El count no da 3.475.226**
Los scripts usan `write_disposition="replace"` y el 02 recrea la tabla, así que
re-ejecutar no duplica. Si el número es mayor, es que quedaron data files viejos
en el bucket Iceberg: vaciarlo y correr 02 de nuevo.

## Estructura

```
Proyecto-Corte-1/
├── docker-compose.yml          # los 4 servicios
├── Dockerfile                  # imagen de Jupyter con las dependencias
├── requirements.txt
├── env.example                 # copiar a .env
├── dlt/
│   ├── config.toml             # configuración no secreta (versionable)
│   └── secrets.example.toml    # copiar a secrets.toml (NO versionar)
├── scripts/
│   ├── config.py               # configuración compartida y helpers
│   ├── 01_http_to_minio.py
│   ├── 02_parquet_to_iceberg.py
│   ├── 03_minio_to_azure.py
│   ├── 04_minio_to_clickhouse.py
│   └── verificar.py            # reporte de evidencias
├── notebooks/
└── evidencias/                 # capturas para el documento
```

## Seguridad

`dlt/secrets.toml` y `.env` están en `.gitignore` y no deben commitearse. Los
scripts no contienen ninguna credencial: todo sale de variables de entorno o de
`secrets.toml`.

# Deber Data Mining — QBO Historical Backfill
**Emilio Soria - 00326990**

## Descripción del proyecto
Este proyecto implementa un pipeline de backfill histórico que extrae información de QuickBooks Online (QBO) para las entidades:

- **Customers**
- **Invoices**
- **Items**

y la deposita en PostgreSQL dentro de un esquema raw.

**Componentes**
- **Orquestación:** Mage
- **Despliegue:** Docker Compose
- **Seguridad:** Mage Secrets (credenciales/tokens)
- **Alcance:** solo backfill histórico (no incluye pipelines diarios, capa clean ni modelo dimensional)

## Arquitectura
### Diagrama
<img src="https://github.com/user-attachments/assets/0ef948ee-6898-4b7c-828e-b53c7b6f6289" width="300">


### Notas de red Docker
- Los contenedores se comunican por **nombre de servicio** (por ejemplo: `warehouse`), no por nombre de contenedor.

## Estructura del repositorio y evidencias

Estructura esperada del proyecto:

```
.
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── README.md
├── scheduler_data/              # Código Mage: pipelines y blocks
├── warehouse_ui_data/           # Persistencia pgAdmin
├── evidencias/                  # Evidencias solicitadas
│   ├── customers/
│   ├── invoices/
│   └── items/
└── .gitignore
```

## Pasos para levantar contenedores y configurar el proyecto

### Clonar repositorio
```
git clone https://github.com/ImSoriex/DM_Deber1_EmilioSoria
cd DM_Deber1_EmilioSoria
```

### Levantar servicios

```
docker build -t "deber1_emiliosoria"
docker compose up -d
```

Servicios expuestos:
- Mage UI: http://localhost:6789
- pgAdmin: http://localhost:8080
- 
## 5) Gestión de secretos (sin valores)

Todos los secretos se configuran en **Mage Secrets**. A continuación se documentan **nombres, propósito y rotación**, sin incluir valores.
### QuickBooks
- **qbo_client_id**  
  Propósito: Identificador de la aplicación registrada en QBO  
  Rotación: cada 90 días

- **qbo_client_secret**  
  Propósito: Credencial privada asociada al Client ID  
  Rotación: cada 90 días

- **qbo_realm_id**  
  Propósito: Identificador único de la compañía en QBO  
  Rotación: cambia por ambiente (sandbox / prod)

- **qbo_refresh_token**  
  Propósito: Token de larga duración para renovar access tokens  
  Rotación: aproximadamente 101 dias de acuerdo a QBO. Sin embargo, se experimentaron rotaciones sin aviso cada 24 horas. 

- **qbo_env**  
  Propósito: Define el ambiente (sandbox o prod)

### PostgreSQL
- **pg_host**
- **pg_port**
- **pg_database**
- **pg_user**
- **pg_password**

## Pipelines de backfill

Se implementaron tres pipelines:
- **qb_customers_backfill**
- **qb_invoices_backfill**
- **qb_items_backfill**

### Parámetros (Trigger one-time)
Cada pipeline se ejecuta mediante un **trigger one-time** con los parámetros:
- **fecha_inicio** (UTC, ISO-8601). De acuerdo a pruebas se recomienda que se use la fecha 2025-01-01T00:00:00Z
- **fecha_fin** (UTC, ISO-8601). De acuerdo a pruebas se recomienda que se use la fecha 2026-12-31T00:00:00Z
- **chunk_days** (opcional, default = 30)

### Límites y reintentos
- Máximo **5 reintentos**.
- Backoff exponencial: 2^i segundos.
- En errores que no bloqueen el programa, se loguea y se continúa con el siguiente chunk.

## Evidencias

Las capturas y logs requeridos por la guía se encuentran en la carpeta:
```
evidencias/
```
separadas por entidad (customers / invoices / items).

## Checklist de aceptacion
✔️Mage y Postgres se comunican por nombre de servicio.
✔️Todos los secretos (QBO y Postgres) están en Mage Secrets; no hay secretos en el repo/entorno expuesto.
✔️Pipelines qb_<entidad>_backfill acepta fecha_inicio y fecha_fin (UTC) y segmenta el rango.
✔️Trigger one-time configurado, ejecutado y luego deshabilitado/marcado como completado.
✔️Esquema raw con tablas por entidad, payload completo y metadatos obligatorios.
✔️Idempotencia verificada: reejecución de un tramo no genera duplicados.
✔️Paginación y rate limits manejados y documentados.
✔️Volumetría y validaciones mínimas registradas y archivadas como evidencia.
✔️Runbook de reanudación y reintentos disponible y seguido.



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
├── README.md
├── scheduler_data/              # Código Mage: pipelines y blocks
├── warehouse_ui_data/           # Persistencia pgAdmin
├── evidencias/                  # Evidencias solicitadas
│   ├── customers/
│   ├── invoices/
│   └── items/
└── .gitignore
```

**Importante:**
- `scheduler_data/` **sí se versiona** (contiene el código del ETL).
- `warehouse_data/` **no se versiona** (datos internos de Postgres).

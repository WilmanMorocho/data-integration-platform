# Data Integration Platform

Plataforma para integración y procesamiento de archivos de datos (JSON/XML) vía API REST.

## Instalación

1. Clona el repo.
2. Crea un entorno virtual: `python -m venv .venv`
3. Activa: `.venv\Scripts\activate` (Windows)
4. Instala dependencias: `pip install -r requirements.txt`

## Uso rápido

### Backend
Ejecuta:
```bash
uvicorn backend.main:app --reload
```

## Endpoints principales

- `POST /process?company_name=...`  
	Sube un archivo JSON o XML para procesar. El procesamiento es asíncrono.
	- Parámetros: archivo (form-data), company_name (query)
	- Respuesta: `{ "message": "Processing started in background" }`

- `GET /status/{company_name}`  
	Consulta el estado y los registros procesados para una compañía.
	- Respuesta:
		```json
		{
			"company_name": "test",
			"records": [
				{"field1": "value1", "field2": 123, "field3": "data1", "status": "processed", "file_type": "json"}
			]
		}
		```

## Estados posibles de los registros

- `uploaded`: archivo recibido
- `processing`: en proceso
- `processed`: procesado correctamente
- `failed`/`error`: error de validación o procesamiento

## Flujo de procesamiento

1. El usuario sube un archivo (JSON/XML) vía `/process`.
2. El sistema valida el contenido y transforma los datos.
3. Los datos se guardan en la base de datos con el estado correspondiente.
4. El usuario puede consultar el estado y los datos vía `/status/{company_name}`.

## Ejemplo de uso con curl

```bash
# Subir archivo JSON
curl -F "file=@data.json" "http://localhost:8000/process?company_name=acme"

# Consultar estado
curl "http://localhost:8000/status/acme"
```

## Pruebas

Ejecuta:
```bash
pytest -s tests/test_upload.py -v
```
Verás el estado de cada archivo procesado en la salida.
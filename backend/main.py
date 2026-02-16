import logging
from fastapi import FastAPI, HTTPException, Request
from backend.database import engine, Base
from backend.models import company_data
from backend.routers import upload
from pydantic import BaseModel, ValidationError
from lxml import etree

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/app.log"),  # Archivo de log
        logging.StreamHandler()  # Consola
    ]
)
logger = logging.getLogger(__name__)

# Crear las tablas en la base de datos
Base.metadata.create_all(bind=engine)

# Inicializar la aplicación FastAPI
app = FastAPI()

# Ruta principal
@app.get("/")
def home():
    logger.info("Home endpoint accessed")
    return {"message": "Data Integration Platform running"}

# Incluir el router existente
app.include_router(upload.router)

# Modelo Pydantic para validar JSON
class DataModel(BaseModel):
    field1: str
    field2: int
    field3: str

# Ruta para validar JSON o XML
@app.post("/validate")
async def validate_data(request: Request):
    logger.info("Validate endpoint called")
    content_type = request.headers.get("content-type")
    
    # Validar JSON
    if content_type and "application/json" in content_type:
        try:
            body = await request.json()
            if not body:
                logger.error("JSON body is empty")
                raise HTTPException(status_code=400, detail="JSON body is empty")
            # Validar estructura JSON
            DataModel(**body)
            logger.info("JSON validation successful")
        except ValidationError as e:
            logger.error(f"JSON validation error: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid JSON structure: {e}")
        return {"message": "Valid JSON"}
    
    # Validar XML
    elif content_type and ("application/xml" in content_type or "text/xml" in content_type):
        try:
            body = await request.body()
            if not body.strip():
                logger.error("XML body is empty")
                raise HTTPException(status_code=400, detail="XML body is empty")
            # Validar estructura XML
            etree.fromstring(body)
            logger.info("XML validation successful")
        except etree.XMLSyntaxError as e:
            logger.error(f"XML validation error: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid XML: {e}")
        return {"message": "Valid XML"}
    
    # Tipo de contenido no soportado o faltante
    else:
        logger.warning(f"Unsupported content-type: {content_type}")
        raise HTTPException(status_code=415, detail="Unsupported or missing Content-Type. Use application/json or application/xml.")
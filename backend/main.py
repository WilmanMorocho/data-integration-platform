from fastapi import FastAPI, HTTPException, Request
from backend.database import engine, Base
from backend.models import company_data
from backend.routers import upload
from pydantic import BaseModel, ValidationError
from lxml import etree

# Crear las tablas en la base de datos
Base.metadata.create_all(bind=engine)

# Inicializar la aplicación FastAPI
app = FastAPI()

# Ruta principal
@app.get("/")
def home():
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
    content_type = request.headers.get("content-type")
    
    # Validar JSON
    if content_type and "application/json" in content_type:
        try:
            body = await request.json()
            if not body:
                raise HTTPException(status_code=400, detail="JSON body is empty")
            # Validar estructura JSON
            DataModel(**body)
        except ValidationError as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON structure: {e}")
        return {"message": "Valid JSON"}
    
    # Validar XML
    elif content_type and ("application/xml" in content_type or "text/xml" in content_type):
        try:
            body = await request.body()
            if not body.strip():
                raise HTTPException(status_code=400, detail="XML body is empty")
            # Validar estructura XML
            etree.fromstring(body)
        except etree.XMLSyntaxError as e:
            raise HTTPException(status_code=400, detail=f"Invalid XML: {e}")
        return {"message": "Valid XML"}
    
    # Tipo de contenido no soportado o faltante
    else:
        raise HTTPException(status_code=415, detail="Unsupported or missing Content-Type. Use application/json or application/xml.")
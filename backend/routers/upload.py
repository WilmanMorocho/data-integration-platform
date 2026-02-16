import io
import pandas as pd
import xml.etree.ElementTree as ET
import logging
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, ValidationError
from lxml import etree
from backend.database import SessionLocal
from backend.models.company_data import CompanyData

logger = logging.getLogger(__name__)

router = APIRouter()

# Dependencia para la base de datos
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Modelo Pydantic para validar JSON
class DataModel(BaseModel):
    field1: str
    field2: int
    field3: str

# Función para validar contenido
def validate_content(file_content: bytes, file_type: str):
    if file_type == "json":
        try:
            import json
            data = json.loads(file_content.decode("utf-8"))
            if not data:
                raise ValueError("JSON body is empty")
            if isinstance(data, list):
                for item in data:
                    DataModel(**item)
            else:
                DataModel(**data)
            return True
        except (json.JSONDecodeError, ValidationError) as e:
            raise ValueError(f"Invalid JSON: {e}")
    elif file_type == "xml":
        try:
            if not file_content.strip():
                raise ValueError("XML body is empty")
            etree.fromstring(file_content)
            return True
        except etree.XMLSyntaxError as e:
            raise ValueError(f"Invalid XML: {e}")
    else:
        raise ValueError("Unsupported file type")

# Función para transformar los datos
def transform_data(file_content: bytes, file_type: str):
    if file_type == "json":
        df = pd.read_json(io.BytesIO(file_content))
        df.drop_duplicates(inplace=True)
        df.fillna("N/A", inplace=True)
        for col in ["field1", "field2", "field3"]:
            if col not in df.columns:
                df[col] = "N/A"
        return df
    elif file_type == "xml":
        try:
            root = ET.fromstring(file_content.decode("utf-8"))
            data = []
            for child in root:
                data.append({subchild.tag: subchild.text for subchild in child})
            df = pd.DataFrame(data)
            df.drop_duplicates(inplace=True)
            df.fillna("N/A", inplace=True)
            for col in ["field1", "field2", "field3"]:
                if col not in df.columns:
                    df[col] = "N/A"
            return df
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML format: {e}")
    else:
        raise ValueError("Unsupported file type. Only JSON and XML are allowed.")

# Función para cargar los datos en la base de datos con status
def load_data_to_db(df, db: Session, company_name: str, status: str = "processed"):
    df["company_name"] = company_name
    
    for _, row in df.iterrows():
        record = CompanyData(
            company_name=row["company_name"],
            field1=row["field1"],
            field2=row["field2"],
            field3=row["field3"],
            status=status
        )
        db.add(record)
    db.commit()

# Función para procesar en background
def process_in_background(company_name: str, file_content: bytes, file_type: str):
    logger.info(f"Starting background processing for company: {company_name}, file_type: {file_type}")
    db = SessionLocal()
    try:
        # Status inicial: uploaded
        df_dummy = pd.DataFrame([{"field1": "N/A", "field2": 0, "field3": "N/A"}])
        load_data_to_db(df_dummy, db, company_name, status="uploaded")
        logger.info(f"Initial records created with status 'uploaded' for {company_name}")
        
        # Validar
        validate_content(file_content, file_type)
        logger.info(f"Validation successful for {company_name}")
        
        # Status: processing
        records = db.query(CompanyData).filter(CompanyData.company_name == company_name, CompanyData.status == "uploaded").all()
        for record in records:
            record.status = "processing"
        db.commit()
        logger.info(f"Status updated to 'processing' for {company_name}")
        
        # Transform
        df = transform_data(file_content, file_type)
        logger.info(f"Transformation successful for {company_name}")
        
        # Load con status "processed"
        load_data_to_db(df, db, company_name, status="processed")
        logger.info(f"ETL completed successfully for {company_name}")
        
    except Exception as e:
        # Status: error
        records = db.query(CompanyData).filter(CompanyData.company_name == company_name).all()
        for record in records:
            record.status = "error"
        db.commit()
        logger.error(f"Error in background processing for {company_name}: {e}")
    finally:
        db.close()

# Endpoint para procesar el archivo
@router.post("/process")
async def process_file(
    company_name: str,
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
):
    logger.info(f"Process endpoint called for company: {company_name}, file: {file.filename}")
    file_content = await file.read()
    file_type = file.filename.split(".")[-1]
    
    # Iniciar procesamiento en background
    background_tasks.add_task(process_in_background, company_name, file_content, file_type)
    
    return {"message": "Processing started in background"}

# Endpoint para consultar el status
@router.get("/status/{company_name}")
def get_status(company_name: str, db: Session = Depends(get_db)):
    logger.info(f"Status endpoint called for company: {company_name}")
    records = db.query(CompanyData).filter(CompanyData.company_name == company_name).all()
    if not records:
        logger.warning(f"No records found for company: {company_name}")
        raise HTTPException(status_code=404, detail="No records found for this company")
    
    return {
        "company_name": company_name,
        "records": [
            {
                "id": record.id,
                "status": record.status,
                "field1": record.field1,
                "field2": record.field2,
                "field3": record.field3,
                "created_at": record.created_at
            }
            for record in records
        ]
    }
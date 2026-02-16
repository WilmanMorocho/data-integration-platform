import io
import pandas as pd
import xml.etree.ElementTree as ET
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, ValidationError
from lxml import etree

from backend.database import SessionLocal
from backend.models.company_data import CompanyData

logger = logging.getLogger(__name__)

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class DataModel(BaseModel):
    field1: str
    field2: int
    field3: str

def validate_content(file_content: bytes, file_type: str):
    import json
    if file_type == "json":
        try:
            data = json.loads(file_content.decode("utf-8"))
            if not data:
                raise ValueError("JSON body is empty")
            # Soporta {"records": [...]} o lista directa
            if isinstance(data, dict) and "records" in data:
                for item in data["records"]:
                    DataModel(**item)
            elif isinstance(data, list):
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
            # Validar sintaxis XML
            root = ET.fromstring(file_content)
            # Validar campos requeridos parseando directamente el XML
            for child in root:
                record_dict = {}
                for subchild in child:
                    record_dict[subchild.tag] = subchild.text
                # Validar que los campos requeridos existan
                if "field1" not in record_dict or "field2" not in record_dict or "field3" not in record_dict:
                    raise ValueError("Missing required fields: field1, field2, or field3")
                # Validar con DataModel
                try:
                    field2_value = int(record_dict["field2"]) if record_dict["field2"] else 0
                except (ValueError, TypeError):
                    raise ValueError(f"Invalid field2 value: {record_dict.get('field2')}")
                DataModel(field1=record_dict["field1"] or "N/A", field2=field2_value, field3=record_dict["field3"] or "N/A")
            return True
        except (etree.XMLSyntaxError, ET.ParseError, ValueError, ValidationError) as e:
            raise ValueError(f"Invalid XML: {e}")
    else:
        raise ValueError("Unsupported file type")

def transform_data(file_content: bytes, file_type: str):
    import json
    if file_type == "json":
        parsed = json.loads(file_content.decode("utf-8"))
        if isinstance(parsed, dict) and "records" in parsed:
            records = parsed["records"]
        elif isinstance(parsed, list):
            records = parsed
        elif isinstance(parsed, dict):
            records = [parsed]
        else:
            raise ValueError("Unsupported JSON structure")
        df = pd.DataFrame(records)
    elif file_type == "xml":
        try:
            root = ET.fromstring(file_content.decode("utf-8"))
            data = []
            for child in root:
                data.append({subchild.tag: subchild.text for subchild in child})
            df = pd.DataFrame(data)
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML format: {e}")
    else:
        raise ValueError("Unsupported file type. Only JSON and XML are allowed.")

    df.drop_duplicates(inplace=True)
    # Asegura columnas requeridas
    for col in ["field1", "field2", "field3"]:
        if col not in df.columns:
            df[col] = None

    # field2 a entero, inválidos a 0
    df["field2"] = pd.to_numeric(df["field2"], errors="coerce").fillna(0).astype(int)
    df["field1"] = df["field1"].fillna("N/A").astype(str)
    df["field3"] = df["field3"].fillna("N/A").astype(str)
    return df

def load_data_to_db(df, db: Session, company_name: str, status: str = "processed", file_type: str = None):
    for _, row in df.iterrows():
        record = CompanyData(
            company_name=company_name,
            file_type=file_type,
            status=status,
            created_at=datetime.now(timezone.utc),
            field1=row["field1"],
            field2=int(row["field2"]),
            field3=row["field3"],
        )
        db.add(record)
    db.commit()

def process_in_background(company_name: str, file_content: bytes, file_type: str):
    logger.info(f"Starting background processing for company: {company_name}, file_type: {file_type}")
    db = SessionLocal()
    try:
        # Status inicial: uploaded
        df_dummy = pd.DataFrame([{"field1": "N/A", "field2": 0, "field3": "N/A"}])
        load_data_to_db(df_dummy, db, company_name, status="uploaded", file_type=file_type)
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

        # Eliminar registros anteriores antes de crear los nuevos con status "processed"
        old_records = db.query(CompanyData).filter(
            CompanyData.company_name == company_name,
            CompanyData.status.in_(["uploaded", "processing"])
        ).all()
        for old_record in old_records:
            db.delete(old_record)
        db.commit()
        logger.info(f"Old records deleted for {company_name}")

        # Load con status "processed"
        load_data_to_db(df, db, company_name, status="processed", file_type=file_type)
        logger.info(f"ETL completed successfully for {company_name}")

    except Exception as e:
        logger.error(f"Error in background processing for {company_name}: {e}")
        try:
            db.rollback()
        except Exception:
            pass
        # Marca como failed si hay error
        try:
            fresh_db = SessionLocal()
            records = fresh_db.query(CompanyData).filter(CompanyData.company_name == company_name).all()
            for rec in records:
                rec.status = "failed"
            fresh_db.commit()
            fresh_db.close()
        except Exception:
            pass
    finally:
        db.close()

@router.post("/process")
async def process_file(
    company_name: str,
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
):
    logger.info(f"Process endpoint called for company: {company_name}, file: {file.filename}")
    file_content = await file.read()
    file_type = file.filename.split(".")[-1].lower()

    if file_type not in ("json", "xml"):
        raise HTTPException(status_code=400, detail="Unsupported file type")

    background_tasks.add_task(process_in_background, company_name, file_content, file_type)
    return {"message": "Processing started in background"}

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
                "created_at": record.created_at.isoformat() if record.created_at else None,
                "file_type": record.file_type,
            }
            for record in records
        ],
    }
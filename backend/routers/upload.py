import io
import pandas as pd
import xml.etree.ElementTree as ET
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from sqlalchemy.orm import Session
from backend.database import SessionLocal
from backend.models.company_data import CompanyData

router = APIRouter()

# Dependencia para la base de datos
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Función para transformar los datos
def transform_data(file_content: bytes, file_type: str):
    if file_type == "json":
        # Leer y transformar JSON
        df = pd.read_json(io.BytesIO(file_content))
        df.drop_duplicates(inplace=True)
        df.fillna("N/A", inplace=True)
        # Asegurar que las columnas requeridas existan
        for col in ["field1", "field2", "field3"]:
            if col not in df.columns:
                df[col] = "N/A"
        return df
    elif file_type == "xml":
        # Leer y transformar XML
        try:
            root = ET.fromstring(file_content.decode("utf-8"))
            data = []
            for child in root:
                data.append({subchild.tag: subchild.text for subchild in child})
            df = pd.DataFrame(data)
            df.drop_duplicates(inplace=True)
            df.fillna("N/A", inplace=True)
            # Asegurar que las columnas requeridas existan
            for col in ["field1", "field2", "field3"]:
                if col not in df.columns:
                    df[col] = "N/A"
            return df
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML format: {e}")
    else:
        raise ValueError("Unsupported file type. Only JSON and XML are allowed.")

# Función para cargar los datos en la base de datos
def load_data_to_db(df, db: Session, company_name: str):
    # Agregar la columna 'company_name' al DataFrame
    df["company_name"] = company_name
    
    for _, row in df.iterrows():
        record = CompanyData(
            company_name=row["company_name"],
            field1=row["field1"],
            field2=row["field2"],
            field3=row["field3"]
        )
        db.add(record)
    db.commit()

# Endpoint para procesar el archivo (ETL)
@router.post("/process")
async def process_file(
    company_name: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    # Extract
    file_content = await file.read()
    file_type = file.filename.split(".")[-1]

    # Transform
    try:
        df = transform_data(file_content, file_type)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Load
    load_data_to_db(df, db, company_name)

    return {"message": "ETL process completed"}
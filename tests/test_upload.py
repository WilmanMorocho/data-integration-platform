import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
import time
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from backend.main import app
from backend.models.company_data import CompanyData
from backend.database import get_db

@pytest.fixture
def client():
    return TestClient(app)

@pytest.fixture
def db_session():
    db = next(get_db())
    yield db
    db.rollback()

def wait_for_status(db_session, company_name, expected_status, timeout=12):
    start = time.time()
    while time.time() - start < timeout:
        records = db_session.query(CompanyData).filter(CompanyData.company_name == company_name).all()
        if records and all(r.status == expected_status for r in records):
            return True
        time.sleep(1)
    return False

valid_json = """{"records": [{"field1": "value1", "field2": 123, "field3": "data1"}]}"""
invalid_xml = """<?xml version="1.0"?><root><record><field1>value1</field1><field3>data1</field3></record></root>"""  # Missing field2
valid_xml = """<?xml version="1.0"?><root><record><field1>value1</field1><field2>123</field2><field3>data1</field3></record></root>"""

def test_upload_valid_xml(client, db_session):
    import io
    file = io.BytesIO(valid_xml.encode())
    file.name = "test.xml"
    response = client.post("/process?company_name=test_xml", files={"file": ("test.xml", file, "application/xml")})
    assert response.status_code == 200
    assert response.json() == {"message": "Processing started in background"}
    
    # Esperar a que el procesamiento termine
    assert wait_for_status(db_session, "test_xml", "processed"), "El archivo no fue procesado correctamente"
    
    # Verificar que los datos fueron guardados correctamente
    records = db_session.query(CompanyData).filter(CompanyData.company_name == "test_xml").all()
    assert len(records) > 0, "No se encontraron registros procesados"
    assert all(r.status == "processed" for r in records), "No todos los registros tienen estado 'processed'"
    
    # Verificar que los datos son correctos
    record = records[0]
    assert record.field1 == "value1", f"field1 incorrecto: esperado 'value1', obtenido '{record.field1}'"
    assert record.field2 == 123, f"field2 incorrecto: esperado 123, obtenido {record.field2}"
    assert record.field3 == "data1", f"field3 incorrecto: esperado 'data1', obtenido '{record.field3}'"
    if record.file_type:
        assert record.file_type == "xml", f"file_type incorrecto: esperado 'xml', obtenido '{record.file_type}'"
    
    print("[ESTADO] Archivo XML:")
    if record.status == "processed":
        print("   - Estado: PROCESADO")
    elif record.status == "validated":
        print("   - Estado: VALIDADO")
    elif record.status in ["failed", "error"]:
        print("   - Estado: ERROR")
    else:
        print(f"   - Estado: {record.status}")
    print(f"   - field1: {record.field1}")
    print(f"   - field2: {record.field2}")
    print(f"   - field3: {record.field3}")
    print(f"   - Tipo de archivo: {record.file_type or 'No especificado'}")

def test_upload_invalid_xml(client, db_session):
    import io
    file = io.BytesIO(invalid_xml.encode())
    file.name = "test.xml"
    response = client.post("/process?company_name=test_invalid_xml", files={"file": ("test.xml", file, "application/xml")})
    assert response.status_code == 200
    assert response.json() == {"message": "Processing started in background"}
    
    # Espera a que el status sea "failed" (o "error" si así lo maneja tu backend)
    failed = wait_for_status(db_session, "test_invalid_xml", "failed")
    error = wait_for_status(db_session, "test_invalid_xml", "error")
    assert failed or error, "El archivo inválido debería haber fallado pero no se marcó como 'failed' o 'error'"
    
    # Verificar que el archivo fue rechazado correctamente
    records = db_session.query(CompanyData).filter(CompanyData.company_name == "test_invalid_xml").all()
    assert len(records) > 0, "Debería haber registros con estado 'failed'"
    assert all(r.status in ["failed", "error"] for r in records), "Todos los registros deberían tener estado 'failed' o 'error'"
    
    print("[ESTADO] Archivo XML inválido:")
    estado = records[0].status
    if estado == "processed":
        print("   - Estado: PROCESADO")
    elif estado == "validated":
        print("   - Estado: VALIDADO")
    elif estado in ["failed", "error"]:
        print("   - Estado: ERROR")
    else:
        print(f"   - Estado: {estado}")
    print(f"   - Razon: Falta el campo requerido 'field2'")

def test_upload_valid_json(client, db_session):
    import io
    file = io.BytesIO(valid_json.encode())
    file.name = "test.json"
    response = client.post("/process?company_name=test_json", files={"file": ("test.json", file, "application/json")})
    assert response.status_code == 200
    assert response.json() == {"message": "Processing started in background"}
    
    # Esperar a que el procesamiento termine
    assert wait_for_status(db_session, "test_json", "processed"), "El archivo no fue procesado correctamente"
    
    # Verificar que los datos fueron guardados correctamente
    records = db_session.query(CompanyData).filter(CompanyData.company_name == "test_json").all()
    assert len(records) > 0, "No se encontraron registros procesados"
    assert all(r.status == "processed" for r in records), "No todos los registros tienen estado 'processed'"
    
    # Verificar que los datos son correctos
    record = records[0]
    assert record.field1 == "value1", f"field1 incorrecto: esperado 'value1', obtenido '{record.field1}'"
    assert record.field2 == 123, f"field2 incorrecto: esperado 123, obtenido {record.field2}"
    assert record.field3 == "data1", f"field3 incorrecto: esperado 'data1', obtenido '{record.field3}'"
    if record.file_type:
        assert record.file_type == "json", f"file_type incorrecto: esperado 'json', obtenido '{record.file_type}'"
    
    print("[ESTADO] Archivo JSON:")
    if record.status == "processed":
        print("   - Estado: PROCESADO")
    elif record.status == "validated":
        print("   - Estado: VALIDADO")
    elif record.status in ["failed", "error"]:
        print("   - Estado: ERROR")
    else:
        print(f"   - Estado: {record.status}")
    print(f"   - field1: {record.field1}")
    print(f"   - field2: {record.field2}")
    print(f"   - field3: {record.field3}")
    print(f"   - Tipo de archivo: {record.file_type or 'No especificado'}")

def test_check_status(client, db_session):
    import io
    file = io.BytesIO(valid_json.encode())
    file.name = "test.json"
    response = client.post("/process?company_name=status_test", files={"file": ("test.json", file, "application/json")})
    assert response.status_code == 200
    assert response.json() == {"message": "Processing started in background"}
    
    # Esperar a que el procesamiento termine
    assert wait_for_status(db_session, "status_test", "processed"), "El archivo no fue procesado correctamente"
    
    # Consultar el status real a través del endpoint
    response = client.get("/status/status_test")
    assert response.status_code == 200
    data = response.json()
    
    # Verificar estructura de la respuesta
    assert data["company_name"] == "status_test", f"Nombre de compañía incorrecto: esperado 'status_test', obtenido '{data['company_name']}'"
    assert len(data["records"]) > 0, "No se encontraron registros en la respuesta"
    assert all(r["status"] == "processed" for r in data["records"]), "No todos los registros tienen estado 'processed'"
    
    # Verificar que los datos en la respuesta son correctos
    record = data["records"][0]
    assert record["field1"] == "value1", f"field1 incorrecto: esperado 'value1', obtenido '{record['field1']}'"
    assert record["field2"] == 123, f"field2 incorrecto: esperado 123, obtenido {record['field2']}"
    assert record["field3"] == "data1", f"field3 incorrecto: esperado 'data1', obtenido '{record['field3']}'"
    if record.get("file_type"):
        assert record["file_type"] == "json", f"file_type incorrecto: esperado 'json', obtenido '{record['file_type']}'"
    
    print("[ESTADO] Endpoint de status:")
    print(f"   - Compania: {data['company_name']}")
    print(f"   - Registros encontrados: {len(data['records'])}")
    estados = []
    for r in data["records"]:
        if r["status"] == "processed":
            estados.append("PROCESADO")
        elif r["status"] == "validated":
            estados.append("VALIDADO")
        elif r["status"] in ["failed", "error"]:
            estados.append("ERROR")
        else:
            estados.append(r["status"])
    print(f"   - Estado de todos los registros: {estados}")
    print(f"   - Datos del primer registro:")
    print(f"     * field1: {record['field1']}")
    print(f"     * field2: {record['field2']}")
    print(f"     * field3: {record['field3']}")
    print(f"     * file_type: {record.get('file_type', 'No especificado')}")
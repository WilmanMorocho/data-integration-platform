from sqlalchemy import Column, Integer, String, DateTime
from backend.database import Base
from datetime import datetime

class CompanyData(Base):
    __tablename__ = "company_data"

    id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String, index=True)
    file_type = Column(String)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    field1 = Column(String, nullable=False)
    field2 = Column(Integer, nullable=False)
    field3 = Column(String, nullable=False)
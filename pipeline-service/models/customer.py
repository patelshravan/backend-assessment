from sqlalchemy import Column, Date, DateTime, Numeric, String, Text
from database import Base

class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = {"schema": "public"}

    # Assessment schema:
    # customer_id VARCHAR(50) PRIMARY KEY
    customer_id = Column(String(50), primary_key=True)

    # Assessment schema:
    # first_name VARCHAR(100) NOT NULL
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(255), nullable=False)

    # Remaining fields (nullable not specified in rubric)
    phone = Column(String(20), nullable=True)
    address = Column(Text, nullable=True)
    date_of_birth = Column(Date, nullable=True)
    account_balance = Column(Numeric(15, 2), nullable=True)
    created_at = Column(DateTime(timezone=False), nullable=True)
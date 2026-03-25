from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import func

from database import Base, SessionLocal, engine
from models.customer import Customer
from services.ingestion import ingest

app = FastAPI()

# Ensure endpoints don't 500 when called before the first ingest run.
Base.metadata.create_all(bind=engine)


class CustomerOut(BaseModel):
    customer_id: str
    first_name: str
    last_name: str
    email: str
    phone: Optional[str] = None
    address: Optional[str] = None
    date_of_birth: Optional[date] = None
    account_balance: Optional[Decimal] = None
    created_at: Optional[datetime] = None


class PaginatedCustomersOut(BaseModel):
    data: List[CustomerOut]
    total: int
    page: int
    limit: int


@app.post("/api/ingest")
def run_ingest():
    count = ingest()
    return {"status": "success", "records_processed": count}


@app.get("/api/customers", response_model=PaginatedCustomersOut)
def get_customers(page: int = 1, limit: int = 10):
    db = SessionLocal()
    try:
        start = (page - 1) * limit

        total = db.query(func.count(Customer.customer_id)).scalar() or 0
        rows = db.query(Customer).order_by(Customer.customer_id).offset(start).limit(limit).all()

        # Convert ORM instances -> Pydantic
        data = [
            CustomerOut(
                customer_id=r.customer_id,
                first_name=r.first_name,
                last_name=r.last_name,
                email=r.email,
                phone=r.phone,
                address=r.address,
                date_of_birth=r.date_of_birth,
                account_balance=r.account_balance,
                created_at=r.created_at,
            )
            for r in rows
        ]

        return {"data": data, "total": total, "page": page, "limit": limit}
    finally:
        db.close()


@app.get("/api/customers/{id}", response_model=CustomerOut)
def get_customer(id: str):
    db = SessionLocal()
    try:
        customer = db.query(Customer).filter_by(customer_id=id).first()
        if not customer:
            raise HTTPException(status_code=404, detail="Not found")

        return CustomerOut(
            customer_id=customer.customer_id,
            first_name=customer.first_name,
            last_name=customer.last_name,
            email=customer.email,
            phone=customer.phone,
            address=customer.address,
            date_of_birth=customer.date_of_birth,
            account_balance=customer.account_balance,
            created_at=customer.created_at,
        )
    finally:
        db.close()
from sqlalchemy import create_engine, Column, Integer, String, Date, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Product(Base):
    __tablename__ = 'products'
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String, nullable=False)
    category = Column(String)
    price = Column(Float)

class Store(Base):
    __tablename__ = 'stores'
    store_id = Column(Integer, primary_key=True)
    store_name = Column(String, nullable=False)
    location = Column(String)

class DateDim(Base):
    __tablename__ = 'dates'
    date_id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    year = Column(Integer)
    quarter = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)

class SalesFact(Base):
    __tablename__ = 'sales_data'
    sale_id = Column(Integer, primary_key=True)
    product_id = Column(Integer)
    store_id = Column(Integer)
    date_id = Column(Integer)
    quantity_sold = Column(Integer)
    total_sales = Column(Float)

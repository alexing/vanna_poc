import re
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Float, Date, Integer
from sqlalchemy.ext.declarative import declarative_base

# SQLAlchemy setup
Base = declarative_base()

# Function to normalize and convert dates
def normalize_date(date_str):
    # Regular expression to match date patterns
    match = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    if match:
        month, day, year = match.groups()
        # Add leading zeros to day and month if needed
        day = day.zfill(2)
        month = month.zfill(2)
        # Convert to Python datetime.date object
        return datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d").date()
    else:
        return None  # Return None if the pattern doesn't match

def normalize_time(time_str):
    # Regular expression to match time patterns
    match = re.match(r"(\d{1,2}):(\d{1,2})", time_str)
    if match:
        hour, minute = match.groups()
        # Add leading zeros to hour and minute if needed
        hour = hour.zfill(2)
        minute = minute.zfill(2)
        # Convert to Python datetime.time object
        return datetime.strptime(f"{hour}:{minute}", "%H:%M").time()
    else:
        return None  # Return None if the pattern doesn't match

# Define a table structure with SQLAlchemy, including a primary key
class Sales(Base):
    __tablename__ = 'sales'
    id = Column(Integer, primary_key=True, autoincrement=True)
    week_day = Column(String, nullable=True)
    date = Column(Date, nullable=False)  # Store date as DATE type
    time = Column(Date, nullable=True)
    ticket_number = Column(String, nullable=True)
    waiter = Column(Integer, nullable=True)
    product_name = Column(String, nullable=True)
    quantity = Column(Integer, nullable=True)
    unitary_price = Column(Integer, nullable=True)
    total = Column(Integer, nullable=True)

def load_csv_to_sqlite(csv_file, db_file, table_name):
    # Read CSV
    df = pd.read_csv(csv_file)

    # Normalize the date column
    date_column = "date"  # Replace with the name of your date column
    if date_column in df.columns:
        df[date_column] = df[date_column].apply(normalize_date)

    time_column = "time"
    if time_column in df.columns:
        df[time_column] = df[time_column].apply(normalize_time)
    # Remove rows with invalid dates
    df = df.dropna(subset=[date_column])

    # Create SQLite engine
    engine = create_engine(f'sqlite:///{db_file}')
    Base.metadata.drop_all(engine)  # Clear existing table
    Base.metadata.create_all(engine)  # Create table structure

    # Insert data into the database
    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists='append', index=False)
    print(f"Loaded {csv_file} into {db_file} as table '{table_name}' with proper DATE type.")

if __name__ == "__main__":
    # Example usage
    load_csv_to_sqlite('data.csv', 'data.db', 'sales')

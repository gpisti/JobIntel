import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert
from database.models import Base, RawJob, Company, Location
from logger import LoggerManager
from contextlib import contextmanager
from typing import List, Dict

DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "job_db"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
ADMIN_DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres"

logger_manager = LoggerManager(log_dir="logs")
logger = logger_manager.get_logger("database")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


@contextmanager
def session_scope():
    """
    Provide a transactional scope for database operations.

    This context manager yields a database session, commits changes on success,
    rolls back on failure, and closes the session when complete.
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database transaction error: {e}")
        raise
    finally:
        session.close()


def create_database():
    """
    Create a new PostgreSQL database if it does not already exist.

    Connects to the administrative database, checks whether the target database
    exists, and creates it if missing. Logs the result of the operation and closes
    the connection. Raises an exception for any errors encountered.
    """
    try:
        conn = psycopg2.connect(ADMIN_DB_URL)
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f"CREATE DATABASE {DB_NAME};")
            logger.info(f"Database '{DB_NAME}' created successfully.")
        else:
            logger.info(f"Database '{DB_NAME}' already exists.")

        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error while creating database: {e}")


def init_db():
    """
    Initialize the database and tables.

    Creates the database if necessary, sets up all defined tables, and logs a success message.
    """
    create_database()
    Base.metadata.create_all(engine)
    logger.info("Tables initialized successfully.")


def get_or_create_company(session: Session, name: str) -> int:
    """
    Retrieve or create a company record in the database.

    :param session: Database session instance.
    :param name: The name of the company.
    :return: The ID of the existing or newly created company record.
    """
    name = (name or "Unknown Company").strip()
    company = session.query(Company).filter_by(name=name).first()
    if not company:
        company = Company(name=name)
        session.add(company)
        session.commit()
        session.refresh(company)
        logger.info(f"New company added: {name}")
    return company.id


def get_or_create_location(
    session: Session, city: str, country: str = "Hungary"
) -> int:
    """
    Return the location ID for the given city and country, creating a new entry if none exists.

    Parameters:
        session (Session): Database session object.
        city (str): Name of the city.
        country (str, optional): Name of the country. Defaults to "Hungary".

    Returns:
        int: ID of the retrieved or newly created location.
    """
    city = (city or "Unknown Location").strip()
    location = session.query(Location).filter_by(city=city, country=country).first()
    if not location:
        location = Location(city=city, country=country)
        session.add(location)
        session.commit()
        session.refresh(location)
        logger.info(f"New location added: {city}, {country}")
    return location.id


def save_raw_jobs_to_db(job_data: List[Dict]):
    """
    Saves raw job data to the database by creating or retrieving the related company
    and location entries. Logs an error and skips any job with missing company or
    location IDs. Inserts all valid new jobs into the database and logs a success
    message upon completion.

    Parameters:
        job_data (List[Dict]): A list of dictionaries containing job details such
            as title, company, location, job_url, and full_description.
    """
    with session_scope() as session:
        new_jobs = []
        for job in job_data:
            company_name = (job.get("company") or "Unknown Company").strip()
            location_name = (job.get("location") or "Unknown Location").strip()

            company_id = get_or_create_company(session, company_name)
            location_id = get_or_create_location(session, location_name)

            if company_id is None or location_id is None:
                logger.error(f"Missing ID! Skipping job: {job['title']}")
                continue

            new_jobs.append(
                {
                    "title": job["title"],
                    "company_id": company_id,
                    "location_id": location_id,
                    "job_url": job["job_url"],
                    "full_description": job["full_description"],
                    "source_platform": job.get("source_platform", "unknown"),
                    "session_id": job.get("session_id", "N/A"),
                }
            )

        if new_jobs:
            stmt = insert(RawJob).values(new_jobs).on_conflict_do_nothing()
            session.execute(stmt)
            logger.info(f"Successfully saved {len(new_jobs)} jobs to the database.")

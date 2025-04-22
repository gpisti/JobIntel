import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from database.postgres.models import Base
from logger import LoggerManager
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


class DatabaseInitializer:
    def __init__(self):
        self.logger = LoggerManager().get_logger("db_init")
        self.admin_uri = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
        self.target_uri = "postgresql://postgres:postgres@127.0.0.1:5432/jobs"
        self.target_db = "jobs"

        self.mongo_uri = "mongodb://localhost:27017"
        self.mongo_db_name = "jobs_db"
        self.mongo_collection_name = "raw_jobs"

    def _create_database(self):
        """Create database if not exists"""
        try:
            engine = create_engine(self.admin_uri)
            with engine.connect() as conn:
                conn.execute(text("COMMIT"))
                conn.execute(text(f"CREATE DATABASE {self.target_db}"))
                self.logger.info(f"Created database: {self.target_db}")
            return True
        except ProgrammingError as e:
            if "already exists" in str(e):
                self.logger.info(f"Database {self.target_db} already exists")
                return True
            self.logger.error(f"Database creation failed: {str(e)}")
            return False
        except OperationalError as e:
            self.logger.error(f"Connection failed: {str(e)}")
            return False

    def _enable_postgis(self):
        """Enable PostGIS extension if not already enabled"""
        try:
            engine = create_engine(self.target_uri)
            with engine.connect() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
                conn.commit()
            self.logger.info("PostGIS extension enabled")
            return True
        except Exception as e:
            self.logger.error(f"PostGIS extension enable failed: {str(e)}")
            return False

    def _create_tables(self):
        """Create all tables"""
        try:
            engine = create_engine(self.target_uri)
            Base.metadata.create_all(engine)
            self.logger.info("Tables created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Table creation failed: {str(e)}")
            return False

    def _init_mongodb(self):
        """Initialize MongoDB with collection and indexes"""
        try:
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=3000)
            db = client[self.mongo_db_name]

            if self.mongo_collection_name not in db.list_collection_names():
                db.create_collection(self.mongo_collection_name)
                self.logger.info(
                    f"MongoDB collection '{self.mongo_collection_name}' created"
                )

            collection = db[self.mongo_collection_name]
            collection.create_index("scraped_at")
            collection.create_index("source_platform")
            collection.create_index("session_id")

            self.logger.info("MongoDB indexes created")
            return True
        except ConnectionFailure as e:
            self.logger.error(f"MongoDB connection failed: {str(e)}")
            return False
        except OperationFailure as e:
            self.logger.error(f"MongoDB operation failed: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"MongoDB initialization failed: {str(e)}")
            return False

    def initialize(self):
        """Main initialization flow"""
        self.logger.info("Starting database initialization...")

        if not self._create_database():
            return False

        if not self._enable_postgis():
            return False

        if not self._create_tables():
            return False

        if not self._init_mongodb():
            return False

        self.logger.info("All databases initialized successfully")
        return True


if __name__ == "__main__":
    initializer = DatabaseInitializer()
    if not initializer.initialize():
        exit(1)

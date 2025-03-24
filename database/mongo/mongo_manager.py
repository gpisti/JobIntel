from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, PyMongoError
from contextlib import contextmanager
from typing import List, Dict
from logger import LoggerManager


class MongoManager:
    """
    Manages MongoDB connections and job insert operations in batches. Uses a provided LoggerManager
    for logging, ensures resource cleanup through a context manager, and handles errors such as
    invalid documents and bulk write exceptions.
    """

    def __init__(self, logger_manager: LoggerManager):
        """
        Initializes the MongoDB manager with a provided LoggerManager, sets up a dedicated logger,
        and configures default connection settings, including batch size for bulk operations.

        Args:
            logger_manager (LoggerManager): The logger manager to handle log messages.
        """
        self.logger_manager = logger_manager
        self.logger = logger_manager.get_logger("mongodb")
        self.client = None
        self.batch_size = 500

        self.uri = "mongodb://localhost:27017"
        self.db_name = "jobs_db"
        self.col_name = "raw_jobs"

    @contextmanager
    def _get_collection(self):
        """
        Context manager for acquiring a MongoDB collection and closing the client connection automatically.

        Yields:
            The MongoDB collection for database operations.

        Raises:
            PyMongoError: If connecting to MongoDB fails.
        """
        try:
            client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            yield client[self.db_name][self.col_name]
            client.close()
        except PyMongoError as e:
            self.logger.error(f"MongoDB connection failed: {str(e)}")
            raise

    def insert_jobs(self, jobs: List[Dict]) -> Dict:
        """
        Inserts job documents into the database in batches, capturing invalid entries and bulk write errors.

        Parameters:
            jobs (List[Dict]): The job documents to be inserted.

        Returns:
            Dict: Summary of the insertion process, including total jobs processed, successful inserts,
                  invalid documents, and any write errors that occurred.
        """
        results = {
            "total": len(jobs),
            "success": 0,
            "invalid_documents": [],
            "write_errors": [],
        }

        valid_ops = []
        for idx, job in enumerate(jobs):
            try:
                valid_ops.append(InsertOne(job))
            except Exception as e:
                results["invalid_documents"].append(
                    {"index": idx, "error": str(e), "document": job}
                )
                self.logger_manager.log_failed_insert(
                    "raw_jobs", job, f"Insert build failed: {str(e)}"
                )

        try:
            with self._get_collection() as collection:
                for i in range(0, len(valid_ops), self.batch_size):
                    batch = valid_ops[i : i + self.batch_size]
                    result = collection.bulk_write(batch, ordered=False)
                    results["success"] += result.inserted_count
        except BulkWriteError as bwe:
            self.logger.error(f"Bulk write error: {str(bwe.details)}")
            for err in bwe.details.get("writeErrors", []):
                results["write_errors"].append(
                    {
                        "index": err["index"],
                        "code": err["code"],
                        "message": err["errmsg"],
                    }
                )
        except Exception as e:
            self.logger.error(f"Unexpected insert error: {str(e)}")
            raise

        return results

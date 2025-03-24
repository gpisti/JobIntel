from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
from logger import LoggerManager


class PostgreSQLManager:
    def __init__(self, logger: LoggerManager):
        """
        Initialize the database manager with a logger, create a Postgres engine, and configure a session for database operations.

        Args:
            logger (LoggerManager): The logger manager used for logging.
        """
        self.logger = logger.get_logger("postgres")
        self.engine = create_engine(
            "postgresql://user:pass@localhost:5432/jobs",
            pool_size=20,
            max_overflow=10,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 5},
        )
        self.session_factory = sessionmaker(
            bind=self.engine, autoflush=False, autocommit=False
        )
        self.Session = scoped_session(self.session_factory)

    @contextmanager
    def session_scope(self, session_id: str = None):
        """
        Provide a transactional scope around a SQLAlchemy session.
        A new session is created and assigned a session ID (if not provided, defaults to 'default').
        On successful completion, the session is committed; otherwise, it is rolled back.
        The session is closed after exiting the context.
        """
        session = self.Session()
        session.info["session_id"] = session_id or "default"

        try:
            self.logger.debug(f"Starting session {session.info['session_id']}")
            yield session
            session.commit()
            self.logger.debug(f"Committed session {session.info['session_id']}")
        except Exception as e:
            session.rollback()
            self.logger.error(
                f"Rollback session {session.info['session_id']}: {str(e)}",
                exc_info=True,
            )
            raise
        finally:
            self.logger.debug(f"Closing session {session.info['session_id']}")
            self.Session.remove()

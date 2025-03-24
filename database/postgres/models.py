from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Float,
    TIMESTAMP,
    Boolean,
    ForeignKey,
    CheckConstraint,
    Index,
    UniqueConstraint,
    ARRAY,
    JSON,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import UUID
from geoalchemy2 import Geography

Base = declarative_base()


class Company(Base):
    """
    Represents a company entity in the database.

    Attributes:
        id: The primary key identifier of the company.
        name: The company's name.
        normalized_name: A unique, normalized form of the company's name.
        aliases: A list of alternative names for the company.

    Relationships:
        jobs: The associated Job entities.
    """

    __tablename__ = "companies"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    normalized_name = Column(String(255), unique=True, nullable=False)
    aliases = Column(ARRAY(String(255)))

    jobs = relationship("Job", back_populates="company")

    __table_args__ = (
        Index("ix_companies_normalized", "normalized_name", postgresql_using="hash"),
    )


class Location(Base):
    """
    Represents a location entity with city, country, region, and a geographic point for mapping.
    Enforces a unique constraint on city-country and indexes geographical data for efficient queries.
    """

    __tablename__ = "locations"
    id = Column(Integer, primary_key=True)
    city = Column(String(255), nullable=False)
    country = Column(String(255), nullable=False, server_default="Hungary")
    region = Column(String(255))
    geo_point = Column(Geography(geometry_type="POINT", srid=4326))

    jobs = relationship("Job", back_populates="location")

    __table_args__ = (
        UniqueConstraint("city", "country", name="uc_city_country"),
        Index("ix_location_geo", "geo_point", postgresql_using="gist"),
    )


class Technology(Base):
    """
    Represents a technology entry with a unique name and optional category, storing details
    for technologies used within the system.
    """

    __tablename__ = "technologies"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    category = Column(String(50))


class JobTechnology(Base):
    """
    Association table that links a job and a technology with optional context text and a mention count.

    Attributes:
        job_id: The unique identifier of the related job.
        tech_id: The unique identifier of the related technology.
        context: Additional notes regarding the relationship.
        mention_count: Number of times the technology is mentioned.
    """

    __tablename__ = "job_technologies"
    job_id = Column(Integer, ForeignKey("jobs.id"), primary_key=True)
    tech_id = Column(Integer, ForeignKey("technologies.id"), primary_key=True)
    context = Column(Text)
    mention_count = Column(Integer, server_default="1")

    job = relationship("Job", back_populates="technologies")
    technology = relationship("Technology")


class Job(Base):
    """
    Represents a job listing in the database, linking it to a company, location, and associated technologies.
    Enforces constraints on salary ranges, employment type, and remote options.
    """

    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True)
    title = Column(Text, nullable=False)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    job_url = Column(Text, nullable=False)
    salary_min = Column(Float)
    salary_max = Column(Float)
    currency = Column(String(3))
    employment_type = Column(String(20))
    remote_option = Column(String(10))
    experience_level = Column(String(15))
    job_fingerprint = Column(String(64), nullable=False, unique=True)
    scraped_at = Column(TIMESTAMP, nullable=False)
    processed_at = Column(TIMESTAMP, nullable=False)
    source_platform = Column(String(255), nullable=False)
    processing_session = Column(UUID, nullable=False)
    deprecated = Column(Boolean, server_default="false")
    deprecated_at = Column(TIMESTAMP)
    job_metadata = Column(JSON)

    company = relationship("Company", back_populates="jobs")
    location = relationship("Location", back_populates="jobs")
    technologies = relationship("JobTechnology", back_populates="job")

    __table_args__ = (
        CheckConstraint("salary_min <= salary_max", name="check_salary_range"),
        CheckConstraint(
            "employment_type IN ('full-time', 'part-time', 'contract', 'freelance', 'other')",
            name="check_employment_type",
        ),
        CheckConstraint(
            "remote_option IN ('onsite', 'remote', 'hybrid', 'unknown')",
            name="check_remote_option",
        ),
        Index("ix_jobs_processed_session", "processing_session"),
        Index("ix_jobs_fingerprint", "job_fingerprint", postgresql_using="hash"),
    )

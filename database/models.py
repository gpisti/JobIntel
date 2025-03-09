from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    TIMESTAMP,
    ForeignKey,
    Boolean,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


class Company(Base):
    """
    Represents a company entity in the database, holding basic information
    and maintaining relationships to raw and processed job entries.
    """

    __tablename__ = "companies"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    jobs = relationship("RawJob", back_populates="company")
    processed_jobs = relationship("Job", back_populates="company")


class Location(Base):
    """
    Represents a location record with a unique city-country combination
    and defines relationships with associated job entities.
    """

    __tablename__ = "locations"

    id = Column(Integer, primary_key=True)
    city = Column(String, nullable=False)
    country = Column(String, nullable=False)

    __table_args__ = (UniqueConstraint("city", "country"),)

    jobs = relationship("RawJob", back_populates="location")
    processed_jobs = relationship("Job", back_populates="location")


class RawJob(Base):
    """
    Represents a raw job entry in the database.

    Stores essential job information, including title, company, location, and a detailed description.
    Tracks when the job was scraped, identifies its source, and indicates whether it is deprecated.
    Defines relationships to the associated company and location models.
    """

    __tablename__ = "raw_jobs"

    id = Column(Integer, primary_key=True)
    title = Column(Text, nullable=False)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    job_url = Column(Text, unique=True, nullable=False)
    full_description = Column(Text, nullable=False)
    scraped_at = Column(TIMESTAMP, server_default=func.now())
    deprecated = Column(Boolean, default=False)
    source_platform = Column(String, nullable=False)
    session_id = Column(String, nullable=False)

    company = relationship("Company", back_populates="jobs")
    location = relationship("Location", back_populates="jobs")


class Job(Base):
    """
    Represents a job posting record in the database, storing essential fields like title, company, location, and status flags.
    Provides relationships to the associated company, location, and related technologies.
    """

    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True)
    title = Column(Text, nullable=False)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    job_url = Column(Text, unique=True, nullable=False)
    description = Column(Text, nullable=False)
    processed_at = Column(TIMESTAMP, server_default=func.now())
    deprecated = Column(Boolean, default=False)
    deprecated_at = Column(TIMESTAMP, nullable=True)

    company = relationship("Company", back_populates="processed_jobs")
    location = relationship("Location", back_populates="processed_jobs")
    technologies = relationship("JobTechnology", back_populates="job")


class JobTechnology(Base):
    """
    Represents a technology entry linked to a Job record.

    Attributes:
        id (int): Unique identifier for this record.
        job_id (int): Foreign key referencing the jobs table.
        tech_name (str): Name of the technology used.
        job (relationship): Relationship to the associated Job.
    """

    __tablename__ = "job_technologies"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    tech_name = Column(Text, nullable=False)

    job = relationship("Job", back_populates="technologies")

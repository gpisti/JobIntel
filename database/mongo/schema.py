from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List
from uuid import UUID


class RawJobDocument(BaseModel):
    """
    A Pydantic model for storing raw job posting data. Includes required fields
    such as title, company, and location, as well as optional fields for salary,
    industry, experience level, remote options, and more. The model also captures
    metadata like the source_platform, scraping time, session ID, and raw
    technologies.
    """

    title: str = Field(..., min_length=3, max_length=255)
    company: str = Field(..., min_length=2, max_length=255)
    location: str = Field(..., min_length=2, max_length=255)
    job_url: str = Field(..., pattern=r"^https?://[^\s/$.?#].[^\s]*$")

    description: str = Field(..., min_length=100, alias="full_description")

    salary_min: Optional[float] = Field(None, ge=0)
    salary_max: Optional[float] = Field(None, ge=0)
    currency: Optional[str] = Field(None, min_length=3, max_length=3)
    employment_type: Optional[str] = Field(
        None, pattern=r"^(full-time|part-time|contract|freelance|other)$"
    )
    remote_option: Optional[str] = Field(
        None, pattern=r"^(onsite|remote|hybrid|unknown)$"
    )
    experience_level: Optional[str] = Field(
        None, pattern=r"^(junior|mid-level|senior|lead|not-specified)$"
    )
    industry: Optional[str] = Field(None, max_length=100)

    source_platform: str

    scraped_at: datetime
    session_id: UUID
    raw_technologies: Optional[List[str]] = Field(default_factory=list)

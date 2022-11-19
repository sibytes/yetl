from pydantic import BaseModel, Field


class Warning:

    message: str = Field(...)

    def __str__(self) -> str:
        return self.message

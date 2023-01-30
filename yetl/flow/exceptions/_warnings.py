from pydantic import BaseModel, Field


class Warning(BaseModel):

    message: str = Field(...)

    def __str__(self) -> str:
        return self.message


class ThresholdWarning(Warning):
    pass

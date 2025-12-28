from dataclasses import dataclass

@dataclass
class User:
    ExternalId: str
    ManagerExternalId : str
    UserName: str
    LegalName: str
    JobTitle: str
    IsAgent: bool
    BusinessUnit: str
    EmployeeDetails:str
    InsertedAt: str
    UpdatedAt: str
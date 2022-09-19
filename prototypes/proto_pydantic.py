from pydantic import BaseModel


class Name(BaseModel):
    firstName: str
    lastName: str


class FullName(BaseModel):
    fullName: str


class User(BaseModel):
    id: int
    name: Name | FullName


data1 = {
    "id": 1,
    "name": {"firstName": "Foo", "lastName": "Bar"},
}
user = User(**data1)
print(user.name.lastName)
print(user.name.firstName)

data2 = {
    "id": 1,
    "name": {"fullName": "Foo Bar"},
}
user2 = User(**data2)
print(user2.name.fullName)

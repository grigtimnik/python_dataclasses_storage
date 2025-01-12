from dataclasses import dataclass

from storage import Storage, BaseRecord


@dataclass
class Cats(BaseRecord):
    kot: str


@dataclass
class AdvancedRecord(BaseRecord):
    name: str
    description: str
    value: float
    tags: str


storage = Storage('TEST_DATABASE')
storage.create_table_from_dataclasses(
    BaseRecord,
    Cats,
    AdvancedRecord
)

print("Appending records")
advanced_records = [
    AdvancedRecord("001", name="Record1", description="This is the first record", value=123.45, tags="tag1"),
    AdvancedRecord("002", name="Record2", description="Second record here", value=678.90, tags="tag2"),
    AdvancedRecord("003", name="Record1", description="Version two of first record", value=321.00, tags="tag1"),
]

for record in advanced_records:
    storage[AdvancedRecord].append(record)

cats_records = [
    Cats("004", kot="Korzhik"),
    Cats("005", kot="Oxford"),
]

for record in cats_records:
    storage[Cats].append(record)


print("\nSearching by ID:")
record = storage[AdvancedRecord]["001"]
if record:
    print("Found AdvancedRecord:", record)


print("\nSearching with filters:")
for record in storage[AdvancedRecord].search(name="Record1"):
    print("Found record:", record)


print("\nDeleting by ID:")
del storage[Cats]["004"]
print("Record with ID 004 deleted")


print("\nSearching after deletion:")
record = storage[Cats]["004"]
if not record:
    print("Record with ID 004 not found, as expected")


print("\nRemoving with filters:")
storage[AdvancedRecord].remove(value=123.45)
print("Records with value=123.45 removed")
record = storage[AdvancedRecord]["001"]
if not record:
    print("Record with ID 001 not found, as expected")


storage.close_connection()

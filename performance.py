import timeit
import random
from tqdm import tqdm

from dataclasses import dataclass

from storage import Storage, BaseRecord


@dataclass
class AdvancedRecord(BaseRecord):
    name: str
    description: str
    value: float
    tags: str


def generate_records(num):
    return [AdvancedRecord(
        id=str(i),
        name=f"Record{i}",
        description=f"Description for record {i}",
        value=random.uniform(1.0, 1000.0),
        tags=f"tag{i % 10}"
    ) for i in range(num)]


def test_performance(storage: Storage, num_records):
    records = generate_records(num_records)
    names = [record.name for record in records]

    for record in tqdm(records):
        storage.append(record)

    def search_by_id():
        random_id = str(random.randint(0, num_records - 1))
        return storage[AdvancedRecord][random_id]

    search_time = timeit.timeit(search_by_id, number=100)

    def search_with_filter():
        random_name = random.choice(names)
        return list(storage[AdvancedRecord].search(name=random_name))

    filter_time = timeit.timeit(search_with_filter, number=100)

    return search_time, filter_time


storage = Storage('PERFORMANCE DB')
storage.create_table_from_dataclasses(AdvancedRecord)

for num_records in (1, 1000, 10000, 100000):
    search_time, filter_time = test_performance(storage, num_records)
    print(f"Number of records: {num_records}")
    print(f"Search by ID time: {search_time:.6f} seconds")
    print(f"Search with filter time: {filter_time:.6f} seconds")
    print("-" * 40)

storage.close_connection()

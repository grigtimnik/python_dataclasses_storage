import sqlite3

from dataclasses import dataclass, field, fields, asdict
from typing import Type, Any
from time import time


@dataclass
class BaseRecord:
    id: str
    timestamp: int = field(init=False)


def python_to_sqlite_type(py_type: Any) -> str:
    if issubclass(py_type, int):
        return 'INTEGER'
    elif issubclass(py_type, float):
        return 'REAL'
    elif issubclass(py_type, str):
        return 'TEXT'
    elif issubclass(py_type, bytes):
        return 'BLOB'
    elif py_type is type(None):
        return 'NULL'
    else:
        raise ValueError(f"Unsupported type: {py_type}")


class Storage:
    def __init__(self, db_name: str):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def close_connection(self):
        self.conn.close()

    def create_table_from_dataclass(self, cls: Type[BaseRecord]):
        cls_fields = []
        for field_data in fields(cls):
            cls_fields.append(f"{field_data.name} {python_to_sqlite_type(field_data.type)}")
        field_definitions = ', '.join(cls_fields)

        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {cls.__name__} (
                {field_definitions},
                PRIMARY KEY (id)
            )
        ''')
        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {cls.__name__}_history (
                {field_definitions},
                PRIMARY KEY (id, timestamp)
            )
        ''')

    def create_table_from_dataclasses(self, *classes: Type[BaseRecord]):
        for cls in classes:
            self.create_table_from_dataclass(cls)

    def append(self, record: BaseRecord):
        record.timestamp = int(time() * 1000)
        record_dict = asdict(record)
        fields = ', '.join(record_dict.keys())
        values_description = ', '.join(['?'] * len(record_dict))
        values = tuple(record_dict.values())
        self.cursor.execute(
            f'INSERT OR REPLACE INTO {record.__class__.__name__} ({fields}) VALUES ({values_description})',
            values
        )
        self.cursor.execute(
            f'INSERT OR REPLACE INTO {record.__class__.__name__}_history ({fields}) VALUES ({values_description})',
            values
        )
        self.conn.commit()

    def search(self, cls: Type[BaseRecord], **filters):
        table_name = cls.__name__
        if 'timestamp' in filters:
            table_name += '_history'

        if filters:
            query = f'SELECT * FROM {table_name} WHERE '
            conditions = []
            for fld, _ in filters.items():
                conditions.append(f"{fld} = ?")
            sql = query + ' AND '.join(conditions)
            self.cursor.execute(sql, tuple(filters.values()))
        else:
            sql = f'SELECT * FROM {table_name}'
            self.cursor.execute(sql)
        return self.cursor.fetchall()

    def search_by_id(self, cls: Type[BaseRecord], id: str):
        query = f"SELECT * FROM {cls.__name__} WHERE id = ? LIMIT 1"
        self.cursor.execute(query, (id,))
        return self.cursor.fetchone()

    def get_last_version(self, cls: Type[BaseRecord], record_id: str):
        query = f"""
            SELECT * FROM {cls.__name__}_history
            WHERE id = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """
        self.cursor.execute(query, (record_id,))
        return self.cursor.fetchone()

    def delete_by_key(self, cls: Type[BaseRecord], record_id: str):
        last_version = self.get_last_version(cls, record_id)
        if last_version:
            fields_names = [f.name for f in fields(cls)]
            record_dict = dict(zip(fields_names, last_version))
            timestamp = record_dict.pop('timestamp', None)
            self.remove(cls, id=record_id, timestamp=timestamp)

        self.remove(cls, id=record_id)

        last_version_after_removal = self.get_last_version(cls, record_id)
        if last_version_after_removal:
            self.cursor.execute(
                f"""
                INSERT OR REPLACE INTO {cls.__name__}
                SELECT * FROM {cls.__name__}_history
                WHERE id = ?
                ORDER BY timestamp DESC
                LIMIT 1""",
                (record_id,)
            )
            self.conn.commit()

    def remove(self, cls: Type[BaseRecord], **filters):
        table_name = cls.__name__
        if 'timestamp' in filters:
            table_name += '_history'

        conditions = [f"{fld} = ?" for fld in filters.keys()]
        sql = f"DELETE FROM {table_name} WHERE " + ' AND '.join(conditions)
        self.cursor.execute(sql, tuple(filters.values()))
        self.conn.commit()

    def remove_by_filters(self, cls: Type[BaseRecord], **filters):
        if 'timestamp' not in filters:
            matched_query = f"""
                SELECT id FROM {cls.__name__}
                WHERE {" AND ".join(f"{key} = ?" for key in filters.keys())}
            """
            self.cursor.execute(matched_query, tuple(filters.values()))
            matched_ids = [row[0] for row in self.cursor.fetchall()]

            for record_id in matched_ids:
                self.delete_by_key(cls, record_id)

        else:
            query = f"""
                SELECT DISTINCT id FROM {cls.__name__}_history
                WHERE {" AND ".join(f"{key} = ?" for key in filters.keys())}
            """
            self.cursor.execute(query, tuple(filters.values()))
            matched_ids = [row[0] for row in self.cursor.fetchall()]

            for record_id in matched_ids:
                last_version = self.get_last_version(cls, record_id)

                if last_version:
                    fields_names = [f.name for f in fields(cls)]
                    record_dict = dict(zip(fields_names, last_version))
                    if filters['timestamp'] == record_dict.pop('timestamp', None):
                        self.delete_by_key(cls, record_id)
                    else:
                        self.remove(cls, id=record_id, timestamp=filters['timestamp'])

        self.conn.commit()

    def __getitem__(self, cls: Type[BaseRecord]):
        class Wrapper:
            def __init__(self, storage: Storage, cls):
                self.storage = storage
                self.cls = cls

            def append(self, record):
                """
                Appends dataclass instance to the storage.

                example: storage[Record].append(newrecord)
                """
                self.storage.append(record)

            def __getitem__(self, record_id):
                """
                Retrieves a dataclass instance from the storage by its ID.

                :param record_id: The unique identifier of the record to retrieve.
                :return: An instance of the dataclass if found, otherwise None.
                """
                result = self.storage.search_by_id(self.cls, record_id)
                if not result:
                    return None

                fields_names = [f.name for f in fields(self.cls)]
                record_dict = dict(zip(fields_names, result))
                timestamp = record_dict.pop('timestamp', None)
                obj = self.cls(**record_dict)
                setattr(obj, 'timestamp', timestamp)

                return obj

            def search(self, **filters):
                """
                Searches for dataclass instances that match the given filters.

                :param filters: Keyword arguments where keys are field names and values are the desired
                                values to filter the records by.
                :yield: Instances of the dataclass that match the filters.

                example: for record in storage[Record].search(name = "Колесо", kind = "Квадратное"):
                """
                rows = self.storage.search(self.cls, **filters)
                if not rows:
                    return None

                fields_names = [f.name for f in fields(self.cls)]
                for row in rows:
                    record_dict = dict(zip(fields_names, row))
                    timestamp = record_dict.pop('timestamp', None)
                    obj = self.cls(**record_dict)
                    setattr(obj, 'timestamp', timestamp)

                    yield obj

            def __delitem__(self, record_id):
                """ Deletes a dataclass instance from the storage by its ID. """
                self.storage.delete_by_key(self.cls, record_id)

            def remove(self, **filters):
                """
                Removes records from the storage that match the given filters.

                :param filters: Keyword arguments where keys are field names and values are the desired
                                values to filter and remove the records by.
                """
                self.storage.remove_by_filters(self.cls, **filters)

        return Wrapper(self, cls)

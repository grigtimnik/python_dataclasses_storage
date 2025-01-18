# Хранилище Python-объектов

https://uneex.org/FrBrGeorge/ActualEducationalTasks/NoSqlite

**API для хранения и эффективной выборки экземпляров Python dataclasses во встроенной в Python СУБД sqlite**


### Поиск эелемента

Запрос к таблице возвращает итератор по подходящим объектам

    for record in storage[Record].search():

Запрос может включать в себя фильтр по полям класса:

    for record in storage[Record].search(name = "Колесо", kind = "Квадратное"):

«Ключом» (не вполне уникальным, см. ниже замечания про версионирование) в таблице является строковое поле ID. Запрос по этому полю должен возвращать один объект или None, если такого нет.

    storage[Record]["Идентификатор"]


### Версионирование, добавление и удаление элементов

Все объекты хранилища сопровождаются версией (временно́й меткой). В хранилище могут храниться объекты с разными версиями и одинаковым ID.

Если при поиске (по фильтру или ключу) не указано имя поля-версии, возвращается самая свежая версия объекта

Операция добавления элемента добавляет в таблицу соответствующий элемент (версия - это текущий timestamp; ID — ключ — обязательное поле объекта).

    storage[Record].append(newrecord)

Удаление элементов по ключу удаляет последнюю версию и имеет синтаксис словаря:

    del storage[Record]["Идентификатор"]

Удаление элементов по результатам поиска использует тот же синтаксис, что и поиск.

    storage[Record].remove(name = "Колесо", kind = "Квадратное")


## Особенности реализации

- Для каждого датакласса создаются две таблицы — самых свежих версий, с ключом ID, и с историей версий, с ключом ID + timestamp

- Каждый используемый dataclass должен быть унаследован от BaseRecord, в котором задаются поля id и timestamp

- timestamp - время в миллисекундах, задающееся автоматически при добавлении, id определяет пользователь

- можно сгенерировать id автоматически, создавая экземпляры классов через create_with_auto_id


## Производительность

Скорость поиска по id оценивается как O(log(N)) в худшем случае, где N - количество уникальных (имеющих разные id, т.е. последних версий) экземпляров датаклассов внутри одного датакласса

Скорость поиска с фильтрами оценивается как O(N) в худшем случае

Такая скорость обусловлена
- разделением на таблицы "стейта" (только последние версии) и "истории" (все версии) для каждого из классов
- скоростью поиска с помощью запросов SQLite

В файле performance.py приведены некоторые замеры производительности для 1, 1000, 10000 и 100000 объектов

- Number of records: 1

  Search by ID time: 0.003337 seconds

  Search with filter time: 0.630852 seconds

- Number of records: 1000

  Search by ID time: 0.005506 seconds

  Search with filter time: 0.734000 seconds

- Number of records: 10000

  Search by ID time: 0.004854 seconds

  Search with filter time: 0.745792 seconds

- Number of records: 100000

  Search by ID time: 0.004282 seconds

  Search with filter time: 0.845630 seconds

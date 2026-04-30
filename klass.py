import mysql.connector
from mysql.connector import Error
import csv

class DatabaseManager:

    def __init__(self, config: dict = None, db_type='mysql'):
        self.db_type = db_type
        self.config = config or {
            'host': 'mysql.65e3ab49565f.hosting.myjino.ru',
            'user': 'j30084097_137',
            'password': 'Gruppa137',
            'database': 'j30084097_137',
            'connection_timeout': 900,
            'autocommit': False
        }
        self.connection = None
        self.cursor = None
        self._connect()

    def _connect(self):
        try:
            if self.db_type == 'mysql':
                self.connection = mysql.connector.connect(**self.config)
                self.cursor = self.connection.cursor(dictionary=True)
            elif self.db_type == 'postgresql':
                import psycopg2
                from psycopg2.extras import RealDictCursor
                self.connection = psycopg2.connect(**self.config)
                self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            print(f"✅ Подключение к {self.db_type.upper()} установлено")
        except Exception as e:
            print(f"❌ Ошибка подключения: {e}")
            raise

    def _ensure_connection(self):
        if not self.connection:
            self._connect()
        elif self.db_type == 'mysql':
            if not self.connection.is_connected():
                self._connect()

    # --- СТАНДАРТНЫЕ CRUD ОПЕРАЦИИ ---

    def create(self, table: str, data: dict) -> int:
        self._ensure_connection()
        try:
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            self.cursor.execute(query, tuple(data.values()))
            self.connection.commit()
            print(f"✅ Запись добавлена в таблицу '{table}'")
            return self.cursor.lastrowid
        except Error as e:
            self.connection.rollback()
            print(f"❌ Ошибка CREATE: {e}")
            raise

    def read(self, table: str, columns: list = None, conditions: dict = None,
             order_by: str = None, limit: int = None, offset: int = None,
             filters: dict = None) -> list:
        self._ensure_connection()
        try:
            cols = ', '.join(columns) if columns else '*'
            query = f"SELECT {cols} FROM {table}"
            params = []
            where_clauses = []

            if conditions:
                for key, value in conditions.items():
                    where_clauses.append(f"{key} = %s")
                    params.append(value)

            if filters:
                for key, ops in filters.items():
                    if isinstance(ops, dict):
                        for op, value in ops.items():
                            if op in ['>', '<', '>=', '<=', '!=', 'LIKE']:
                                where_clauses.append(f"{key} {op} %s")
                                params.append(value)
                            elif op == 'IN':
                                placeholders = ', '.join(['%s'] * len(value))
                                where_clauses.append(f"{key} IN ({placeholders})")
                                params.extend(value)

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            if order_by:
                query += f" ORDER BY {order_by}"
            if limit:
                query += f" LIMIT {limit}"
            if offset:
                query += f" OFFSET {offset}"

            self.cursor.execute(query, params)
            result = self.cursor.fetchall()
            print(f"✅ Прочитано {len(result)} записей из '{table}'")
            return result
        except Error as e:
            print(f"❌ Ошибка READ: {e}")
            raise

    def update(self, table: str, data: dict, conditions: dict) -> int:
        self._ensure_connection()
        try:
            set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
            where_clause = ' AND '.join([f"{k} = %s" for k in conditions.keys()])
            query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
            params = list(data.values()) + list(conditions.values())
            self.cursor.execute(query, params)
            self.connection.commit()
            print(f"✅ Обновлено {self.cursor.rowcount} записей в '{table}'")
            return self.cursor.rowcount
        except Error as e:
            self.connection.rollback()
            print(f"❌ Ошибка UPDATE: {e}")
            raise

    def delete(self, table: str, conditions: dict) -> int:
        self._ensure_connection()
        try:
            where_clause = ' AND '.join([f"{k} = %s" for k in conditions.keys()])
            query = f"DELETE FROM {table} WHERE {where_clause}"
            self.cursor.execute(query, list(conditions.values()))
            self.connection.commit()
            print(f"✅ Удалено {self.cursor.rowcount} записей из '{table}'")
            return self.cursor.rowcount
        except Error as e:
            self.connection.rollback()
            print(f"❌ Ошибка DELETE: {e}")
            raise

    # --- НОВЫЕ МЕТОДЫ (ПО ВАШЕМУ СПИСКУ) ---

    # 1. Вывод конкретного столбца в порядке убывания или возрастания
    def get_sorted_column(self, table: str, column: str, ascending: bool = True) -> list:
        self._ensure_connection()
        order = "ASC" if ascending else "DESC"
        query = f"SELECT {column} FROM {table} ORDER BY {column} {order}"
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Error as e:
            print(f"❌ Ошибка сортировки: {e}")
            return []

    # 2. Вывод диапазона строк по айди
    def get_range_by_id(self, table: str, start_id: int, end_id: int, id_column: str = 'id') -> list:
        self._ensure_connection()
        query = f"SELECT * FROM {table} WHERE {id_column} BETWEEN %s AND %s"
        try:
            self.cursor.execute(query, (start_id, end_id))
            return self.cursor.fetchall()
        except Error as e:
            print(f"❌ Ошибка получения диапазона: {e}")
            return []

    # 3. Удаление диапазона строк по айди
    def delete_range_by_id(self, table: str, start_id: int, end_id: int, id_column: str = 'id') -> int:
        self._ensure_connection()
        query = f"DELETE FROM {table} WHERE {id_column} BETWEEN %s AND %s"
        try:
            self.cursor.execute(query, (start_id, end_id))
            self.connection.commit()
            print(f"✅ Удалено {self.cursor.rowcount} строк в диапазоне {start_id}-{end_id}")
            return self.cursor.rowcount
        except Error as e:
            self.connection.rollback()
            print(f"❌ Ошибка удаления диапазона: {e}")
            return 0

    # 4. Вывод структуры таблицы
    def get_table_structure(self, table: str) -> list:
        self._ensure_connection()
        try:
            if self.db_type == 'mysql':
                self.cursor.execute(f"DESCRIBE {table}")
            else:
                self.cursor.execute("""
                    SELECT column_name, data_type, is_nullable, column_default 
                    FROM information_schema.columns WHERE table_name = %s
                """, (table,))
            return self.cursor.fetchall()
        except Error as e:
            print(f"❌ Ошибка получения структуры: {e}")
            return []

    # 5. Вывод строки содержащей конкретное значение в конкретном столбце
    def find_by_value(self, table: str, column: str, value) -> list:
        self._ensure_connection()
        query = f"SELECT * FROM {table} WHERE {column} = %s"
        try:
            self.cursor.execute(query, (value,))
            return self.cursor.fetchall()
        except Error as e:
            print(f"❌ Ошибка поиска по значению: {e}")
            return []

    # 6. Удаление таблицы
    def drop_table(self, table: str) -> bool:
        self._ensure_connection()
        try:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
            self.connection.commit()
            print(f"🔥 Таблица '{table}' успешно удалена")
            return True
        except Error as e:
            print(f"❌ Ошибка удаления таблицы: {e}")
            return False

    # 7. Добавление и удаление нового столбца
    def add_column(self, table: str, column_name: str, column_type: str) -> bool:
        self._ensure_connection()
        try:
            self.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type}")
            self.connection.commit()
            print(f"✅ Колонка '{column_name}' добавлена в '{table}'")
            return True
        except Error as e:
            print(f"❌ Ошибка добавления колонки: {e}")
            return False

    def drop_column(self, table: str, column_name: str) -> bool:
        self._ensure_connection()
        try:
            self.cursor.execute(f"ALTER TABLE {table} DROP COLUMN {column_name}")
            self.connection.commit()
            print(f"🗑️ Колонка '{column_name}' удалена из '{table}'")
            return True
        except Error as e:
            print(f"❌ Ошибка удаления колонки: {e}")
            return False

    # 8. Экспорт и импорт в CSV
    def export_to_csv(self, table: str, file_path: str):
        self._ensure_connection()
        try:
            self.cursor.execute(f"SELECT * FROM {table}")
            rows = self.cursor.fetchall()
            if not rows:
                print("⚠️ Таблица пуста, нечего экспортировать")
                return

            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            print(f"💾 Данные из '{table}' экспортированы в {file_path}")
        except (Error, IOError) as e:
            print(f"❌ Ошибка экспорта: {e}")

    def import_from_csv(self, table: str, file_path: str):
        self._ensure_connection()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                data = [tuple(row.values()) for row in reader]
                if not data: return
                
                headers = reader.fieldnames
                placeholders = ', '.join(['%s'] * len(headers))
                columns = ', '.join(headers)
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                
                self.cursor.executemany(query, data)
                self.connection.commit()
                print(f"📥 Импортировано {len(data)} строк в '{table}'")
        except (Error, IOError) as e:
            self.connection.rollback()
            print(f"❌ Ошибка импорта: {e}")

    # --- ДОПОЛНИТЕЛЬНЫЕ И СИСТЕМНЫЕ МЕТОДЫ ---

    def join(self, primary_table: str, join_table: str, on_condition: str,
             join_type: str = 'INNER', columns: list = None,
             conditions: dict = None) -> list:
        self._ensure_connection()
        try:
            cols = ', '.join(columns) if columns else '*'
            query = f"SELECT {cols} FROM {primary_table} {join_type} JOIN {join_table} ON {on_condition}"
            params = []
            if conditions:
                where_clause = ' AND '.join([f"{k} = %s" for k in conditions.keys()])
                query += f" WHERE {where_clause}"
                params = list(conditions.values())
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Error as e:
            print(f"❌ Ошибка JOIN: {e}")
            raise

    def union(self, queries: list, all: bool = False) -> list:
        self._ensure_connection()
        try:
            union_type = "UNION ALL" if all else "UNION"
            query = f" {union_type} ".join(queries)
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            print(f"✅ Выполнен {union_type}, найдено {len(result)} записей")
            return result
        except Error as e:
            print(f"❌ Ошибка UNION: {e}")
            raise
        
    def execute_query(self, query: str, params: tuple = None) -> list:
        self._ensure_connection()
        try:
            self.cursor.execute(query, params or ())
            if query.strip().upper().startswith("SELECT"):
                return self.cursor.fetchall()
            else:
                self.connection.commit()
                return self.cursor.rowcount
        except Error as e:
            self.connection.rollback()
            raise

    def transaction(self, queries: list) -> bool:
        self._ensure_connection()
        try:
            for query, params in queries:
                self.cursor.execute(query, params or ())
            self.connection.commit()
            return True
        except Error as e:
            self.connection.rollback()
            return False

    def close(self):
        if self.cursor: self.cursor.close()
        if self.connection:
            if self.db_type == 'mysql' and self.connection.is_connected():
                self.connection.close()
            elif self.db_type == 'postgresql':
                self.connection.close()
            print("🔒 Соединение закрыто")

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.close()
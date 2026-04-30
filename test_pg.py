from klass import DatabaseManager
import os

print("=== PostgreSQL Full Test ===")


pg_config = {
    'host': 'localhost',
    'user': 'admin',
    'password': 'admin',
    'database': 'postgres',
    'port': 5432
}

try:
    with DatabaseManager(db_type='postgresql', config=pg_config) as db:
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS test_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                age INT,
                email VARCHAR(100)
            )
        """)
        

        db.execute_query("TRUNCATE TABLE test_users")

  
        db.create('test_users', {'name': 'Alice', 'age': 25, 'email': 'alice@example.com'})
        db.create('test_users', {'name': 'Bob', 'age': 19, 'email': 'bob@example.com'})
        db.create('test_users', {'name': 'Charlie', 'age': 30, 'email': 'charlie@example.com'})
        db.create('test_users', {'name': 'Diana', 'age': 22, 'email': 'diana@example.com'})

        print("\n1. --- Сортировка (Возраст по убыванию) ---")
        sorted_users = db.get_sorted_column('test_users', 'age', ascending=False)
        print(sorted_users)

        print("\n2. --- Диапазон строк (ID от 1 до 3) ---")
        range_users = db.get_range_by_id('test_users', 1, 3)
        print(f"Найдено: {len(range_users)} записи(ей)")

        print("\n3. --- Структура таблицы ---")
        struct = db.get_table_structure('test_users')
        for col in struct:
            print(col)

        print("\n4. --- Поиск конкретного значения (Bob) ---")
        user_bob = db.find_by_value('test_users', 'name', 'Bob')
        print(user_bob)

        print("\n5. --- Работа с колонками (Добавление 'phone') ---")
        db.add_column('test_users', 'phone', 'VARCHAR(20)')
        
        print("\n6. --- Экспорт в CSV ---")
        csv_file = "exported_users.csv"
        db.export_to_csv('test_users', csv_file)
        
        print("\n7. --- Импорт из CSV (в новую таблицу) ---")
        db.execute_query("CREATE TABLE IF NOT EXISTS users_backup (id INT, name VARCHAR(100), age INT, email VARCHAR(100), phone VARCHAR(20))")
        db.import_from_csv('users_backup', csv_file)

        print("\n8. --- Удаление диапазона (удалим ID 1 и 2) ---")
        deleted = db.delete_range_by_id('test_users', 1, 2)
        print(f"Удалено строк: {deleted}")

        print("\n9. --- Удаление таблиц (Cleanup) ---")
        db.drop_table('users_backup')
        db.drop_table('test_users')

   
        if os.path.exists(csv_file):
            os.remove(csv_file)

    print("\n✅ PostgreSQL тест успешно завершён!")

except Exception as e:
    print(f"\n❌ Тест прерван из-за ошибки: {e}")